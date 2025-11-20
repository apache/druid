/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.inject.Injector;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.CompactionRunner;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Order;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.CombinedDataSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.planner.ColumnMapping;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MSQCompactionRunner implements CompactionRunner
{
  private static final Logger log = new Logger(MSQCompactionRunner.class);
  public static final String TYPE = "msq";
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;

  private final ObjectMapper jsonMapper;
  private final ExprMacroTable exprMacroTable;
  private final Injector injector;
  // Needed as output column name while grouping in the scenario of:
  // a) no query granularity -- to specify an output name for the time dimension column since __time is a reserved name.
  // b) custom query granularity -- to create a virtual column containing the rounded-off row timestamp.
  // In both cases, the new column is converted back to __time later using columnMappings.
  public static final String TIME_VIRTUAL_COLUMN = "__vTime";
  public static final String ARRAY_VIRTUAL_COLUMN_PREFIX = "__vArray_";

  @JsonIgnore
  private final CurrentSubTaskHolder currentSubTaskHolder = new CurrentSubTaskHolder(
      (taskObject, config) -> {
        final MSQControllerTask msqControllerTask = (MSQControllerTask) taskObject;
        msqControllerTask.stopGracefully(config);
      });


  @JsonCreator
  public MSQCompactionRunner(
      @JacksonInject final ObjectMapper jsonMapper,
      @JacksonInject final ExprMacroTable exprMacroTable,
      @JacksonInject final Injector injector
  )
  {
    this.jsonMapper = jsonMapper;
    this.exprMacroTable = exprMacroTable;
    this.injector = injector;
  }

  /**
   * Checks if the provided compaction config is supported by MSQ. The same validation is done at
   * {@link ClientCompactionRunnerInfo#compactionConfigSupportedByMSQEngine}
   * The following configs aren't supported:
   * <ul>
   * <li>partitionsSpec of type HashedParititionsSpec.</li>
   * <li>'range' partitionsSpec with multi-valued or non-string partition dimensions.</li>
   * <li>maxTotalRows in DynamicPartitionsSpec.</li>
   * <li>Rollup without metricsSpec being specified or vice-versa.</li>
   * <li>Any aggregatorFactory {@code A} s.t. {@code A != A.combiningFactory()}.</li>
   * <li>Multiple disjoint intervals in compaction task</li>
   * </ul>
   */
  @Override
  public CompactionConfigValidationResult validateCompactionTask(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalToDataSchemaMap
  )
  {
    if (intervalToDataSchemaMap.size() > 1) {
      // We are currently not able to handle multiple intervals in the map for multiple reasons, one of them being that
      // the subsequent worker ids clash -- since they are derived from MSQControllerTask ID which in turn is equal to
      // CompactionTask ID for each sequentially launched MSQControllerTask.
      return CompactionConfigValidationResult.failure(
          "MSQ: Disjoint compaction intervals[%s] not supported",
          intervalToDataSchemaMap.keySet()
      );
    }
    List<CompactionConfigValidationResult> validationResults = new ArrayList<>();
    DataSchema dataSchema = Iterables.getOnlyElement(intervalToDataSchemaMap.values());
    if (compactionTask.getTuningConfig() != null) {
      validationResults.add(
          ClientCompactionRunnerInfo.validatePartitionsSpecForMSQ(
              compactionTask.getTuningConfig().getPartitionsSpec(),
              dataSchema.getDimensionsSpec().getDimensions()
          )
      );
      validationResults.add(
          validatePartitionDimensionsAreNotMultiValued(
              compactionTask.getTuningConfig().getPartitionsSpec(),
              dataSchema.getDimensionsSpec(),
              dataSchema instanceof CombinedDataSchema
              ? ((CombinedDataSchema) dataSchema).getMultiValuedDimensions()
              : null
          )
      );

    }
    if (compactionTask.getGranularitySpec() != null) {
      validationResults.add(ClientCompactionRunnerInfo.validateRollupForMSQ(
          compactionTask.getMetricsSpec(),
          compactionTask.getGranularitySpec().isRollup()
      ));
    }
    validationResults.add(ClientCompactionRunnerInfo.validateMaxNumTasksForMSQ(compactionTask.getContext()));
    validationResults.add(ClientCompactionRunnerInfo.validateMetricsSpecForMSQ(compactionTask.getMetricsSpec()));
    return validationResults.stream()
                            .filter(result -> !result.isValid())
                            .findFirst()
                            .orElse(CompactionConfigValidationResult.success());
  }

  private CompactionConfigValidationResult validatePartitionDimensionsAreNotMultiValued(
      PartitionsSpec partitionsSpec,
      DimensionsSpec dimensionsSpec,
      Set<String> multiValuedDimensions
  )
  {
    List<String> dimensionSchemas = dimensionsSpec.getDimensionNames();
    if (partitionsSpec instanceof DimensionRangePartitionsSpec
        && dimensionSchemas != null
        && multiValuedDimensions != null
        && !multiValuedDimensions.isEmpty()) {
      Optional<String> multiValuedDimension = ((DimensionRangePartitionsSpec) partitionsSpec)
          .getPartitionDimensions()
          .stream()
          .filter(multiValuedDimensions::contains)
          .findAny();
      if (multiValuedDimension.isPresent()) {
        return CompactionConfigValidationResult.failure(
            "MSQ: Multi-valued string partition dimension[%s] not supported with 'range' partition spec",
            multiValuedDimension.get()
        );
      }
    }
    return CompactionConfigValidationResult.success();
  }

  @Override
  public CurrentSubTaskHolder getCurrentSubTaskHolder()
  {
    return currentSubTaskHolder;
  }

  @Override
  public TaskStatus runCompactionTasks(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalDataSchemas,
      TaskToolbox taskToolbox
  ) throws Exception
  {
    List<MSQControllerTask> msqControllerTasks = createMsqControllerTasks(compactionTask, intervalDataSchemas);

    if (msqControllerTasks.isEmpty()) {
      String msg = StringUtils.format(
          "Can't find segments from inputSpec[%s], nothing to do.",
          compactionTask.getIoConfig().getInputSpec()
      );
      return TaskStatus.failure(compactionTask.getId(), msg);
    }
    return runSubtasks(
        msqControllerTasks,
        taskToolbox,
        currentSubTaskHolder,
        compactionTask.getId()
    );
  }

  public List<MSQControllerTask> createMsqControllerTasks(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalDataSchemas
  ) throws JsonProcessingException
  {
    final List<MSQControllerTask> msqControllerTasks = new ArrayList<>();

    for (Map.Entry<Interval, DataSchema> intervalDataSchema : intervalDataSchemas.entrySet()) {
      Query<?> query;
      Interval interval = intervalDataSchema.getKey();
      DataSchema dataSchema = intervalDataSchema.getValue();
      Map<String, VirtualColumn> inputColToVirtualCol = getVirtualColumns(dataSchema, interval);

      if (isGroupBy(dataSchema)) {
        query = buildGroupByQuery(compactionTask, interval, dataSchema, inputColToVirtualCol);
      } else {
        query = buildScanQuery(compactionTask, interval, dataSchema, inputColToVirtualCol);
      }

      QueryContext compactionTaskContext = new QueryContext(compactionTask.getContext());

      DataSourceMSQDestination destination = buildMSQDestination(compactionTask, dataSchema);

      boolean isReindex = MSQControllerTask.isReplaceInputDataSourceTask(query, destination);

      LegacyMSQSpec msqSpec = LegacyMSQSpec.builder()
                               .query(query.withOverriddenContext(Collections.singletonMap(MultiStageQueryContext.CTX_IS_REINDEX, isReindex)))
                               .columnMappings(getColumnMappings(dataSchema))
                               .destination(destination)
                               .assignmentStrategy(MultiStageQueryContext.getAssignmentStrategy(compactionTaskContext))
                               .tuningConfig(buildMSQTuningConfig(compactionTask, compactionTaskContext))
                               .build();

      Map<String, Object> msqControllerTaskContext = createMSQTaskContext(compactionTask, dataSchema);

      MSQControllerTask controllerTask = new MSQControllerTask(
          compactionTask.getId(),
          msqSpec.withOverriddenContext(msqControllerTaskContext),
          null,
          msqControllerTaskContext,
          null,
          null,
          null,
          msqControllerTaskContext,
          injector
      );
      msqControllerTasks.add(controllerTask);
    }
    return msqControllerTasks;
  }

  private static DataSourceMSQDestination buildMSQDestination(
      CompactionTask compactionTask,
      DataSchema dataSchema
  )
  {
    final Interval replaceInterval = compactionTask.getIoConfig()
                                                   .getInputSpec()
                                                   .findInterval(compactionTask.getDataSource());

    return new DataSourceMSQDestination(
        dataSchema.getDataSource(),
        dataSchema.getGranularitySpec().getSegmentGranularity(),
        null,
        ImmutableList.of(replaceInterval),
        dataSchema.getDimensionsSpec()
                  .getDimensions()
                  .stream()
                  .collect(Collectors.toMap(DimensionSchema::getName, Function.identity())),
        dataSchema.getProjections(),
        null
    );
  }

  private static MSQTuningConfig buildMSQTuningConfig(CompactionTask compactionTask, QueryContext compactionTaskContext)
  {
    // Transfer MSQ-related context params, if any, from the compaction context itself.

    final int maxNumTasks = MultiStageQueryContext.getMaxNumTasks(compactionTaskContext);

    // This parameter is used internally for the number of worker tasks only, so we subtract 1
    final int maxNumWorkers = maxNumTasks - 1;

    // We don't consider maxRowsInMemory coming via CompactionTuningConfig since it always sets a default value if no
    // value specified by user.
    final int maxRowsInMemory = MultiStageQueryContext.getRowsInMemory(compactionTaskContext);
    final Integer maxNumSegments = MultiStageQueryContext.getMaxNumSegments(compactionTaskContext);

    Integer rowsPerSegment = getRowsPerSegment(compactionTask);

    return new MSQTuningConfig(
        maxNumWorkers,
        maxRowsInMemory,
        rowsPerSegment,
        maxNumSegments,
        compactionTask.getTuningConfig() != null ? compactionTask.getTuningConfig().getIndexSpec() : null
    );
  }

  private static Integer getRowsPerSegment(CompactionTask compactionTask)
  {
    if (compactionTask.getTuningConfig() != null && compactionTask.getTuningConfig().getPartitionsSpec() != null) {
      return compactionTask.getTuningConfig().getPartitionsSpec().getMaxRowsPerSegment();
    }
    return PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT;
  }

  private static RowSignature getRowSignature(DataSchema dataSchema)
  {
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    if (dataSchema.getDimensionsSpec().isForceSegmentSortByTime() == true) {
      // If sort not forced by time, __time appears as part of dimensions in DimensionsSpec
      rowSignatureBuilder.add(dataSchema.getTimestampSpec().getTimestampColumn(), ColumnType.LONG);
    }
    if (!isQueryGranularityEmptyOrNone(dataSchema)) {
      // A virtual column for query granularity would have been added. Add corresponding column type.
      rowSignatureBuilder.add(TIME_VIRTUAL_COLUMN, ColumnType.LONG);
    }
    for (DimensionSchema dimensionSchema : dataSchema.getDimensionsSpec().getDimensions()) {
      rowSignatureBuilder.add(dimensionSchema.getName(), dimensionSchema.getColumnType());
    }
    // There can be columns that are part of metricsSpec for a datasource.
    for (AggregatorFactory aggregatorFactory : dataSchema.getAggregators()) {
      rowSignatureBuilder.add(aggregatorFactory.getName(), aggregatorFactory.getIntermediateType());
    }
    return rowSignatureBuilder.build();
  }

  /**
   * Creates a {@link DataSource} and uses 'system' {@link AuthorizationResult} using an {@link Escalator} and
   * {@link AuthorizerMapper} and applies any resulting {@link org.apache.druid.query.policy.Policy} to it using
   * {@link DataSource#withPolicies(Map, PolicyEnforcer)}
   */
  private DataSource getInputDataSource(String name)
  {
    TableDataSource dataSource = new TableDataSource(name);
    final Escalator escalator = injector.getInstance(Escalator.class);
    if (escalator != null) {
      final AuthorizerMapper authorizerMapper = injector.getInstance(AuthorizerMapper.class);
      final PolicyEnforcer policyEnforcer = injector.getInstance(PolicyEnforcer.class);
      final AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
          escalator.createEscalatedAuthenticationResult(),
          List.of(new ResourceAction(new Resource(name, ResourceType.DATASOURCE), Action.READ)),
          authorizerMapper
      );
      return dataSource.withPolicies(authResult.getPolicyMap(), policyEnforcer);
    }
    return dataSource;
  }

  private static List<DimensionSpec> getAggregateDimensions(
      DataSchema dataSchema,
      Map<String, VirtualColumn> inputColToVirtualCol
  )
  {
    List<DimensionSpec> dimensionSpecs = new ArrayList<>();

    if (isQueryGranularityEmptyOrNone(dataSchema)) {
      // Dimensions in group-by aren't allowed to have time column name as the output name.
      dimensionSpecs.add(new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, TIME_VIRTUAL_COLUMN, ColumnType.LONG));
    } else {
      // The changed granularity would result in a new virtual column that needs to be aggregated upon.
      dimensionSpecs.add(new DefaultDimensionSpec(TIME_VIRTUAL_COLUMN, TIME_VIRTUAL_COLUMN, ColumnType.LONG));
    }
    // If virtual columns are created from dimensions, replace dimension columns names with virtual column names.
    dimensionSpecs.addAll(
        dataSchema.getDimensionsSpec().getDimensions().stream()
                  .map(dim -> {
                    String dimension = dim.getName();
                    ColumnType colType = dim.getColumnType();
                    if (inputColToVirtualCol.containsKey(dim.getName())) {
                      VirtualColumn virtualColumn = inputColToVirtualCol.get(dimension);
                      dimension = virtualColumn.getOutputName();
                      if (virtualColumn instanceof ExpressionVirtualColumn) {
                        colType = ((ExpressionVirtualColumn) virtualColumn).getOutputType();
                      }
                    }
                    return new DefaultDimensionSpec(dimension, dimension, colType);
                  })
                  .collect(Collectors.toList()));
    return dimensionSpecs;
  }

  private static ColumnMappings getColumnMappings(DataSchema dataSchema)
  {
    List<ColumnMapping> columnMappings = new ArrayList<>();
    // For scan queries, a virtual column is created from __time if a custom query granularity is provided. For
    // group-by queries, as insert needs __time, it will always be one of the dimensions. Since dimensions in groupby
    // aren't allowed to have time column as the output name, we map time dimension to TIME_VIRTUAL_COLUMN in
    // dimensions, and map it back to the time column here.
    String timeColumn = (isGroupBy(dataSchema) || !isQueryGranularityEmptyOrNone(dataSchema))
                        ? TIME_VIRTUAL_COLUMN
                        : ColumnHolder.TIME_COLUMN_NAME;
    ColumnMapping timeColumnMapping = new ColumnMapping(timeColumn, ColumnHolder.TIME_COLUMN_NAME);
    if (dataSchema.getDimensionsSpec().isForceSegmentSortByTime()) {
      // When not sorted by time, the __time column is missing from dimensionsSpec
      columnMappings.add(timeColumnMapping);
    }
    columnMappings.addAll(
        dataSchema.getDimensionsSpec()
                  .getDimensions()
                  .stream()
                  .map(dim -> dim.getName().equals(ColumnHolder.TIME_COLUMN_NAME)
                              ? timeColumnMapping
                              : new ColumnMapping(dim.getName(), dim.getName()))
                  .collect(Collectors.toList())
    );
    columnMappings.addAll(Arrays.stream(dataSchema.getAggregators())
                                .map(agg -> new ColumnMapping(agg.getName(), agg.getName()))
                                .collect(Collectors.toList()));
    return new ColumnMappings(columnMappings);
  }

  private static List<OrderByColumnSpec> getOrderBySpec(PartitionsSpec partitionSpec)
  {
    if (partitionSpec.getType() == SecondaryPartitionType.RANGE) {
      List<String> dimensions = ((DimensionRangePartitionsSpec) partitionSpec).getPartitionDimensions();
      return dimensions.stream()
                       .map(dim -> new OrderByColumnSpec(dim, OrderByColumnSpec.Direction.ASCENDING))
                       .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  private static Map<String, Object> buildQueryContext(
      Map<String, Object> taskContext,
      DataSchema dataSchema
  )
  {
    if (dataSchema.getDimensionsSpec().isForceSegmentSortByTime()) {
      return taskContext;
    }
    Map<String, Object> queryContext = new HashMap<>(taskContext);
    queryContext.put(MultiStageQueryContext.CTX_FORCE_TIME_SORT, false);
    return queryContext;
  }

  private Query<?> buildScanQuery(
      CompactionTask compactionTask,
      Interval interval,
      DataSchema dataSchema,
      Map<String, VirtualColumn> inputColToVirtualCol
  )
  {
    RowSignature rowSignature = getRowSignature(dataSchema);
    VirtualColumns virtualColumns = VirtualColumns.create(new ArrayList<>(inputColToVirtualCol.values()));
    Druids.ScanQueryBuilder scanQueryBuilder = new Druids.ScanQueryBuilder()
        .dataSource(getInputDataSource(dataSchema.getDataSource()))
        .columns(rowSignature.getColumnNames())
        .virtualColumns(virtualColumns)
        .columnTypes(rowSignature.getColumnTypes())
        .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(interval)))
        .filters(dataSchema.getTransformSpec().getFilter())
        .context(buildQueryContext(compactionTask.getContext(), dataSchema));

    if (compactionTask.getTuningConfig() != null && compactionTask.getTuningConfig().getPartitionsSpec() != null) {
      List<OrderByColumnSpec> orderByColumnSpecs = getOrderBySpec(compactionTask.getTuningConfig().getPartitionsSpec());

      scanQueryBuilder.orderBy(
          orderByColumnSpecs
              .stream()
              .map(orderByColumnSpec ->
                       new OrderBy(
                           orderByColumnSpec.getDimension(),
                           Order.fromString(orderByColumnSpec.getDirection().toString())
                       ))
              .collect(Collectors.toList())
      );
    }
    return scanQueryBuilder.build();
  }

  private static boolean isGroupBy(DataSchema dataSchema)
  {
    if (dataSchema.getGranularitySpec() != null) {
      // If rollup is true without any metrics, all columns are treated as dimensions and
      // duplicate rows are removed in line with native compaction. This case can only happen if the rollup is
      // specified as null in the compaction spec and is then inferred to be true by segment analysis. metrics=null and
      // rollup=true combination in turn can only have been recorded for natively ingested segments.
      return dataSchema.getGranularitySpec().isRollup();
    }
    // If no rollup specified, decide based on whether metrics are present.
    return dataSchema.getAggregators().length > 0;
  }

  private static boolean isQueryGranularityEmptyOrNone(DataSchema dataSchema)
  {
    return dataSchema.getGranularitySpec() == null
           || dataSchema.getGranularitySpec().getQueryGranularity() == null
           || Objects.equals(
        dataSchema.getGranularitySpec().getQueryGranularity(),
        Granularities.NONE
    );
  }

  /**
   * Conditionally creates below virtual columns
   * <ul>
   * <li>timestamp column (for custom queryGranularity): converts __time field in line with the provided
   * queryGranularity, since the queryGranularity field itself in MSQControllerTask is mandated to be ALL.</li>
   * <li>mv_to_array columns (for group-by queries): temporary columns that convert MVD columns to array to enable
   * grouping on them without unnesting.</li>
   * </ul>
   */
  private Map<String, VirtualColumn> getVirtualColumns(DataSchema dataSchema, Interval interval)
  {
    Map<String, VirtualColumn> inputColToVirtualCol = new HashMap<>();
    if (!isQueryGranularityEmptyOrNone(dataSchema)) {
      // Round-off time field according to provided queryGranularity
      String timeVirtualColumnExpr;
      if (dataSchema.getGranularitySpec()
                    .getQueryGranularity()
                    .equals(Granularities.ALL)) {
        // For ALL query granularity, all records in a segment are assigned the interval start timestamp of the segment.
        // It's the same behaviour in native compaction.
        timeVirtualColumnExpr = StringUtils.format("timestamp_parse('%s')", interval.getStart());
      } else {
        PeriodGranularity periodQueryGranularity = (PeriodGranularity) dataSchema.getGranularitySpec()
                                                                                 .getQueryGranularity();
        // Round off the __time column according to the required granularity.
        timeVirtualColumnExpr =
            StringUtils.format(
                "timestamp_floor(\"%s\", '%s')",
                ColumnHolder.TIME_COLUMN_NAME,
                periodQueryGranularity.getPeriod().toString()
            );
      }
      inputColToVirtualCol.put(ColumnHolder.TIME_COLUMN_NAME, new ExpressionVirtualColumn(
          TIME_VIRTUAL_COLUMN,
          timeVirtualColumnExpr,
          ColumnType.LONG,
          exprMacroTable
      ));
    }
    if (isGroupBy(dataSchema)) {
      // Convert MVDs to arrays for grouping to avoid unnest, assuming all string cols to be MVDs.
      Set<String> multiValuedColumns = dataSchema.getDimensionsSpec()
                                                 .getDimensions()
                                                 .stream()
                                                 .filter(dim -> dim.getColumnType().equals(ColumnType.STRING))
                                                 .map(DimensionSchema::getName)
                                                 .collect(Collectors.toSet());
      if (dataSchema instanceof CombinedDataSchema &&
          ((CombinedDataSchema) dataSchema).getMultiValuedDimensions() != null) {
        // Filter actual MVDs from schema info.
        Set<String> multiValuedColumnsFromSchema =
            ((CombinedDataSchema) dataSchema).getMultiValuedDimensions();
        multiValuedColumns = multiValuedColumns.stream()
                                               .filter(multiValuedColumnsFromSchema::contains)
                                               .collect(Collectors.toSet());
      }

      for (String dim : multiValuedColumns) {
        String virtualColumnExpr = StringUtils.format("mv_to_array(\"%s\")", dim);
        inputColToVirtualCol.put(
            dim,
            new ExpressionVirtualColumn(
                ARRAY_VIRTUAL_COLUMN_PREFIX + dim,
                virtualColumnExpr,
                ColumnType.STRING_ARRAY,
                exprMacroTable
            )
        );
      }
    }
    return inputColToVirtualCol;
  }

  private Query<?> buildGroupByQuery(
      CompactionTask compactionTask,
      Interval interval,
      DataSchema dataSchema,
      Map<String, VirtualColumn> inputColToVirtualCol
  )
  {
    DimFilter dimFilter = dataSchema.getTransformSpec().getFilter();

    VirtualColumns virtualColumns = VirtualColumns.create(new ArrayList<>(inputColToVirtualCol.values()));

    // Convert MVDs converted to arrays back to MVDs, with the same name as the input column.
    // This is safe since input column names no longer exist at post-aggregation stage.
    List<PostAggregator> postAggregators =
        inputColToVirtualCol.entrySet()
                            .stream()
                            .filter(entry -> !entry.getKey().equals(ColumnHolder.TIME_COLUMN_NAME))
                            .map(
                                entry ->
                                    new ExpressionPostAggregator(
                                        entry.getKey(),
                                        StringUtils.format("array_to_mv(\"%s\")", entry.getValue().getOutputName()),
                                        null,
                                        ColumnType.STRING,
                                        exprMacroTable
                                    )
                            )
                            .collect(Collectors.toList());

    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(getInputDataSource(compactionTask.getDataSource()))
        .setVirtualColumns(virtualColumns)
        .setDimFilter(dimFilter)
        .setGranularity(new AllGranularity())
        .setDimensions(getAggregateDimensions(dataSchema, inputColToVirtualCol))
        .setAggregatorSpecs(Arrays.asList(dataSchema.getAggregators()))
        .setPostAggregatorSpecs(postAggregators)
        .setContext(buildQueryContext(compactionTask.getContext(), dataSchema))
        .setInterval(interval);

    if (compactionTask.getTuningConfig() != null && compactionTask.getTuningConfig().getPartitionsSpec() != null) {
      getOrderBySpec(compactionTask.getTuningConfig().getPartitionsSpec()).forEach(builder::addOrderByColumn);
    }
    return builder.build();
  }

  private String serializeGranularity(Granularity granularity, ObjectMapper jsonMapper) throws JsonProcessingException
  {
    if (granularity != null) {
      // AllGranularity by default gets deserialized into {"type": "all"} since there is no custom serialize impl -- as
      // is there for PeriodGranularity. Not implementing the serializer itself to avoid things breaking elsewhere.
      return granularity.equals(Granularities.ALL) ? "ALL" : jsonMapper.writeValueAsString(granularity);
    }
    return null;
  }

  private Map<String, Object> createMSQTaskContext(CompactionTask compactionTask, DataSchema dataSchema)
      throws JsonProcessingException
  {
    Map<String, Object> context = new HashMap<>(compactionTask.getContext());
    context.put(
        DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
        serializeGranularity(dataSchema.getGranularitySpec() != null
                             ? dataSchema.getGranularitySpec()
                                         .getSegmentGranularity()
                             : DEFAULT_SEGMENT_GRANULARITY, jsonMapper)
    );
    if (!isQueryGranularityEmptyOrNone(dataSchema)) {
      context.put(
          DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY,
          serializeGranularity(dataSchema.getGranularitySpec().getQueryGranularity(), jsonMapper)
      );
    }
    // Similar to compaction using the native engine, don't finalize aggregations.
    // Used for writing the data schema during segment generation phase.
    context.putIfAbsent(MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS, false);
    // Add appropriate finalization to native query context i.e. for the GroupBy query
    context.putIfAbsent(QueryContexts.FINALIZE_KEY, false);
    // Only scalar or array-type dimensions are allowed as grouping keys.
    context.putIfAbsent(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false);
    // Always override CTX_ARRAY_INGEST_MODE since it can otherwise lead to mixed ARRAY and MVD types for a column.
    context.put(MultiStageQueryContext.CTX_ARRAY_INGEST_MODE, "array");
    return context;
  }

  private static TaskStatus runSubtasks(
      List<MSQControllerTask> tasks,
      TaskToolbox toolbox,
      CurrentSubTaskHolder currentSubTaskHolder,
      String compactionTaskId
  ) throws JsonProcessingException
  {
    final int totalNumSpecs = tasks.size();
    log.info("Generated [%d] MSQControllerTask specs", totalNumSpecs);

    int failCnt = 0;

    for (MSQControllerTask eachTask : tasks) {
      final String json = toolbox.getJsonMapper().writerWithDefaultPrettyPrinter().writeValueAsString(eachTask);
      if (!currentSubTaskHolder.setTask(eachTask)) {
        String errMsg = "Task was asked to stop. Finish as failed.";
        log.info(errMsg);
        return TaskStatus.failure(compactionTaskId, errMsg);
      }
      try {
        if (eachTask.isReady(toolbox.getTaskActionClient())) {
          log.info("Running MSQControllerTask: " + json);
          final TaskStatus eachResult = eachTask.run(toolbox);
          if (!eachResult.isSuccess()) {
            failCnt++;
            log.warn("Failed to run MSQControllerTask: [%s].\nTrying the next MSQControllerTask.", json);
          }
        } else {
          failCnt++;
          log.warn("MSQControllerTask is not ready: [%s].\nTrying the next MSQControllerTask.", json);
        }
      }
      catch (Exception e) {
        failCnt++;
        log.warn(e, "Failed to run MSQControllerTask: [%s].\nTrying the next MSQControllerTask.", json);
      }
    }
    String msg = StringUtils.format(
        "Ran [%d] MSQControllerTasks, [%d] succeeded, [%d] failed",
        totalNumSpecs,
        totalNumSpecs - failCnt,
        failCnt
    );
    log.info(msg);
    return failCnt == 0 ? TaskStatus.success(compactionTaskId) : TaskStatus.failure(compactionTaskId, msg);
  }
}
