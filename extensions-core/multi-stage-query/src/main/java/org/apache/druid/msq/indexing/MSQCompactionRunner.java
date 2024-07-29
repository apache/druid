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
import com.google.inject.Injector;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
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
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampParseExprMacro;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.CombinedDataSchema;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
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
import java.util.stream.Collectors;

public class MSQCompactionRunner implements CompactionRunner
{
  private static final Logger log = new Logger(MSQCompactionRunner.class);
  public static final String TYPE = "msq";
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;

  private final ObjectMapper jsonMapper;
  private final Injector injector;
  // Needed as output column name while grouping in the scenario of:
  // a) no query granularity -- to specify an output name for the time dimension column since __time is a reserved name.
  // b) custom query granularity -- to create a virtual column containing the rounded-off row timestamp.
  // In both cases, the new column is converted back to __time later using columnMappings.
  public static final String TIME_VIRTUAL_COLUMN = "__vTime";

  @JsonIgnore
  private final CurrentSubTaskHolder currentSubTaskHolder = new CurrentSubTaskHolder(
      (taskObject, config) -> {
        final MSQControllerTask msqControllerTask = (MSQControllerTask) taskObject;
        msqControllerTask.stopGracefully(config);
      });


  @JsonCreator
  public MSQCompactionRunner(@JacksonInject ObjectMapper jsonMapper, @JacksonInject Injector injector)
  {
    this.jsonMapper = jsonMapper;
    this.injector = injector;
  }

  /**
   * Checks if the provided compaction config is supported by MSQ. The same validation is done at
   * {@link ClientCompactionRunnerInfo#compactionConfigSupportedByMSQEngine}
   * The following configs aren't supported:
   * <ul>
   * <li>partitionsSpec of type HashedParititionsSpec.</li>
   * <li>maxTotalRows in DynamicPartitionsSpec.</li>
   * <li>rollup set to false in granularitySpec when metricsSpec is specified. Null is treated as true.</li>
   * <li>queryGranularity set to ALL in granularitySpec.</li>
   * <li>Each metric has output column name same as the input name.</li>
   * </ul>
   */
  @Override
  public CompactionConfigValidationResult validateCompactionTask(
      CompactionTask compactionTask,
      Map<Interval, DataSchema> intervalToDataSchemaMap
  )
  {
    List<CompactionConfigValidationResult> validationResults = new ArrayList<>();
    if (compactionTask.getTuningConfig() != null) {
      validationResults.add(ClientCompactionRunnerInfo.validatePartitionsSpecForMSQ(
          compactionTask.getTuningConfig().getPartitionsSpec())
      );
    }
    if (compactionTask.getGranularitySpec() != null) {
      validationResults.add(ClientCompactionRunnerInfo.validateRollupForMSQ(
          compactionTask.getMetricsSpec(),
          compactionTask.getGranularitySpec().isRollup()
      ));
    }
    validationResults.add(ClientCompactionRunnerInfo.validateMaxNumTasksForMSQ(compactionTask.getContext()));
    validationResults.add(validateRolledUpSegments(intervalToDataSchemaMap));
    return validationResults.stream()
                            .filter(result -> !result.isValid())
                            .findFirst()
                            .orElse(CompactionConfigValidationResult.success());
  }

  /**
   * Valides that there are no rolled-up segments where either:
   * <ul>
   * <li>aggregator factory differs from its combining factory </li>
   * <li>input col name is different from the output name (non-idempotent)</li>
   * </ul>
   */
  private CompactionConfigValidationResult validateRolledUpSegments(Map<Interval, DataSchema> intervalToDataSchemaMap)
  {
    for (Map.Entry<Interval, DataSchema> intervalDataSchema : intervalToDataSchemaMap.entrySet()) {
      if (intervalDataSchema.getValue() instanceof CombinedDataSchema) {
        CombinedDataSchema combinedDataSchema = (CombinedDataSchema) intervalDataSchema.getValue();
        if (combinedDataSchema.hasRolledUpSegments()) {
          for (AggregatorFactory aggregatorFactory : combinedDataSchema.getAggregators()) {
            // This is a conservative check as existing rollup may have been idempotent but the aggregator provided in
            // compaction spec isn't. This would get properly compacted yet fails in the below pre-check.
            if (
                !(
                    aggregatorFactory.getClass().equals(aggregatorFactory.getCombiningFactory().getClass()) &&
                    (
                        aggregatorFactory.requiredFields().isEmpty() ||
                        (aggregatorFactory.requiredFields().size() == 1 &&
                         aggregatorFactory.requiredFields()
                                          .get(0)
                                          .equals(aggregatorFactory.getName()))
                    )
                )
            ) {
              // MSQ doesn't support rolling up already rolled-up segments when aggregate column name is different from
              // the aggregated column name. This is because the aggregated values would then get overwritten by new
              // values and the existing values would be lost. Note that if no rollup is specified in an index spec,
              // the default value is true.
              return CompactionConfigValidationResult.failure(
                  "MSQ: Rolled-up segments in compaction interval[%s].",
                  intervalDataSchema.getKey()
              );
            }
          }
        }
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

      if (isGroupBy(dataSchema)) {
        query = buildGroupByQuery(compactionTask, interval, dataSchema);
      } else {
        query = buildScanQuery(compactionTask, interval, dataSchema);
      }
      QueryContext compactionTaskContext = new QueryContext(compactionTask.getContext());

      MSQSpec msqSpec = MSQSpec.builder()
                               .query(query)
                               .columnMappings(getColumnMappings(dataSchema))
                               .destination(buildMSQDestination(compactionTask, dataSchema))
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
        ImmutableList.of(replaceInterval)
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
    Integer rowsPerSegment = PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT;
    if (compactionTask.getTuningConfig() != null) {
      PartitionsSpec partitionsSpec = compactionTask.getTuningConfig().getPartitionsSpec();
      if (partitionsSpec instanceof DynamicPartitionsSpec) {
        rowsPerSegment = partitionsSpec.getMaxRowsPerSegment();
      } else if (partitionsSpec instanceof DimensionRangePartitionsSpec) {
        DimensionRangePartitionsSpec dimensionRangePartitionsSpec = (DimensionRangePartitionsSpec) partitionsSpec;
        rowsPerSegment = dimensionRangePartitionsSpec.getTargetRowsPerSegment() != null
                         ? dimensionRangePartitionsSpec.getTargetRowsPerSegment()
                         : dimensionRangePartitionsSpec.getMaxRowsPerSegment();
      }
    }
    return rowsPerSegment;
  }

  private static RowSignature getRowSignature(DataSchema dataSchema)
  {
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add(dataSchema.getTimestampSpec().getTimestampColumn(), ColumnType.LONG);
    if (!isQueryGranularityEmptyOrNone(dataSchema)) {
      // A virtual column for query granularity would have been added. Add corresponding column type.
      rowSignatureBuilder.add(TIME_VIRTUAL_COLUMN, ColumnType.LONG);
    }
    for (DimensionSchema dimensionSchema : dataSchema.getDimensionsSpec().getDimensions()) {
      rowSignatureBuilder.add(dimensionSchema.getName(), ColumnType.fromString(dimensionSchema.getTypeName()));
    }
    // There can be columns that are part of metricsSpec for a datasource.
    for (AggregatorFactory aggregatorFactory : dataSchema.getAggregators()) {
      rowSignatureBuilder.add(aggregatorFactory.getName(), aggregatorFactory.getIntermediateType());
    }
    return rowSignatureBuilder.build();
  }

  private static List<DimensionSpec> getAggregateDimensions(DataSchema dataSchema)
  {
    List<DimensionSpec> dimensionSpecs = new ArrayList<>();

    if (isQueryGranularityEmptyOrNone(dataSchema)) {
      // Dimensions in group-by aren't allowed to have time column name as the output name.
      dimensionSpecs.add(new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, TIME_VIRTUAL_COLUMN, ColumnType.LONG));
    } else {
      // The changed granularity would result in a new virtual column that needs to be aggregated upon.
      dimensionSpecs.add(new DefaultDimensionSpec(TIME_VIRTUAL_COLUMN, TIME_VIRTUAL_COLUMN, ColumnType.LONG));
    }

    dimensionSpecs.addAll(dataSchema.getDimensionsSpec().getDimensions().stream()
                                    .map(dim -> new DefaultDimensionSpec(
                                        dim.getName(),
                                        dim.getName(),
                                        dim.getColumnType()
                                    ))
                                    .collect(Collectors.toList()));
    return dimensionSpecs;
  }

  private static ColumnMappings getColumnMappings(DataSchema dataSchema)
  {
    List<ColumnMapping> columnMappings = dataSchema.getDimensionsSpec()
                                                   .getDimensions()
                                                   .stream()
                                                   .map(dim -> new ColumnMapping(
                                                       dim.getName(), dim.getName()))
                                                   .collect(Collectors.toList());
    columnMappings.addAll(Arrays.stream(dataSchema.getAggregators())
                                .map(agg -> new ColumnMapping(agg.getName(), agg.getName()))
                                .collect(
                                    Collectors.toList()));
    if (isGroupBy(dataSchema) || !isQueryGranularityEmptyOrNone(dataSchema)) {
      // For scan queries, a virtual column is created from __time if a custom query granularity is provided. For
      // group-by queries, as insert needs __time, it will always be one of the dimensions. Since dimensions in groupby
      // aren't allowed to have time column as the output name, we map time dimension to TIME_VIRTUAL_COLUMN in
      // dimensions, and map it back to the time column here.
      columnMappings.add(new ColumnMapping(TIME_VIRTUAL_COLUMN, ColumnHolder.TIME_COLUMN_NAME));
    } else {
      columnMappings.add(new ColumnMapping(ColumnHolder.TIME_COLUMN_NAME, ColumnHolder.TIME_COLUMN_NAME));
    }
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

  private static Query<?> buildScanQuery(CompactionTask compactionTask, Interval interval, DataSchema dataSchema)
  {
    RowSignature rowSignature = getRowSignature(dataSchema);
    Druids.ScanQueryBuilder scanQueryBuilder = new Druids.ScanQueryBuilder()
        .dataSource(dataSchema.getDataSource())
        .columns(rowSignature.getColumnNames())
        .virtualColumns(getVirtualColumns(dataSchema, interval))
        .columnTypes(rowSignature.getColumnTypes())
        .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(interval)))
        .filters(dataSchema.getTransformSpec().getFilter())
        .context(compactionTask.getContext());

    if (compactionTask.getTuningConfig() != null && compactionTask.getTuningConfig().getPartitionsSpec() != null) {
      List<OrderByColumnSpec> orderByColumnSpecs = getOrderBySpec(compactionTask.getTuningConfig().getPartitionsSpec());

      scanQueryBuilder.orderBy(
          orderByColumnSpecs
              .stream()
              .map(orderByColumnSpec ->
                       new ScanQuery.OrderBy(
                           orderByColumnSpec.getDimension(),
                           ScanQuery.Order.fromString(orderByColumnSpec.getDirection().toString())
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
      // duplicate rows are removed in line with native compaction.
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
   * Creates a virtual timestamp column to create a new __time field according to the provided queryGranularity, as
   * queryGranularity field itself is mandated to be ALL in MSQControllerTask.
   */
  private static VirtualColumns getVirtualColumns(DataSchema dataSchema, Interval interval)
  {
    if (isQueryGranularityEmptyOrNone(dataSchema)) {
      return VirtualColumns.EMPTY;
    }
    String virtualColumnExpr;
    if (dataSchema.getGranularitySpec()
                  .getQueryGranularity()
                  .equals(Granularities.ALL)) {
      // For ALL query granularity, all records in a segment are assigned the interval start timestamp of the segment.
      // It's the same behaviour in native compaction.
      virtualColumnExpr = StringUtils.format("timestamp_parse('%s')", interval.getStart());
    } else {
      PeriodGranularity periodQueryGranularity = (PeriodGranularity) dataSchema.getGranularitySpec()
                                                                               .getQueryGranularity();
      // Round of the __time column according to the required granularity.
      virtualColumnExpr =
          StringUtils.format(
              "timestamp_floor(\"%s\", '%s')",
              ColumnHolder.TIME_COLUMN_NAME,
              periodQueryGranularity.getPeriod().toString()
          );
    }
    return VirtualColumns.create(new ExpressionVirtualColumn(
        TIME_VIRTUAL_COLUMN,
        virtualColumnExpr,
        ColumnType.LONG,
        new ExprMacroTable(ImmutableList.of(new TimestampFloorExprMacro(), new TimestampParseExprMacro()))
    ));
  }

  private static Query<?> buildGroupByQuery(CompactionTask compactionTask, Interval interval, DataSchema dataSchema)
  {
    DimFilter dimFilter = dataSchema.getTransformSpec().getFilter();

    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(new TableDataSource(compactionTask.getDataSource()))
        .setVirtualColumns(getVirtualColumns(dataSchema, interval))
        .setDimFilter(dimFilter)
        .setGranularity(new AllGranularity())
        .setDimensions(getAggregateDimensions(dataSchema))
        .setAggregatorSpecs(Arrays.asList(dataSchema.getAggregators()))
        .setContext(compactionTask.getContext())
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
    context.put(QueryContexts.FINALIZE_KEY, false);
    // Only scalar or array-type dimensions are allowed as grouping keys.
    context.putIfAbsent(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false);
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
