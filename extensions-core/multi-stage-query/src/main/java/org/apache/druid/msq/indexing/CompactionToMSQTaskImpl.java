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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CompactionToMSQTask;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.java.util.common.NonnullPair;
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
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
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
import java.util.stream.Collectors;

public class CompactionToMSQTaskImpl implements CompactionToMSQTask
{
  private static final Logger log = new Logger(CompactionToMSQTaskImpl.class);
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;
  final OverlordClient overlordClient;
  final ObjectMapper jsonMapper;

  private static final String TIME_VIRTUAL_COLUMN = "vTime";
  private static final String TIME_COLUMN = ColumnHolder.TIME_COLUMN_NAME;

  @Inject
  public CompactionToMSQTaskImpl(final OverlordClient overlordClient, final ObjectMapper jsonMapper)
  {
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TaskStatus createAndRunMSQTasks(
      CompactionTask compactionTask,
      TaskToolbox taskToolbox,
      List<NonnullPair<Interval, DataSchema>> intervalDataSchemas
  ) throws JsonProcessingException
  {
    List<MSQControllerTask> msqControllerTasks = new ArrayList<>();
    QueryContext compactionTaskContext = new QueryContext(compactionTask.getContext());

    if (!MultiStageQueryContext.isFinalizeAggregations(compactionTaskContext)) {
      throw InvalidInput.exception(
          "finalizeAggregations=false currently not supported for auto-compaction with MSQ engine.");
    }

    for (NonnullPair<Interval, DataSchema> intervalDataSchema : intervalDataSchemas) {
      Query<?> query;
      Interval interval = intervalDataSchema.lhs;
      DataSchema ds = intervalDataSchema.rhs;

      if (!isGroupBy(ds)) {
        query = buildScanQuery(compactionTask, interval, ds);
      } else {
        query = buildGroupByQuery(compactionTask, interval, ds);
      }

      MSQSpec msqSpec = MSQSpec.builder()
                               .query(query)
                               .columnMappings(getColumnMappings(ds))
                               .destination(buildMSQDestination(compactionTask, ds, compactionTaskContext))
                               .assignmentStrategy(MultiStageQueryContext.getAssignmentStrategy(compactionTaskContext))
                               .tuningConfig(buildMSQTuningConfig(compactionTask, compactionTaskContext))
                               .build();

      Map<String, Object> msqControllerTaskContext = createMSQTaskContext(compactionTask, ds);

      MSQControllerTask controllerTask = new MSQControllerTask(
          compactionTask.getId(),
          msqSpec.withOverriddenContext(msqControllerTaskContext),
          null,
          msqControllerTaskContext,
          null,
          null,
          null,
          msqControllerTaskContext
      );

      // Doing a serde roundtrip for MSQControllerTask as the "injector" field of this class is supposed to be injected
      // by the mapper.
      MSQControllerTask serdedMSQControllerTask = jsonMapper.readerFor(MSQControllerTask.class)
                                                            .readValue(jsonMapper.writeValueAsString(controllerTask));
      msqControllerTasks.add(serdedMSQControllerTask);
    }

    if (msqControllerTasks.isEmpty()) {
      log.warn(
          "Can't find segments from inputSpec[%s], nothing to do.",
          compactionTask.getIoConfig().getInputSpec()
      );
    }
    return runSubtasks(
        msqControllerTasks,
        taskToolbox,
        compactionTask.getCurrentSubTaskHolder(),
        compactionTask.getId()
    );
  }

  private static DataSourceMSQDestination buildMSQDestination(
      CompactionTask compactionTask,
      DataSchema ds,
      QueryContext compactionTaskContext
  )
  {
    final Interval replaceInterval = compactionTask.getIoConfig()
                                                   .getInputSpec()
                                                   .findInterval(compactionTask.getDataSource());

    final List<String> segmentSortOrder = MultiStageQueryContext.getSortOrder(compactionTaskContext);

    return new DataSourceMSQDestination(
        ds.getDataSource(),
        ds.getGranularitySpec().getSegmentGranularity(),
        segmentSortOrder,
        ImmutableList.of(replaceInterval)
    );
  }

  private static MSQTuningConfig buildMSQTuningConfig(CompactionTask compactionTask, QueryContext compactionTaskContext)
  {

    // Transfer MSQ-related context params, if any, from the compaction context itself.
    final int maxNumTasks = MultiStageQueryContext.getMaxNumTasks(compactionTaskContext);
    if (maxNumTasks < 2) {
      throw InvalidInput.exception(
          "MSQ context maxNumTasks [%,d] cannot be less than 2, "
          + "since at least 1 controller and 1 worker is necessary.",
          maxNumTasks
      );
    }
    // This parameter is used internally for the number of worker tasks only, so we subtract 1
    final int maxNumWorkers = maxNumTasks - 1;
    final int maxRowsInMemory = MultiStageQueryContext.getRowsInMemory(compactionTaskContext);

    Integer rowsPerSegment = getRowsPerSegment(compactionTask);

    return new MSQTuningConfig(
        maxNumWorkers,
        maxRowsInMemory,
        rowsPerSegment,
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
    for (DimensionSchema ds : dataSchema.getDimensionsSpec().getDimensions()) {
      rowSignatureBuilder.add(ds.getName(), ColumnType.fromString(ds.getTypeName()));
    }
    return rowSignatureBuilder.build();
  }

  private static List<DimensionSpec> getAggregateDimensions(DataSchema ds)
  {
    List<DimensionSpec> dimensionSpecs = ds.getDimensionsSpec().getDimensions().stream()
                                           .map(dim -> new DefaultDimensionSpec(
                                               dim.getName(),
                                               dim.getName(),
                                               dim.getColumnType()
                                           ))
                                           .collect(Collectors.toList());


    // Dimensions in group-by aren't allowed to have time column as the output name.
    if (isQueryGranularityEmpty(ds)) {
      dimensionSpecs.add(new DefaultDimensionSpec(TIME_COLUMN, TIME_VIRTUAL_COLUMN, ColumnType.LONG));
    } else {
      // The changed granularity would result in a new virtual column that needs to be aggregated upon.
      dimensionSpecs.add(new DefaultDimensionSpec(TIME_VIRTUAL_COLUMN, TIME_VIRTUAL_COLUMN, ColumnType.LONG));
    }
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
    if (isGroupBy(dataSchema)) {
      // For group-by queries, time will always be one of the dimension. Since dimensions in groupby aren't allowed to
      // have time column as the output name, we map time dimension to a fixed column name in dimensions, and map it
      // back to the time column here.
      columnMappings.add(new ColumnMapping(TIME_VIRTUAL_COLUMN, TIME_COLUMN));
    } else {
      columnMappings.add(new ColumnMapping(TIME_COLUMN, TIME_COLUMN));
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
    return new Druids.ScanQueryBuilder().dataSource(dataSchema.getDataSource())
                                        .columns(rowSignature.getColumnNames())
                                        .columnTypes(rowSignature.getColumnTypes())
                                        .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(interval)))
                                        .legacy(false)
                                        .filters(dataSchema.getTransformSpec().getFilter())
                                        .context(compactionTask.getContext())
                                        .build();
  }

  private static boolean isGroupBy(DataSchema ds)
  {
    return ds.getAggregators().length > 0;
  }

  private static boolean isQueryGranularityEmpty(DataSchema ds)
  {
    return ds.getGranularitySpec() == null || ds.getGranularitySpec().getQueryGranularity() == null;
  }

  private static VirtualColumns getVirtualColumns(DataSchema ds)
  {
    VirtualColumns virtualColumns = VirtualColumns.EMPTY;

    if (!isQueryGranularityEmpty(ds) && !ds.getGranularitySpec().getQueryGranularity().equals(Granularities.ALL)) {
      PeriodGranularity periodQueryGranularity = (PeriodGranularity) ds.getGranularitySpec().getQueryGranularity();
      VirtualColumn virtualColumn = new ExpressionVirtualColumn(
          TIME_VIRTUAL_COLUMN,
          StringUtils.format(
              "timestamp_floor(\"%s\", '%s')",
              TIME_COLUMN,
              periodQueryGranularity.getPeriod().toString()
          ),
          ColumnType.LONG,
          new ExprMacroTable(Collections.singletonList(new TimestampFloorExprMacro()))
      );
      virtualColumns = VirtualColumns.create(virtualColumn);
    }
    return virtualColumns;
  }

  private static Query<?> buildGroupByQuery(CompactionTask compactionTask, Interval interval, DataSchema dataSchema)
  {
    DimFilter dimFilter = dataSchema.getTransformSpec().getFilter();

    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(new TableDataSource(compactionTask.getDataSource()))
        .setVirtualColumns(getVirtualColumns(dataSchema))
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

  private Map<String, Object> createMSQTaskContext(CompactionTask compactionTask, DataSchema ds)
      throws JsonProcessingException
  {
    Map<String, Object> context = new HashMap<>(compactionTask.getContext());
    context.put(
        DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
        jsonMapper.writeValueAsString(ds.getGranularitySpec() != null
                                      ? ds.getGranularitySpec()
                                          .getSegmentGranularity()
                                      : DEFAULT_SEGMENT_GRANULARITY)
    );
    if (!isQueryGranularityEmpty(ds)) {
      context.put(
          DruidSqlInsert.SQL_INSERT_QUERY_GRANULARITY,
          jsonMapper.writeValueAsString(ds.getGranularitySpec().getQueryGranularity())
      );
    }
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
