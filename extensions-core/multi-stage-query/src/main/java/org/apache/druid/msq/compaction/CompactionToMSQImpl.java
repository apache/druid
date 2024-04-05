package org.apache.druid.msq.compaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CompactionToMSQ;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompactionToMSQImpl implements CompactionToMSQ
{
  private static final Logger log = new Logger(CompactionToMSQImpl.class);
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;


  public CompactionToMSQImpl()
  {
    System.out.println("Initializing MSQCompaction");
  }

  List<String> getColumns(DimensionsSpec dimensionSpec)
  {

    List<String> columns = new ArrayList<>();
    for (DimensionSchema ds : dimensionSpec.getDimensions()) {
      columns.add(ds.getName());
    }
    return columns;
  }

  RowSignature getRowSignature(DimensionsSpec dimensionSpec)
  {
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();

    for (DimensionSchema ds : dimensionSpec.getDimensions()) {
      rowSignatureBuilder.add(ds.getName(), ColumnType.fromString(ds.getTypeName()));
    }
    return rowSignatureBuilder.build();
  }

  public MultipleIntervalSegmentSpec createMultipleIntervalSpec(Interval interval)
  {
    return new MultipleIntervalSegmentSpec(Collections.singletonList(interval));

  }

  public List<OrderByColumnSpec> getOrderBySpec(PartitionsSpec partitionSpec)
  {
    if (partitionSpec.getType() == SecondaryPartitionType.RANGE) {
      List<String> dimensions = ((DimensionRangePartitionsSpec) partitionSpec).getPartitionDimensions();
      return dimensions.stream()
                       .map(dim -> new OrderByColumnSpec(dim, OrderByColumnSpec.Direction.ASCENDING))
                       .collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  public Query<?> buildScanQuery(CompactionTask compactionTask, Interval interval, DataSchema dataSchema)
  {
    return new Druids.ScanQueryBuilder().dataSource(dataSchema.getDataSource())
                                        .columns(getColumns(dataSchema.getDimensionsSpec()))
                                        .intervals(createMultipleIntervalSpec(interval))
                                        .legacy(false)
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .filters(dataSchema.getTransformSpec().getFilter())
                                        .context(compactionTask.getContext())
                                        .build();
  }

  public Query<?> buildGroupByQuery(CompactionTask compactionTask, Interval interval, DataSchema dataSchema)
  {

    DimFilter dimFilter = dataSchema.getTransformSpec().getFilter();

    GroupByQuery.Builder builder = new GroupByQuery.Builder()
        .setDataSource(new TableDataSource(compactionTask.getDataSource()))
        .setVirtualColumns(VirtualColumns.EMPTY)
        .setDimFilter(dimFilter)
        .setGranularity(dataSchema.getGranularitySpec().getQueryGranularity() != null
                        ? dataSchema.getGranularitySpec()
                                    .getQueryGranularity()
                        : new AllGranularity()
        )
        .setDimensions(dataSchema.getDimensionsSpec()
                                 .getDimensions()
                                 .stream()
                                 .map(d -> new DefaultDimensionSpec(d.getName(), d.getName()))
                                 .collect(Collectors.toList())
        )
        .setAggregatorSpecs(Arrays.asList(dataSchema.getAggregators()))
        .setContext(compactionTask.getContext())
        .setInterval(interval);

    if (compactionTask.getTuningConfig() != null && compactionTask.getTuningConfig().getPartitionsSpec() != null) {
      getOrderBySpec(compactionTask.getTuningConfig().getPartitionsSpec()).forEach(builder::addOrderByColumn);
    }
    return builder.build();
  }

  @Override
  public TaskStatus createAndRunMSQControllerTasks(
      CompactionTask compactionTask, TaskToolbox taskToolbox, List<Pair<Interval, DataSchema>> intervalDataSchemas
  ) throws Exception
  {

    List<MSQControllerTask> msqControllerTasks = new ArrayList<>();

    for (Pair<Interval, DataSchema> intervalDataSchema : intervalDataSchemas) {
      Query<?> query;
      Interval interval = intervalDataSchema.lhs;
      DataSchema ds = intervalDataSchema.rhs;
      final Interval replaceInterval = compactionTask.getIoConfig()
                                                     .getInputSpec()
                                                     .findInterval(compactionTask.getDataSource());
//      this.escapedRowSignature = "[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"added\",\"type\":\"LONG\"},"
//                                 + "{\"name\":\"channel\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"comment\",\"type\":\"STRING\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"countryName\",\"type\":\"STRING\"},{\"name\":\"deleted\",\"type\":\"LONG\"},"
//                                 + "{\"name\":\"delta\",\"type\":\"LONG\"},{\"name\":\"isAnonymous\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"isMinor\",\"type\":\"STRING\"},{\"name\":\"isNew\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"isUnpatrolled\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"metroCode\",\"type\":\"LONG\"},{\"name\":\"namespace\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"page\",\"type\":\"STRING\"},{\"name\":\"regionIsoCode\",\"type\":\"STRING\"},"
//                                 + "{\"name\":\"regionName\",\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"}]";

      boolean isGroupBy = ds != null && ds.getAggregators().length > 0;

      // CTX_SCAN_SIGNATURE is deprecated and should no longer be reqd
//      compactionTask.getContext().putAll(ImmutableMap.of(DruidQuery.CTX_SCAN_SIGNATURE,
//                                                         new ObjectMapper().writeValueAsString(getRowSignature(ds.getDimensionsSpec())),
      Map<String, Object> context = ImmutableMap.copyOf(compactionTask.getContext());
      // TODO (vishesh): check why the default is day in specific classes whereas ALL in query maker
      context.put(
          DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
          (ds.getGranularitySpec() != null)
          ? ds.getGranularitySpec().getSegmentGranularity()
          : DEFAULT_SEGMENT_GRANULARITY
      );
      context.put(DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS, interval.toString());
      context.put(Tasks.STORE_COMPACTION_STATE_KEY, true);

      if (!isGroupBy) {
        query = buildScanQuery(compactionTask, interval, ds);
      } else {
        query = buildGroupByQuery(compactionTask, interval, ds);
      }

      MSQDestination msqDestination = new DataSourceMSQDestination(
          ds.getDataSource(),
          ds.getGranularitySpec().getSegmentGranularity(),
          null,
          ImmutableList.of(replaceInterval)

      );

      PartitionsSpec partitionsSpec = compactionTask.getTuningConfig().getPartitionsSpec();

      Integer rowsPerSegment = null;

      if (partitionsSpec instanceof DimensionRangePartitionsSpec) {
        rowsPerSegment = ((DimensionRangePartitionsSpec) partitionsSpec).getTargetRowsPerSegment();

      }

      MSQTuningConfig msqTuningConfig = new MSQTuningConfig(
          null,
          null,
          rowsPerSegment,
          compactionTask.getTuningConfig().getIndexSpec()
      );

      MSQSpec msqSpec = MSQSpec.builder()
                               .query(query)
                               .columnMappings(ColumnMappings.identity(getRowSignature(ds.getDimensionsSpec())))
                               .destination(msqDestination)
                               .tuningConfig(msqTuningConfig)
                               .build();

      MSQControllerTask controllerTask = getMsqControllerTask(compactionTask, msqSpec);

      msqControllerTasks.add(controllerTask);
    }
//    if (msqControllerTasks.isEmpty()){
//      log.warn("Can't find segments from inputSpec[%s], nothing to do.",
//               ioConfig.getInputSpec())
//    }
    return runSubtasks(
        msqControllerTasks,
        taskToolbox,
        compactionTask.getCurrentSubTaskHolder(),
        compactionTask.getId()
    );
  }

  private TaskStatus runSubtasks(
      List<MSQControllerTask> tasks,
      TaskToolbox toolbox,
      CurrentSubTaskHolder currentSubTaskHolder,
      String compactionTaskId
  ) throws JsonProcessingException
  {
    final int totalNumSpecs = tasks.size();
    log.info("Generated [%d] compaction task specs", totalNumSpecs);

    int failCnt = 0;
//    Map<String, TaskReport> completionReports = new HashMap<>();
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

//    toolbox.getTaskReportFileWriter().write(compactionTaskId, completionReports);
    log.info(msg);
    return failCnt == 0 ? TaskStatus.success(compactionTaskId) : TaskStatus.failure(compactionTaskId, msg);
  }


  private static MSQControllerTask getMsqControllerTask(CompactionTask compactionTask, MSQSpec msqSpec)
  {
    final String taskId = compactionTask.getId();

    return new MSQControllerTask(
        taskId,
        msqSpec,
        null,
        null,
        null,
        null,
        null,
        compactionTask.getContext()
    );
  }
}
