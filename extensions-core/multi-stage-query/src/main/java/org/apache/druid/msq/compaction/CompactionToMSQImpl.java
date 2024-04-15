package org.apache.druid.msq.compaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.CompactionToMSQ;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
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
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.indexing.OverlordClient;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompactionToMSQImpl implements CompactionToMSQ
{
  private static final Logger log = new Logger(CompactionToMSQImpl.class);
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularities.ALL;

  final OverlordClient overlordClient;
  final ObjectMapper jsonMapper;


  public CompactionToMSQImpl(
      final OverlordClient overlordClient,
      final ObjectMapper jsonMapper
  )
  {
    this.overlordClient = overlordClient;
    this.jsonMapper = jsonMapper;
    System.out.println("Initializing MSQCompaction");
  }

  RowSignature getRowSignature(DataSchema dataSchema)
  {
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();
    rowSignatureBuilder.add(dataSchema.getTimestampSpec().getTimestampColumn(), ColumnType.LONG);
    for (DimensionSchema ds : dataSchema.getDimensionsSpec().getDimensions()) {
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
    RowSignature rowSignature = getRowSignature(dataSchema);
    return new Druids.ScanQueryBuilder().dataSource(dataSchema.getDataSource())
                                        .columns(rowSignature.getColumnNames())
                                        .columnTypes(rowSignature.getColumnTypes())
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
    QueryContext sqlQueryContext = new QueryContext(compactionTask.getContext());

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
      Map<String, Object> context = new HashMap<>(compactionTask.getContext());
      // TODO (vishesh): check why the default is day in specific classes whereas ALL in query maker
      try {
        context.put(
            DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
            (ds != null && ds.getGranularitySpec() != null)
            ? jsonMapper.writeValueAsString(ds.getGranularitySpec().getSegmentGranularity())
            : jsonMapper.writeValueAsString(DEFAULT_SEGMENT_GRANULARITY
            )
        );
      }
      catch (JsonProcessingException e) {
        throw DruidException.defensive()
                            .build(
                                e,
                                "Unable to deserialize the DEFAULT_SEGMENT_GRANULARITY in MSQTaskQueryMaker. "
                                + "This shouldn't have happened since the DEFAULT_SEGMENT_GRANULARITY object is guaranteed to be "
                                + "serializable. Please raise an issue in case you are seeing this message while executing a query."
                            );

      }

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

//      // TODO (vishesh) : There is some logic related to finalize aggs. Check if there's a case
//       // for non-finalize aggs for compaction -- esp. user specified.
//      final int maxNumTasks = 2;
      /*
          For assistance computing return types if !finalizeAggregations.
    final Map<String, ColumnType> aggregationIntermediateTypeMap =
        finalizeAggregations ? null : buildAggregationIntermediateTypeMap(druidQuery);

      final List<SqlTypeName> sqlTypeNames = new ArrayList<>();
      final List<ColumnType> columnTypeList = new ArrayList<>();
      final List<ColumnMapping> columnMappings = QueryUtils.buildColumnMappings(fieldMapping, druidQuery);

      for (final org.apache.calcite.util.Pair<Integer, String> entry : fieldMapping) {
        final String queryColumn = druidQuery.getOutputRowSignature().getColumnName(entry.getKey());

        final SqlTypeName sqlTypeName;

        if (!finalizeAggregations && aggregationIntermediateTypeMap.containsKey(queryColumn)) {
          final ColumnType druidType = aggregationIntermediateTypeMap.get(queryColumn);
          sqlTypeName = new RowSignatures.ComplexSqlType(SqlTypeName.OTHER, druidType, true).getSqlTypeName();
        } else {
          sqlTypeName = druidQuery.getOutputRowType().getFieldList().get(entry.getKey()).getType().getSqlTypeName();
        }
        sqlTypeNames.add(sqlTypeName);
        columnTypeList.add(druidQuery.getOutputRowSignature().getColumnType(queryColumn).orElse(ColumnType.STRING));
      }
       */

      // Pick-up MSQ related context params, if any, from the compaction context itself.
      final int maxNumTasks = MultiStageQueryContext.getMaxNumTasks(sqlQueryContext);
      if (maxNumTasks < 2) {
        throw InvalidInput.exception(
            "MSQ context maxNumTasks [%,d] cannot be less than 2, since at least 1 controller and 1 worker is necessary",
            maxNumTasks
        );
      }

      // This parameter is used internally for the number of worker tasks only, so we subtract 1
      final int maxNumWorkers = maxNumTasks - 1;
      final int maxRowsInMemory = MultiStageQueryContext.getRowsInMemory(sqlQueryContext);
      final boolean finalizeAggregations = MultiStageQueryContext.isFinalizeAggregations(sqlQueryContext);


      Integer rowsPerSegment = null;
      if (compactionTask.getTuningConfig() != null){
        PartitionsSpec partitionsSpec = compactionTask.getTuningConfig().getPartitionsSpec();
        if (partitionsSpec != null) {
          rowsPerSegment = partitionsSpec.getMaxRowsPerSegment();
        }
      }

      MSQTuningConfig msqTuningConfig = new MSQTuningConfig(
          maxNumWorkers,
          maxRowsInMemory,
          rowsPerSegment,
          compactionTask.getTuningConfig().getIndexSpec()
      );

      MSQSpec msqSpec = MSQSpec.builder()
                               .query(query)
                               .columnMappings(ColumnMappings.identity(getRowSignature(ds)))
                               .destination(msqDestination)
                               // TODO (vishesh): Investiage how this is populated
                               .assignmentStrategy(MultiStageQueryContext.getAssignmentStrategy(sqlQueryContext))
                               .tuningConfig(msqTuningConfig)
                               .build();

      MSQControllerTask controllerTask = new MSQControllerTask(
          compactionTask.getId(),
          compactionTask.getId(),
          msqSpec.withOverriddenContext(context),
          null,
          context,
          null,
          null,
          null,
          null
      );
      MSQControllerTask serdeControllerTask = jsonMapper.readerFor(MSQControllerTask.class).readValue(jsonMapper.writeValueAsString(controllerTask));
      msqControllerTasks.add(serdeControllerTask);
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
          // TODO (vishesh) : Check handling of multi-controller tasks -- to be triggered via overlord
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
