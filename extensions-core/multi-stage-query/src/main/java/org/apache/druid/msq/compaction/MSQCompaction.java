package org.apache.druid.msq.compaction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordinator.duty.CompactionClient;
import org.apache.druid.sql.calcite.parser.DruidSqlReplace;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//@JsonTypeName()
public class MSQCompaction implements CompactionClient
{
  public MSQCompaction(){
    System.out.println("Initializing MSQCompaction");
  }
  @JacksonInject
  OverlordClient overlordClient;

  @Override
  public void setOverlordClient(OverlordClient overlordClient)
  {
    this.overlordClient = overlordClient;
  }

  @Override
  public String submitCompactionTask(ClientCompactionTaskQuery compactionParams)
  {

/*
    GroupByQuery.Builder builder = new GroupByQuery.Builder().setGranularity(compactionParams.getGranularitySpec()
                                                                                             .getSegmentGranularity())
                                                             .setInterval(compactionParams.getIoConfig()
                                                                                          .getInputSpec()
                                                                                          .getInterval())
                                                             .setDataSource(compactionParams.getDataSource())
                                                             .setDimensions(compactionParams.getDimensionsSpec()
                                                                                            .getDimensions()
                                                                                            .stream()
                                                                                            .map(d -> new DefaultDimensionSpec(
                                                                                                d.getName(),
                                                                                                d.getName()
                                                                                            ))
                                                                                            .collect(Collectors.toList()));

    if (compactionParams.getMetricsSpec() != null) {
      builder.setAggregatorSpecs(compactionParams.getMetricsSpec());
    }
*/
//    String escapedJson = "[{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"channel\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"comment\",\"type\":\"STRING\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},{\"name\":\"countryName\",\"type\":\"STRING\"},{\"name\":\"deleted\",\"type\":\"LONG\"},{\"name\":\"delta\",\"type\":\"LONG\"},{\"name\":\"isAnonymous\",\"type\":\"STRING\"},{\"name\":\"isMinor\",\"type\":\"STRING\"},{\"name\":\"isNew\",\"type\":\"STRING\"},{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"isUnpatrolled\",\"type\":\"STRING\"},{\"name\":\"metroCode\",\"type\":\"LONG\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"page\",\"type\":\"STRING\"},{\"name\":\"regionIsoCode\",\"type\":\"STRING\"},{\"name\":\"regionName\",\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"},{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"time\",\"type\":\"LONG\"}]";
    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();

//    rowSignatureBuilder.addTimeColumn();
    List<String> columns = new ArrayList<>();

    for (DimensionSchema ds : compactionParams.getDimensionsSpec().getDimensions()) {
      rowSignatureBuilder.add(ds.getName(), ColumnType.fromString(ds.getTypeName()));
      columns.add(ds.getName());
    }

    Interval replaceInterval = compactionParams.getIoConfig()
                                               .getInputSpec()
                                               .getInterval();

    MultipleIntervalSegmentSpec multipleIntervalSegmentSpecFull = new MultipleIntervalSegmentSpec(Collections.singletonList(
        Intervals.ETERNITY));
    MultipleIntervalSegmentSpec multipleIntervalSegmentSpecQuery = new MultipleIntervalSegmentSpec(Collections.singletonList(compactionParams.getIoConfig()
                                                                                                                                             .getInputSpec()
                                                                                                                                             .getInterval()));


    String escapedJson = "[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"added\",\"type\":\"LONG\"},{\"name\":\"channel\",\"type\":\"STRING\"},{\"name\":\"cityName\",\"type\":\"STRING\"},{\"name\":\"comment\",\"type\":\"STRING\"},{\"name\":\"countryIsoCode\",\"type\":\"STRING\"},{\"name\":\"countryName\",\"type\":\"STRING\"},{\"name\":\"deleted\",\"type\":\"LONG\"},{\"name\":\"delta\",\"type\":\"LONG\"},{\"name\":\"isAnonymous\",\"type\":\"STRING\"},{\"name\":\"isMinor\",\"type\":\"STRING\"},{\"name\":\"isNew\",\"type\":\"STRING\"},{\"name\":\"isRobot\",\"type\":\"STRING\"},{\"name\":\"isUnpatrolled\",\"type\":\"STRING\"},{\"name\":\"metroCode\",\"type\":\"LONG\"},{\"name\":\"namespace\",\"type\":\"STRING\"},{\"name\":\"page\",\"type\":\"STRING\"},{\"name\":\"regionIsoCode\",\"type\":\"STRING\"},{\"name\":\"regionName\",\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"}]";
    Druids.ScanQueryBuilder builder = new Druids.ScanQueryBuilder()
        .dataSource(compactionParams.getDataSource())
        .columns(columns)
//        .columns("__time", "added", "channel", "cityName", "comment", "countryIsoCode", "countryName", "deleted", "delta", "isAnonymous", "isMinor", "isNew", "isRobot", "isUnpatrolled", "metroCode", "namespace", "page", "regionIsoCode", "regionName", "user")
        .intervals(multipleIntervalSegmentSpecQuery)
        .legacy(false)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .context(ImmutableMap.of(DruidQuery.CTX_SCAN_SIGNATURE, escapedJson, DruidSqlReplace.SQL_REPLACE_TIME_CHUNKS, replaceInterval.toString(), "sqlInsertSegmentGranularity", "\"HOUR\""));


// QuerySegmentSpec (intervals) for SQL initiated reingest: MultipleIntervalSegmentSpec{intervals=[-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z]}
    Query<?> query = builder.build();


    MSQDestination msqDestination = new DataSourceMSQDestination(
        compactionParams.getDataSource(),
        compactionParams.getGranularitySpec()
                        .getSegmentGranularity(),
        null,
        ImmutableList.of(replaceInterval)

    );

    MSQSpec msqSpec = MSQSpec.builder()
                             .query(query)
                             .columnMappings(ColumnMappings.identity(rowSignatureBuilder.build()))
                             .destination(msqDestination)
                             .tuningConfig(MSQTuningConfig.defaultConfig())

                             .build();


    final String taskId = compactionParams.getId();

//    Map<String, Object> context = compactionParams.getContext();
//    context.put(DruidQuery.CTX_SCAN_SIGNATURE, msqSpec.getColumnMappings());



    MSQControllerTask controllerTask =
        new MSQControllerTask(
            taskId,
            msqSpec,
            null,
            null,
            null,
            null,
            null,
            compactionParams.getContext()
        );

    FutureUtils.getUnchecked(overlordClient.runTask(taskId, controllerTask), true);

    return taskId;
  }
}
