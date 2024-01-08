package org.apache.druid.msq.compaction;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.coordinator.duty.CompactionClient;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@JsonTypeName()
public class MSQCompaction implements CompactionClient
{
  @JacksonInject
  OverlordClient overlordClient;

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
    Druids.ScanQueryBuilder builder = new Druids.ScanQueryBuilder()
        .dataSource(compactionParams.getDataSource())
        .columns()
        .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(compactionParams.getIoConfig()
                                                                                             .getInputSpec()
                                                                                             .getInterval())));


    Query<?> query = builder.build();

    RowSignature.Builder rowSignatureBuilder = RowSignature.builder();

    rowSignatureBuilder.addTimeColumn();

    for (DimensionSchema ds : compactionParams.getDimensionsSpec().getDimensions()) {
      rowSignatureBuilder.add(ds.getName(), ColumnType.fromString(ds.getTypeName()));
    }

    MSQDestination msqDestination = new DataSourceMSQDestination(
        compactionParams.getDataSource() + "-compacted",
        compactionParams.getGranularitySpec()
                        .getSegmentGranularity(),
        null,
        null
    );

    MSQSpec msqSpec = MSQSpec.builder()
                             .query(query)
                             .columnMappings(ColumnMappings.identity(rowSignatureBuilder.build()))
                             .destination(msqDestination)
                             .tuningConfig(MSQTuningConfig.defaultConfig())
                             .build();


    final String taskId = compactionParams.getId();


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
