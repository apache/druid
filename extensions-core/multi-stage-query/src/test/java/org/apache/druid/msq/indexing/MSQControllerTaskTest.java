package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MSQControllerTaskTest
{
  MSQSpec MSQ_SPEC = MSQSpec
      .builder()
      .destination(new DataSourceMSQDestination(
          "target",
          Granularities.DAY,
          null,
          null
      ))
      .query(new Druids.ScanQueryBuilder()
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .legacy(false)
                 .intervals(new MultipleIntervalSegmentSpec(
                     Collections.singletonList(Intervals.of(
                         "2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))))
                 .dataSource("target")
                 .build()
      )
      .columnMappings(new ColumnMappings(ImmutableList.of(new ColumnMapping("a0", "cnt"))))
      .tuningConfig(MSQTuningConfig.defaultConfig())
      .build();

  @Test
  public void testGetInputSourceResources()
  {
    MSQControllerTask msqWorkerTask = new MSQControllerTask(
        null,
        MSQ_SPEC,
        null,
        null,
        null,
        null);
    Assert.assertTrue(msqWorkerTask.getInputSourceResources().isEmpty());
  }
}
