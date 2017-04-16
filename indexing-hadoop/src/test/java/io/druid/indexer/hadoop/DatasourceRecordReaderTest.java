/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.hadoop;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

/**
 */
public class DatasourceRecordReaderTest
{
  @Test
  public void testSanity() throws Exception
  {
    DataSegment segment = new DefaultObjectMapper()
        .readValue(this.getClass().getClassLoader().getResource("test-segment/descriptor.json"), DataSegment.class)
        .withLoadSpec(
            ImmutableMap.<String, Object>of(
                "type",
                "local",
                "path",
                this.getClass().getClassLoader().getResource("test-segment/index.zip").getPath()
            )
        );
    InputSplit split = new DatasourceInputSplit(Lists.newArrayList(WindowedDataSegment.of(segment)), null);

    Configuration config = new Configuration();
    config.set(
        DatasourceInputFormat.CONF_DRUID_SCHEMA,
        HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(
            new DatasourceIngestionSpec(
                segment.getDataSource(),
                segment.getInterval(),
                null,
                null,
                null,
                segment.getDimensions(),
                segment.getMetrics(),
                false
            )
        )
    );

    TaskAttemptContext context = EasyMock.createNiceMock(TaskAttemptContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config).anyTimes();
    EasyMock.replay(context);

    DatasourceRecordReader rr = new DatasourceRecordReader();
    rr.initialize(split, context);

    Assert.assertEquals(0, rr.getProgress(), 0.0001);

    List<InputRow> rows = Lists.newArrayList();
    while(rr.nextKeyValue()) {
      rows.add(rr.getCurrentValue());
    }
    verifyRows(rows);

    Assert.assertEquals(1, rr.getProgress(), 0.0001);

    rr.close();
  }

  private void verifyRows(List<InputRow> actualRows)
  {
    List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T00:00:00.000Z"),
            "host", ImmutableList.of("a.example.com"),
            "visited_sum", 100L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T01:00:00.000Z"),
            "host", ImmutableList.of("b.example.com"),
            "visited_sum", 150L,
            "unique_hosts", 1.0d
        ),
        ImmutableMap.<String, Object>of(
            "time", DateTime.parse("2014-10-22T02:00:00.000Z"),
            "host", ImmutableList.of("c.example.com"),
            "visited_sum", 200L,
            "unique_hosts", 1.0d
        )
    );

    Assert.assertEquals(expectedRows.size(), actualRows.size());

    for (int i = 0; i < expectedRows.size(); i++) {
      Map<String, Object> expected = expectedRows.get(i);
      InputRow actual = actualRows.get(i);

      Assert.assertEquals(ImmutableList.of("host"), actual.getDimensions());

      Assert.assertEquals(expected.get("time"), actual.getTimestamp());
      Assert.assertEquals(expected.get("host"), actual.getDimension("host"));
      Assert.assertEquals(expected.get("visited_sum"), actual.getLongMetric("visited_sum"));
      Assert.assertEquals(
          (Double) expected.get("unique_hosts"),
          (Double) HyperUniquesAggregatorFactory.estimateCardinality(actual.getRaw("unique_hosts")),
          0.001
      );
    }
  }
}
