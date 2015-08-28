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
import com.google.common.collect.Sets;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 */
public class DatasourceInputFormatTest
{
  private List<WindowedDataSegment> segments;
  private Configuration config;
  private JobContext context;

  @Before
  public void setUp() throws Exception
  {
    segments = ImmutableList.of(
        WindowedDataSegment.of(
            new DataSegment(
                "test1",
                Interval.parse("2000/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "local",
                    "path", "/tmp/index1.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                new NoneShardSpec(),
                9,
                2
            )
        ),
        WindowedDataSegment.of(
            new DataSegment(
                "test2",
                Interval.parse("2050/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "hdfs",
                    "path", "/tmp/index2.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                new NoneShardSpec(),
                9,
                11
            )
        ),
        WindowedDataSegment.of(
            new DataSegment(
                "test3",
                Interval.parse("2030/3000"),
                "ver",
                ImmutableMap.<String, Object>of(
                    "type", "hdfs",
                    "path", "/tmp/index3.zip"
                ),
                ImmutableList.of("host"),
                ImmutableList.of("visited_sum", "unique_hosts"),
                new NoneShardSpec(),
                9,
                4
            )
        )
    );

    config = new Configuration();
    config.set(
        DatasourceInputFormat.CONF_INPUT_SEGMENTS,
        new DefaultObjectMapper().writeValueAsString(segments)
    );

    context = EasyMock.createMock(JobContext.class);
    EasyMock.expect(context.getConfiguration()).andReturn(config);
    EasyMock.replay(context);
  }

  @Test
  public void testGetSplitsNoCombining() throws Exception
  {
    List<InputSplit> splits = new DatasourceInputFormat().getSplits(context);

    Assert.assertEquals(segments.size(), splits.size());
    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(segments.get(i), ((DatasourceInputSplit) splits.get(i)).getSegments().get(0));
    }
  }

  @Test
  public void testGetSplitsAllCombined() throws Exception
  {
    config.set(DatasourceInputFormat.CONF_MAX_SPLIT_SIZE, "999999");
    List<InputSplit> splits = new DatasourceInputFormat().getSplits(context);

    Assert.assertEquals(1, splits.size());
    Assert.assertEquals(
        Sets.newHashSet(segments),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );
  }

  @Test
  public void testGetSplitsCombineInTwo() throws Exception
  {
    config.set(DatasourceInputFormat.CONF_MAX_SPLIT_SIZE, "6");
    List<InputSplit> splits = new DatasourceInputFormat().getSplits(context);

    Assert.assertEquals(2, splits.size());

    Assert.assertEquals(
        Sets.newHashSet(segments.get(0), segments.get(2)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(0)).getSegments()))
    );

    Assert.assertEquals(
        Sets.newHashSet(segments.get(1)),
        Sets.newHashSet((((DatasourceInputSplit) splits.get(1)).getSegments()))
    );
  }

  @Test
  public void testGetRecordReader() throws Exception
  {
    Assert.assertTrue(new DatasourceInputFormat().createRecordReader(null, null) instanceof DatasourceRecordReader);
  }
}
