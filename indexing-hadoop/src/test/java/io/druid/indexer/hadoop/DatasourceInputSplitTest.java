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
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;

/**
 */
public class DatasourceInputSplitTest
{
  @Test
  public void testSerde() throws Exception
  {
    Interval interval = Interval.parse("2000/3000");
    DatasourceInputSplit expected = new DatasourceInputSplit(
        Lists.newArrayList(
            new WindowedDataSegment(
                new DataSegment(
                    "test",
                    Interval.parse("2000/3000"),
                    "ver",
                    ImmutableMap.<String, Object>of(
                        "type", "local",
                        "path", "/tmp/index.zip"
                    ),
                    ImmutableList.of("host"),
                    ImmutableList.of("visited_sum", "unique_hosts"),
                    new NoneShardSpec(),
                    9,
                    12334
                ),
                interval
            )
        ),
        new String[] { "server1", "server2", "server3"}
    );

    ByteArrayDataOutput out = ByteStreams.newDataOutput();
    expected.write(out);

    DataInput in = ByteStreams.newDataInput(out.toByteArray());
    DatasourceInputSplit actual = new DatasourceInputSplit();
    actual.readFields(in);

    Assert.assertEquals(expected.getSegments(), actual.getSegments());
    Assert.assertArrayEquals(expected.getLocations(), actual.getLocations());
    Assert.assertEquals(12334, actual.getLength());
  }
}
