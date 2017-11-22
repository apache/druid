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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.Intervals;
import io.druid.segment.TestHelper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class WindowedDataSegmentTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final DataSegment SEGMENT = new DataSegment(
      "test1",
      Intervals.of("2000/3000"),
      "ver",
      ImmutableMap.<String, Object>of(
          "type", "local",
          "path", "/tmp/index1.zip"
      ),
      ImmutableList.of("host"),
      ImmutableList.of("visited_sum", "unique_hosts"),
      NoneShardSpec.instance(),
      9,
      2
  );

  @Test
  public void testSerdeFullWindow() throws IOException
  {
    final WindowedDataSegment windowedDataSegment = WindowedDataSegment.of(SEGMENT);
    final WindowedDataSegment roundTrip = MAPPER.readValue(
        MAPPER.writeValueAsBytes(windowedDataSegment),
        WindowedDataSegment.class
    );
    Assert.assertEquals(windowedDataSegment, roundTrip);
    Assert.assertEquals(SEGMENT, roundTrip.getSegment());
    Assert.assertEquals(SEGMENT.getInterval(), roundTrip.getInterval());
  }

  @Test
  public void testSerdePartialWindow() throws IOException
  {
    final Interval partialInterval = Intervals.of("2500/3000");
    final WindowedDataSegment windowedDataSegment = new WindowedDataSegment(SEGMENT, partialInterval);
    final WindowedDataSegment roundTrip = MAPPER.readValue(
        MAPPER.writeValueAsBytes(windowedDataSegment),
        WindowedDataSegment.class
    );
    Assert.assertEquals(windowedDataSegment, roundTrip);
    Assert.assertEquals(SEGMENT, roundTrip.getSegment());
    Assert.assertEquals(partialInterval, roundTrip.getInterval());
  }
}
