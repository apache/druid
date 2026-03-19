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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class MinorCompactionInputSpecTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    Interval interval = Intervals.of("2015-04-11/2015-04-12");
    List<SegmentDescriptor> segments = List.of(
        new SegmentDescriptor(Intervals.of("2015-04-11/2015-04-12"), "v1", 0)
    );

    MinorCompactionInputSpec spec = new MinorCompactionInputSpec(interval, segments);
    String json = mapper.writeValueAsString(spec);
    MinorCompactionInputSpec deserialized = mapper.readValue(json, MinorCompactionInputSpec.class);

    Assert.assertEquals(spec, deserialized);
    Assert.assertEquals(interval, deserialized.getInterval());
    Assert.assertEquals(segments, deserialized.getSegmentsToCompact());
  }

  @Test
  public void testDeserializeFromClientFormat() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    String clientJson = "{"
                        + "\"type\":\"uncompacted\","
                        + "\"interval\":\"2015-04-11/2015-04-12\","
                        + "\"uncompactedSegments\":[{\"itvl\":\"2015-04-11/2015-04-12\",\"ver\":\"v1\",\"part\":0}]"
                        + "}";

    MinorCompactionInputSpec deserialized = mapper.readValue(clientJson, MinorCompactionInputSpec.class);

    Assert.assertEquals(Intervals.of("2015-04-11/2015-04-12"), deserialized.getInterval());
    Assert.assertEquals(1, deserialized.getSegmentsToCompact().size());
    Assert.assertEquals(
        new SegmentDescriptor(Intervals.of("2015-04-11/2015-04-12"), "v1", 0),
        deserialized.getSegmentsToCompact().get(0)
    );
  }

  @Test
  public void testThrowsExceptionWhenInvalidInterval()
  {
    List<SegmentDescriptor> segments = List.of(
        new SegmentDescriptor(Intervals.of("2015-04-11/2015-04-12"), "v1", 0)
    );

    Assert.assertThrows(DruidException.class, () -> new MinorCompactionInputSpec(null, segments));

    Interval emptyInterval = Intervals.of("2015-04-11/2015-04-11");
    Assert.assertThrows(DruidException.class, () -> new MinorCompactionInputSpec(emptyInterval, segments));
  }

  @Test
  public void testThrowsExceptionWhenInvalidSegments()
  {
    Interval interval = Intervals.of("2015-04-11/2015-04-12");
    Assert.assertThrows(DruidException.class, () -> new MinorCompactionInputSpec(interval, null));
    Assert.assertThrows(DruidException.class, () -> new MinorCompactionInputSpec(interval, List.of()));
  }

  @Test
  public void testThrowsExceptionWhenSegmentsOutsideInterval()
  {
    Interval interval = Intervals.of("2015-04-11/2015-04-12");
    List<SegmentDescriptor> segments = List.of(
        new SegmentDescriptor(Intervals.of("2015-05-11/2015-05-12"), "v1", 0)
    );

    Assert.assertThrows(DruidException.class, () -> new MinorCompactionInputSpec(interval, segments));
  }
}
