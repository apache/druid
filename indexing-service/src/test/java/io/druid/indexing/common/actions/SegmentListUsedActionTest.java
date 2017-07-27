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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.druid.TestUtil;
import io.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class SegmentListUsedActionTest
{
  private static final ObjectMapper MAPPER = TestUtil.MAPPER;

  @Test
  public void testSingleIntervalSerde() throws Exception
  {
    Interval interval = Intervals.of("2014/2015");

    SegmentListUsedAction expected = new SegmentListUsedAction(
        "dataSource",
        interval,
        null
    );

    SegmentListUsedAction actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), SegmentListUsedAction.class);
    Assert.assertEquals(ImmutableList.of(interval), actual.getIntervals());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultiIntervalSerde() throws Exception
  {
    List<Interval> intervals = ImmutableList.of(Intervals.of("2014/2015"), Intervals.of("2016/2017"));
    SegmentListUsedAction expected = new SegmentListUsedAction(
        "dataSource",
        null,
        intervals
    );

    SegmentListUsedAction actual = MAPPER.readValue(MAPPER.writeValueAsString(expected), SegmentListUsedAction.class);
    Assert.assertEquals(intervals, actual.getIntervals());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOldJsonDeserialization() throws Exception
  {
    String jsonStr = "{\"type\": \"segmentListUsed\", \"dataSource\": \"test\", \"interval\": \"2014/2015\"}";
    SegmentListUsedAction actual = (SegmentListUsedAction) MAPPER.readValue(jsonStr, TaskAction.class);

    Assert.assertEquals(new SegmentListUsedAction("test", Intervals.of("2014/2015"), null), actual);
  }
}
