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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestHelper;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 */
public class RetrieveUsedSegmentsActionSerdeTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSingleIntervalSerde() throws Exception
  {
    Interval interval = Intervals.of("2014/2015");

    RetrieveUsedSegmentsAction expected =
        new RetrieveUsedSegmentsAction("dataSource", interval, null, Segments.ONLY_VISIBLE);

    RetrieveUsedSegmentsAction actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), RetrieveUsedSegmentsAction.class);
    Assert.assertEquals(ImmutableList.of(interval), actual.getIntervals());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMultiIntervalSerde() throws Exception
  {
    List<Interval> intervals = ImmutableList.of(Intervals.of("2014/2015"), Intervals.of("2016/2017"));
    RetrieveUsedSegmentsAction expected = new RetrieveUsedSegmentsAction(
        "dataSource",
        intervals
    );

    RetrieveUsedSegmentsAction actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), RetrieveUsedSegmentsAction.class);
    Assert.assertEquals(intervals, actual.getIntervals());
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testOldJsonDeserialization() throws Exception
  {
    String jsonStr = "{\"type\": \"segmentListUsed\", \"dataSource\": \"test\", \"interval\": \"2014/2015\"}";
    RetrieveUsedSegmentsAction actual = (RetrieveUsedSegmentsAction) MAPPER.readValue(jsonStr, TaskAction.class);

    Assert.assertEquals(
        new RetrieveUsedSegmentsAction("test", Intervals.of("2014/2015"), null, Segments.ONLY_VISIBLE),
        actual
    );
  }
}
