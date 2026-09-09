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
import org.apache.druid.timeline.SegmentDetail;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.EnumSet;
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

    RetrieveUsedSegmentsAction expected = new RetrieveUsedSegmentsAction(
        "dataSource",
        Collections.singletonList(interval),
        Segments.ONLY_VISIBLE,
        SegmentDetail.all()
    );

    RetrieveUsedSegmentsAction actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), RetrieveUsedSegmentsAction.class);
    Assertions.assertEquals(ImmutableList.of(interval), actual.getIntervals());
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMultiIntervalSerde() throws Exception
  {
    List<Interval> intervals = ImmutableList.of(Intervals.of("2014/2015"), Intervals.of("2016/2017"));
    RetrieveUsedSegmentsAction expected = new RetrieveUsedSegmentsAction(
        "dataSource",
        intervals,
        SegmentDetail.none()
    );

    RetrieveUsedSegmentsAction actual =
        MAPPER.readValue(MAPPER.writeValueAsString(expected), RetrieveUsedSegmentsAction.class);
    Assertions.assertEquals(intervals, actual.getIntervals());
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testPartialDetailsSerde() throws Exception
  {
    RetrieveUsedSegmentsAction expected = new RetrieveUsedSegmentsAction(
        "dataSource",
        Collections.singletonList(Intervals.of("2014/2015")),
        Segments.INCLUDING_OVERSHADOWED,
        EnumSet.of(SegmentDetail.LOAD_SPEC, SegmentDetail.DIMENSIONS)
    );

    final String json = MAPPER.writeValueAsString(expected);
    RetrieveUsedSegmentsAction actual = MAPPER.readValue(json, RetrieveUsedSegmentsAction.class);
    Assertions.assertEquals(
        EnumSet.of(SegmentDetail.DIMENSIONS, SegmentDetail.LOAD_SPEC),
        actual.getDetails()
    );
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testOldJsonDeserialization() throws Exception
  {
    String jsonStr = "{\"type\": \"segmentListUsed\", \"dataSource\": \"test\", \"intervals\": [\"2014/2015\"]}";
    RetrieveUsedSegmentsAction actual = (RetrieveUsedSegmentsAction) MAPPER.readValue(jsonStr, TaskAction.class);

    Assertions.assertEquals(
        new RetrieveUsedSegmentsAction(
            "test",
            Collections.singletonList(Intervals.of("2014/2015")),
            Segments.ONLY_VISIBLE,
            null
        ),
        actual
    );
    Assertions.assertNull(actual.getDetails());
  }
}
