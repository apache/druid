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

package org.apache.druid.java.util.emitter.service;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class SegmentMetadataEventTest
{
  @Test
  public void testBasicEvent()
  {
    SegmentMetadataEvent event = new SegmentMetadataEvent(
        "dummy_datasource",
        DateTimes.of("2001-01-01T00:00:00.000Z"),
        DateTimes.of("2001-01-02T00:00:00.000Z"),
        DateTimes.of("2001-01-03T00:00:00.000Z"),
        "dummy_version",
        true
    );

    Assert.assertEquals(
        ImmutableMap.<String, Object>builder()
            .put(SegmentMetadataEvent.FEED, "segment_metadata")
            .put(SegmentMetadataEvent.DATASOURCE, "dummy_datasource")
            .put(SegmentMetadataEvent.CREATED_TIME, DateTimes.of("2001-01-01T00:00:00.000Z"))
            .put(SegmentMetadataEvent.START_TIME, DateTimes.of("2001-01-02T00:00:00.000Z"))
            .put(SegmentMetadataEvent.END_TIME, DateTimes.of("2001-01-03T00:00:00.000Z"))
            .put(SegmentMetadataEvent.VERSION, "dummy_version")
            .put(SegmentMetadataEvent.IS_COMPACTED, true)
            .build(),
        event.toMap()
    );
  }

  @Test
  public void testCreate()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("wiki")
                                           .interval(Intervals.of("2023/2024"))
                                           .shardSpec(new NumberedShardSpec(1, 1))
                                           .version("v1")
                                           .size(100)
                                           .build();
    final DateTime eventTime = DateTimes.nowUtc();
    SegmentMetadataEvent event = SegmentMetadataEvent.create(segment, eventTime);
    Assert.assertEquals(
        EventMap.builder()
                .put(SegmentMetadataEvent.FEED, "segment_metadata")
                .put(SegmentMetadataEvent.DATASOURCE, segment.getDataSource())
                .put(SegmentMetadataEvent.CREATED_TIME, eventTime)
                .put(SegmentMetadataEvent.START_TIME, segment.getInterval().getStart())
                .put(SegmentMetadataEvent.END_TIME, segment.getInterval().getEnd())
                .put(SegmentMetadataEvent.VERSION, segment.getVersion())
                .put(SegmentMetadataEvent.IS_COMPACTED, false)
                .build(),
        event.toMap()
    );
  }
}
