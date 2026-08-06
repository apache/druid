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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.metadata.DefaultIndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.segment.metadata.NoopIndexingStateCache;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

public class DataSourceCompactibleSegmentIteratorTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final CompactionCandidateSearchPolicy POLICY = new NewestSegmentFirstPolicy(null);
  private static final IndexingStateFingerprintMapper FINGERPRINT_MAPPER =
      new DefaultIndexingStateFingerprintMapper(new NoopIndexingStateCache(), MAPPER);

  @Test
  public void testFilterSkipIntervals()
  {
    final Interval totalInterval = Intervals.of("2018-01-01/2019-01-01");
    final List<Interval> expectedSkipIntervals = ImmutableList.of(
        Intervals.of("2018-01-15/2018-03-02"),
        Intervals.of("2018-07-23/2018-10-01"),
        Intervals.of("2018-10-02/2018-12-25"),
        Intervals.of("2018-12-31/2019-01-01")
    );
    final List<Interval> skipIntervals = DataSourceCompactibleSegmentIterator.filterSkipIntervals(
        totalInterval,
        Lists.newArrayList(
            Intervals.of("2017-12-01/2018-01-15"),
            Intervals.of("2018-03-02/2018-07-23"),
            Intervals.of("2018-10-01/2018-10-02"),
            Intervals.of("2018-12-25/2018-12-31")
        )
    );

    Assertions.assertEquals(expectedSkipIntervals, skipIntervals);
  }

  @Test
  public void testFindInitialSearchInterval()
  {
    final Iterator<DataSegment> segments = CreateDataSegments.ofDatasource("test_datasource")
                                                             .forIntervals(12, Granularities.HOUR)
                                                             .startingAt("2018-01-01")
                                                             .withNumPartitions(1)
                                                             .eachOfSizeInMb(100)
                                                             .iterator();
    final SegmentTimeline timeline = SegmentTimeline.forSegments(segments);
    final DataSourceCompactionConfig config =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource("test_datasource")
                                              .withSkipOffsetFromLatest(new Period("PT4H"))
                                              .build();
    final DataSourceCompactibleSegmentIterator iterator = new DataSourceCompactibleSegmentIterator(
        config,
        timeline,
        List.of(),
        POLICY,
        FINGERPRINT_MAPPER
    );

    final List<Interval> searchIntervals = iterator.findInitialSearchInterval(timeline, List.of());

    // Expected: Total interval is 2018-01-01T00:00:00/2018-01-01T12:00:00
    // Skip interval: 2018-01-01T08:00:00/2018-01-01T12:00:00 (computed from 4h offset)
    // Search interval should be: [2018-01-01T00:00:00/2018-01-01T08:00:00]
    Assertions.assertEquals(1, searchIntervals.size());
    Assertions.assertEquals(Intervals.of("2018-01-01T00:00:00/2018-01-01T08:00:00"), searchIntervals.get(0));
  }

  @Test
  public void testFindInitialSearchIntervalWithMultipleSkipIntervals()
  {
    final Iterator<DataSegment> segments = CreateDataSegments.ofDatasource("test_datasource")
                                                             .forIntervals(24, Granularities.HOUR)
                                                             .startingAt("2018-01-01")
                                                             .withNumPartitions(1)
                                                             .eachOfSizeInMb(100)
                                                             .iterator();
    final SegmentTimeline timeline = SegmentTimeline.forSegments(segments);
    final DataSourceCompactionConfig config =
        InlineSchemaDataSourceCompactionConfig.builder()
                                              .forDataSource("test_datasource")
                                              .withSkipOffsetFromLatest(new Period("PT4H"))
                                              .withSkipIntervals(List.of(
                                                  Intervals.of("2018-01-01T06:00:00/2018-01-01T08:00:00"),
                                                  Intervals.of("2018-01-01T12:00:00/2018-01-01T14:00:00"),
                                                  Intervals.of("2018-01-01T18:30:00/2018-01-01T21:00:00")
                                              ))
                                              .build();
    final DataSourceCompactibleSegmentIterator iterator = new DataSourceCompactibleSegmentIterator(
        config,
        timeline,
        List.of(),
        POLICY,
        FINGERPRINT_MAPPER
    );

    final List<Interval> searchIntervals = iterator.findInitialSearchInterval(timeline, List.of());

    // The three configured skip intervals and the 4h-offset skip interval (18:30-21:00 and
    // 20:00-00:00 merge) leave three search windows. The last window is clipped to 18:00
    // since segments are hourly and the 18:00-19:00 segment overlaps the 18:30 skip start.
    Assertions.assertEquals(
        List.of(
            Intervals.of("2018-01-01T00:00:00/PT6H"),
            Intervals.of("2018-01-01T08:00:00/PT4H"),
            Intervals.of("2018-01-01T14:00:00/PT4H")
        ),
        searchIntervals
    );
  }
}
