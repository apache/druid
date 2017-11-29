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

package io.druid.server.coordinator.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.coordinator.CoordinatorCompactionConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class NewestSegmentFirstPolicyTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final long SEGMENT_SIZE = 1000;
  private static final int NUM_SEGMENTS_PER_SHARD = 4;

  private final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy();

  @Test
  public void testLargeOffsetAndSmallSegmentInterval()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P2D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                segmentPeriod,
                Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"),
                Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00")
            )
        )
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-14T00:00:00/2017-11-14T01:00:00"),
        Intervals.of("2017-11-15T03:00:00/2017-11-15T04:00:00"),
        true
    );
  }

  @Test
  public void testSmallOffsetAndLargeSegmentInterval()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("PT1M"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                segmentPeriod,
                Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"),
                Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00")
            )
        )
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T21:00:00/2017-11-16T22:00:00"),
        Intervals.of("2017-11-17T02:00:00/2017-11-17T03:00:00"),
        false
    );

    final List<DataSegment> segments = iterator.next();
    Assert.assertNotNull(segments);
    Assert.assertEquals(8, segments.size());

    final List<Interval> expectedIntervals = new ArrayList<>(segments.size());
    for (int i = 0; i < 4; i++) {
      expectedIntervals.add(Intervals.of("2017-11-16T06:00:00/2017-11-16T07:00:00"));
    }
    for (int i = 0; i < 4; i++) {
      expectedIntervals.add(Intervals.of("2017-11-16T20:00:00/2017-11-16T21:00:00"));
    }
    expectedIntervals.sort(Comparators.intervalsByStartThenEnd());

    Assert.assertEquals(
        expectedIntervals,
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-14T00:00:00/2017-11-14T01:00:00"),
        Intervals.of("2017-11-16T05:00:00/2017-11-16T06:00:00"),
        true
    );
  }

  @Test
  public void testLargeGapInData()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("PT1H1M"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                segmentPeriod,
                Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"),
                // larger gap than SegmentCompactorUtil.LOOKUP_PERIOD (1 day)
                Intervals.of("2017-11-14T00:00:00/2017-11-15T07:00:00")
            )
        )
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T20:00:00/2017-11-16T21:00:00"),
        Intervals.of("2017-11-17T01:00:00/2017-11-17T02:00:00"),
        false
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-14T00:00:00/2017-11-14T01:00:00"),
        Intervals.of("2017-11-15T06:00:00/2017-11-15T07:00:00"),
        true
    );
  }

  private static void assertCompactSegmentIntervals(
      CompactionSegmentIterator iterator,
      Period segmentPeriod,
      Interval from,
      Interval to,
      boolean assertLast
  )
  {
    Interval expectedSegmentIntervalStart = to;
    while (iterator.hasNext()) {
      final List<DataSegment> segments = iterator.next();

      final List<Interval> expectedIntervals = new ArrayList<>(segments.size());
      for (int i = 0; i < segments.size(); i++) {
        if (i > 0 && i % NUM_SEGMENTS_PER_SHARD == 0) {
          expectedSegmentIntervalStart = new Interval(segmentPeriod, expectedSegmentIntervalStart.getStart());
        }
        expectedIntervals.add(expectedSegmentIntervalStart);
      }
      expectedIntervals.sort(Comparators.intervalsByStartThenEnd());

      Assert.assertEquals(
          expectedIntervals,
          segments.stream().map(DataSegment::getInterval).collect(Collectors.toList())
      );

      if (expectedSegmentIntervalStart.equals(from)) {
        break;
      }
      expectedSegmentIntervalStart = new Interval(segmentPeriod, expectedSegmentIntervalStart.getStart());
    }

    if (assertLast) {
      Assert.assertFalse(iterator.hasNext());
    }
  }

  private static VersionedIntervalTimeline<String, DataSegment> createTimeline(
      Period segmentPeriod,
      Interval... intervals
  )
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(
        String.CASE_INSENSITIVE_ORDER
    );

    final String version = DateTimes.nowUtc().toString();

    final List<Interval> orderedIntervals = Lists.newArrayList(intervals);
    orderedIntervals.sort(Comparators.intervalsByStartThenEnd());
    Collections.reverse(orderedIntervals);

    for (Interval interval : orderedIntervals) {
      Interval remaininInterval = interval;

      while (!Intervals.isEmpty(remaininInterval)) {
        final Interval segmentInterval;
        if (remaininInterval.toDuration().isLongerThan(segmentPeriod.toStandardDuration())) {
          segmentInterval = new Interval(segmentPeriod, remaininInterval.getEnd());
        } else {
          segmentInterval = remaininInterval;
        }

        for (int i = 0; i < NUM_SEGMENTS_PER_SHARD; i++) {
          final ShardSpec shardSpec = new NumberedShardSpec(NUM_SEGMENTS_PER_SHARD, i);
          final DataSegment segment = new DataSegment(
              DATA_SOURCE,
              segmentInterval,
              version,
              null,
              ImmutableList.of(),
              ImmutableList.of(),
              shardSpec,
              0,
              SEGMENT_SIZE
          );
          timeline.add(
              segmentInterval,
              version,
              shardSpec.createChunk(segment)
          );
        }

        remaininInterval = SegmentCompactorUtil.removeIntervalFromEnd(remaininInterval, segmentInterval);
      }
    }

    return timeline;
  }

  private static CoordinatorCompactionConfig createCompactionConfig(
      long targetCompactionSizeBytes,
      Period skipOffsetFromLatest
  )
  {
    return new CoordinatorCompactionConfig(
        DATA_SOURCE,
        0,
        targetCompactionSizeBytes,
        skipOffsetFromLatest,
        null,
        null
    );
  }
}
