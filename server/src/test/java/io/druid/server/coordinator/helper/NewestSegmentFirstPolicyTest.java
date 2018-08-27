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

package io.druid.server.coordinator.helper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.coordinator.DataSourceCompactionConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class NewestSegmentFirstPolicyTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final long DEFAULT_SEGMENT_SIZE = 1000;
  private static final int DEFAULT_NUM_SEGMENTS_PER_SHARD = 4;

  private final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy();

  @Test
  public void testLargeOffsetAndSmallSegmentInterval()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, 100, new Period("P2D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00"), segmentPeriod)
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, 100, new Period("PT1M"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00"), segmentPeriod)
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, 100, new Period("PT1H1M"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                // larger gap than SegmentCompactorUtil.LOOKUP_PERIOD (1 day)
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-15T07:00:00"), segmentPeriod)
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

  @Test
  public void testSmallNumTargetCompactionSegments()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, 5, new Period("PT1H1M"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                // larger gap than SegmentCompactorUtil.LOOKUP_PERIOD (1 day)
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-15T07:00:00"), segmentPeriod)
            )
        )
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T20:00:00/2017-11-16T21:00:00"),
        // The last interval is not "2017-11-17T01:00:00/2017-11-17T02:00:00". This is because more segments are
        // expected to be added for that interval. See NewestSegmentFirstIterator.returnIfCompactibleSize().
        Intervals.of("2017-11-17T00:00:00/2017-11-17T01:00:00"),
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

  @Test
  public void testHugeShard()
  {
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, 100, new Period("P1D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(
                    Intervals.of("2017-11-17T00:00:00/2017-11-18T03:00:00"),
                    new Period("PT1H"),
                    200,
                    DEFAULT_NUM_SEGMENTS_PER_SHARD
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-11-09T00:00:00/2017-11-17T00:00:00"),
                    new Period("P2D"),
                    13000, // larger than target compact segment size
                    1
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-11-05T00:00:00/2017-11-09T00:00:00"),
                    new Period("PT1H"),
                    200,
                    DEFAULT_NUM_SEGMENTS_PER_SHARD
                )
            )
        )
    );

    Interval lastInterval = null;
    while (iterator.hasNext()) {
      final List<DataSegment> segments = iterator.next();
      lastInterval = segments.get(0).getInterval();

      Interval prevInterval = null;
      for (DataSegment segment : segments) {
        if (prevInterval != null && !prevInterval.getStart().equals(segment.getInterval().getStart())) {
          Assert.assertEquals(prevInterval.getEnd(), segment.getInterval().getStart());
        }

        prevInterval = segment.getInterval();
      }
    }

    Assert.assertNotNull(lastInterval);
    Assert.assertEquals(Intervals.of("2017-11-05T00:00:00/2017-11-05T01:00:00"), lastInterval);
  }

  @Test
  public void testManySegmentsPerShard()
  {
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(800000, 100, new Period("P1D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-04T01:00:00/2017-12-05T03:00:00"),
                    new Period("PT1H"),
                    375,
                    80
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-04T00:00:00/2017-12-04T01:00:00"),
                    new Period("PT1H"),
                    200,
                    150
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-03T18:00:00/2017-12-04T00:00:00"),
                    new Period("PT6H"),
                    200000,
                    1
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-03T11:00:00/2017-12-03T18:00:00"),
                    new Period("PT1H"),
                    375,
                    80
                )
            )
        )
    );

    Interval lastInterval = null;
    while (iterator.hasNext()) {
      final List<DataSegment> segments = iterator.next();
      lastInterval = segments.get(0).getInterval();

      Interval prevInterval = null;
      for (DataSegment segment : segments) {
        if (prevInterval != null && !prevInterval.getStart().equals(segment.getInterval().getStart())) {
          Assert.assertEquals(prevInterval.getEnd(), segment.getInterval().getStart());
        }

        prevInterval = segment.getInterval();
      }
    }

    Assert.assertNotNull(lastInterval);
    Assert.assertEquals(Intervals.of("2017-12-03T11:00:00/2017-12-03T12:00:00"), lastInterval);
  }

  @Test
  public void testManySegmentsPerShard2()
  {
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(800000, 100, new Period("P1D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-04T11:00:00/2017-12-05T05:00:00"),
                    new Period("PT1H"),
                    200,
                    150
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-04T06:00:00/2017-12-04T11:00:00"),
                    new Period("PT1H"),
                    375,
                    80
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-03T18:00:00/2017-12-04T06:00:00"),
                    new Period("PT12H"),
                    257000,
                    1
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-03T11:00:00/2017-12-03T18:00:00"),
                    new Period("PT1H"),
                    200,
                    150
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-02T19:00:00/2017-12-03T11:00:00"),
                    new Period("PT16H"),
                    257000,
                    1
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-02T11:00:00/2017-12-02T19:00:00"),
                    new Period("PT1H"),
                    200,
                    150
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-01T18:00:00/2017-12-02T11:00:00"),
                    new Period("PT17H"),
                    257000,
                    1
                ),
                new SegmentGenerateSpec(
                    Intervals.of("2017-12-01T09:00:00/2017-12-01T18:00:00"),
                    new Period("PT1H"),
                    200,
                    150
                )
            )
        )
    );

    Assert.assertFalse(iterator.hasNext());
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
        if (i > 0 && i % DEFAULT_NUM_SEGMENTS_PER_SHARD == 0) {
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
      SegmentGenerateSpec... specs
  )
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(
        String.CASE_INSENSITIVE_ORDER
    );

    final String version = DateTimes.nowUtc().toString();

    final List<SegmentGenerateSpec> orderedSpecs = Arrays.asList(specs);
    orderedSpecs.sort((s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.totalInterval, s2.totalInterval));
    Collections.reverse(orderedSpecs);

    for (SegmentGenerateSpec spec : orderedSpecs) {
      Interval remaininInterval = spec.totalInterval;

      while (!Intervals.isEmpty(remaininInterval)) {
        final Interval segmentInterval;
        if (remaininInterval.toDuration().isLongerThan(spec.segmentPeriod.toStandardDuration())) {
          segmentInterval = new Interval(spec.segmentPeriod, remaininInterval.getEnd());
        } else {
          segmentInterval = remaininInterval;
        }

        for (int i = 0; i < spec.numSegmentsPerShard; i++) {
          final ShardSpec shardSpec = new NumberedShardSpec(spec.numSegmentsPerShard, i);
          final DataSegment segment = new DataSegment(
              DATA_SOURCE,
              segmentInterval,
              version,
              null,
              ImmutableList.of(),
              ImmutableList.of(),
              shardSpec,
              0,
              spec.segmentSize
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

  private static DataSourceCompactionConfig createCompactionConfig(
      long targetCompactionSizeBytes,
      int numTargetCompactionSegments,
      Period skipOffsetFromLatest
  )
  {
    return new DataSourceCompactionConfig(
        DATA_SOURCE,
        0,
        targetCompactionSizeBytes,
        numTargetCompactionSegments,
        skipOffsetFromLatest,
        null,
        null
    );
  }

  private static class SegmentGenerateSpec
  {
    private final Interval totalInterval;
    private final Period segmentPeriod;
    private final long segmentSize;
    private final int numSegmentsPerShard;

    SegmentGenerateSpec(Interval totalInterval, Period segmentPeriod)
    {
      this(totalInterval, segmentPeriod, DEFAULT_SEGMENT_SIZE, DEFAULT_NUM_SEGMENTS_PER_SHARD);
    }

    SegmentGenerateSpec(Interval totalInterval, Period segmentPeriod, long segmentSize, int numSegmentsPerShard)
    {
      Preconditions.checkArgument(numSegmentsPerShard >= 1);
      this.totalInterval = totalInterval;
      this.segmentPeriod = segmentPeriod;
      this.segmentSize = segmentSize;
      this.numSegmentsPerShard = numSegmentsPerShard;
    }
  }
}
