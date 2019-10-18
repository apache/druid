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

package org.apache.druid.server.coordinator.helper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class NewestSegmentFirstPolicyTest
{
  private static final String DATA_SOURCE = "dataSource";
  private static final long DEFAULT_SEGMENT_SIZE = 1000;
  private static final int DEFAULT_NUM_SEGMENTS_PER_SHARD = 4;

  private final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy(new DefaultObjectMapper());

  @Test
  public void testLargeOffsetAndSmallSegmentInterval()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P2D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00"), segmentPeriod)
            )
        ),
        Collections.emptyMap()
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
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00"), segmentPeriod)
            )
        ),
        Collections.emptyMap()
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T20:00:00/2017-11-16T21:00:00"),
        Intervals.of("2017-11-17T02:00:00/2017-11-17T03:00:00"),
        false
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-14T00:00:00/2017-11-14T01:00:00"),
        Intervals.of("2017-11-16T06:00:00/2017-11-16T07:00:00"),
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
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                // larger gap than SegmentCompactorUtil.LOOKUP_PERIOD (1 day)
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-15T07:00:00"), segmentPeriod)
            )
        ),
        Collections.emptyMap()
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
  public void testHugeShard()
  {
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P1D"))),
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
        ),
        Collections.emptyMap()
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(800000, new Period("P1D"))),
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
        ),
        Collections.emptyMap()
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
  public void testSkipUnknownDataSource()
  {
    final String unknownDataSource = "unknown";
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(
            unknownDataSource,
            createCompactionConfig(10000, new Period("P2D")),
            DATA_SOURCE,
            createCompactionConfig(10000, new Period("P2D"))
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00"), segmentPeriod)
            )
        ),
        Collections.emptyMap()
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
  public void testClearSegmentsToCompactWhenSkippingSegments()
  {
    final long inputSegmentSizeBytes = 800000;
    final VersionedIntervalTimeline<String, DataSegment> timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-12-03T00:00:00/2017-12-04T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2 + 10,
            1
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-12-02T00:00:00/2017-12-03T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes + 10, // large segment
            1
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-12-01T00:00:00/2017-12-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 3 + 10,
            2
        )
    );
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(inputSegmentSizeBytes, new Period("P0D"))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>();
    expectedSegmentsToCompact.addAll(
        timeline
            .lookup(Intervals.of("2017-12-03/2017-12-04"))
            .stream()
            .flatMap(holder -> StreamSupport.stream(holder.getObject().spliterator(), false))
            .map(PartitionChunk::getObject)
            .collect(Collectors.toList())
    );
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedSegmentsToCompact, iterator.next());

    expectedSegmentsToCompact.clear();
    expectedSegmentsToCompact.addAll(
        timeline
            .lookup(Intervals.of("2017-12-01/2017-12-02"))
            .stream()
            .flatMap(holder -> StreamSupport.stream(holder.getObject().spliterator(), false))
            .map(PartitionChunk::getObject)
            .collect(Collectors.toList())
    );
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(expectedSegmentsToCompact, iterator.next());

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfFirstSegmentIsInSkipOffset()
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-12-02T14:00:00/2017-12-03T00:00:00"),
            new Period("PT5H"),
            40000,
            1
        )
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfFirstSegmentOverlapsSkipOffset()
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-12-01T23:00:00/2017-12-03T00:00:00"),
            new Period("PT5H"),
            40000,
            1
        )
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testWithSkipIntervals()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P1D"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                new SegmentGenerateSpec(Intervals.of("2017-11-14T00:00:00/2017-11-16T07:00:00"), segmentPeriod)
            )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            ImmutableList.of(
                Intervals.of("2017-11-16T00:00:00/2017-11-17T00:00:00"),
                Intervals.of("2017-11-15T00:00:00/2017-11-15T20:00:00"),
                Intervals.of("2017-11-13T00:00:00/2017-11-14T01:00:00")
            )
        )
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-15T20:00:00/2017-11-15T21:00:00"),
        Intervals.of("2017-11-15T23:00:00/2017-11-16T00:00:00"),
        false
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-14T01:00:00/2017-11-14T02:00:00"),
        Intervals.of("2017-11-14T23:00:00/2017-11-15T00:00:00"),
        true
    );
  }

  @Test
  public void testHoleInSearchInterval()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("PT1H"))),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T00:00:00/2017-11-17T00:00:00"), segmentPeriod)
            )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            ImmutableList.of(
                Intervals.of("2017-11-16T04:00:00/2017-11-16T10:00:00"),
                Intervals.of("2017-11-16T14:00:00/2017-11-16T20:00:00")
            )
        )
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T20:00:00/2017-11-16T21:00:00"),
        Intervals.of("2017-11-16T22:00:00/2017-11-16T23:00:00"),
        false
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T10:00:00/2017-11-16T11:00:00"),
        Intervals.of("2017-11-16T13:00:00/2017-11-16T14:00:00"),
        false
    );

    assertCompactSegmentIntervals(
        iterator,
        segmentPeriod,
        Intervals.of("2017-11-16T00:00:00/2017-11-16T01:00:00"),
        Intervals.of("2017-11-16T03:00:00/2017-11-16T04:00:00"),
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

      final Interval firstInterval = segments.get(0).getInterval();
      Assert.assertTrue(
          "Intervals should be same or abutting",
          segments.stream().allMatch(
              segment -> segment.getInterval().isEqual(firstInterval) || segment.getInterval().abuts(firstInterval)
          )
      );

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
    List<DataSegment> segments = new ArrayList<>();
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
          final ShardSpec shardSpec = new NumberedShardSpec(i, spec.numSegmentsPerShard);
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
          segments.add(segment);
        }

        remaininInterval = SegmentCompactorUtil.removeIntervalFromEnd(remaininInterval, segmentInterval);
      }
    }

    return VersionedIntervalTimeline.forSegments(segments);
  }

  private DataSourceCompactionConfig createCompactionConfig(
      long inputSegmentSizeBytes,
      Period skipOffsetFromLatest
  )
  {
    return new DataSourceCompactionConfig(
        DATA_SOURCE,
        0,
        inputSegmentSizeBytes,
        null,
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
