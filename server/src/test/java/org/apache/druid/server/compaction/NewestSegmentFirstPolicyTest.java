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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskTransformConfig;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.apache.druid.utils.Streams;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class NewestSegmentFirstPolicyTest
{
  private static final int DEFAULT_NUM_SEGMENTS_PER_SHARD = 4;
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy(null);
  private CompactionStatusTracker statusTracker;

  @Before
  public void setup()
  {
    statusTracker = new CompactionStatusTracker(mapper);
  }

  @Test
  public void testLargeOffsetAndSmallSegmentInterval()
  {
    final Period segmentPeriod = Period.hours(1);
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(new Period("P2D")).build(),
        createTimeline(
            createSegments().forIntervals(8, Granularities.HOUR)
                            .startingAt("2017-11-16T20:00:00Z")
                            .withNumPartitions(4),
            createSegments().forIntervals(55, Granularities.HOUR)
                            .startingAt("2017-11-14")
                            .withNumPartitions(4)
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
    final Period segmentPeriod = Period.hours(1);
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(new Period("PT1M")).build(),
        createTimeline(
            createSegments().forIntervals(8, Granularities.HOUR)
                            .startingAt("2017-11-16T20:00:00Z")
                            .withNumPartitions(4),
            createSegments().forIntervals(55, Granularities.HOUR)
                            .startingAt("2017-11-14")
                            .withNumPartitions(4)
        )
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
    final Period segmentPeriod = Period.hours(1);
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(Period.minutes(61)).build(),
        createTimeline(
            createSegments().forIntervals(8, Granularities.HOUR)
                            .startingAt("2017-11-16T20:00:00Z")
                            .withNumPartitions(4),
            createSegments().forIntervals(31, Granularities.HOUR)
                            .startingAt("2017-11-14")
                            .withNumPartitions(4)
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
  public void testHugeShard()
  {
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(Period.days(1)).build(),
        createTimeline(
            createSegments()
                .forIntervals(27, Granularities.HOUR)
                .startingAt("2017-11-17")
                .withNumPartitions(4),
            createSegments()
                .forIntervals(4, new PeriodGranularity(Period.days(2), null, null))
                .startingAt("2017-11-09")
                .withNumPartitions(1),
            createSegments()
                .forIntervals(96, Granularities.HOUR)
                .startingAt("2017-11-05")
                .withNumPartitions(4)
        )
    );

    Interval lastInterval = null;
    while (iterator.hasNext()) {
      final List<DataSegment> segments = iterator.next().getSegments();
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
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(Period.days(1)).build(),
        createTimeline(
            createSegments().forIntervals(26, Granularities.HOUR)
                            .startingAt("2017-12-04T01:00:00Z")
                            .withNumPartitions(80),
            createSegments().forIntervals(1, Granularities.HOUR)
                            .startingAt("2017-12-04")
                            .withNumPartitions(150),
            createSegments().forIntervals(1, Granularities.SIX_HOUR)
                            .startingAt("2017-12-03T18:00:00Z")
                            .withNumPartitions(1),
            createSegments().forIntervals(7, Granularities.HOUR)
                            .startingAt("2017-12-03T11:00:00Z")
                            .withNumPartitions(80)
        )
    );

    Interval lastInterval = null;
    while (iterator.hasNext()) {
      final List<DataSegment> segments = iterator.next().getSegments();
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
  public void testSkipDataSourceWithNoSegments()
  {
    final Period segmentPeriod = Period.hours(1);
    final CompactionSegmentIterator iterator = new PriorityBasedCompactionSegmentIterator(
        policy,
        ImmutableMap.of(
            TestDataSource.KOALA,
            configBuilder().forDataSource(TestDataSource.KOALA).build(),
            TestDataSource.WIKI,
            configBuilder().forDataSource(TestDataSource.WIKI).withSkipOffsetFromLatest(Period.days(2)).build()
        ),
        ImmutableMap.of(
            TestDataSource.WIKI,
            createTimeline(
                createSegments().forIntervals(8, Granularities.HOUR)
                                .startingAt("2017-11-16T20:00:00Z")
                                .withNumPartitions(4),
                createSegments().forIntervals(55, Granularities.HOUR)
                                .startingAt("2017-11-14")
                                .withNumPartitions(4)
            )
        ),
        Collections.emptyMap(),
        statusTracker
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
    final long inputSegmentSizeBytes = 800_000;
    final List<DataSegment> segments = new ArrayList<>(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-12-03")
            .withNumPartitions(1)
            .eachOfSize(inputSegmentSizeBytes / 2 + 10)
    );
    segments.addAll(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-12-02")
            .withNumPartitions(1)
            .eachOfSize(inputSegmentSizeBytes + 10) // large segment
    );
    segments.addAll(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-12-01")
            .withNumPartitions(2)
            .eachOfSize(inputSegmentSizeBytes / 3 + 10)
    );
    final SegmentTimeline timeline = SegmentTimeline.forSegments(segments);
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withInputSegmentSizeBytes(inputSegmentSizeBytes).build(),
        timeline
    );

    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-12-03/2017-12-04"), Partitions.ONLY_COMPLETE)
    );
    expectedSegmentsToCompact.sort(Comparator.naturalOrder());

    final List<DataSegment> expectedSegmentsToCompact2 = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-12-01/2017-12-02"), Partitions.ONLY_COMPLETE)
    );
    expectedSegmentsToCompact2.sort(Comparator.naturalOrder());

    Set<List<DataSegment>> observedSegments = Streams.sequentialStreamFrom(iterator)
                                                     .map(CompactionCandidate::getSegments)
                                                     .collect(Collectors.toSet());
    Assert.assertEquals(
        ImmutableSet.of(expectedSegmentsToCompact, expectedSegmentsToCompact2),
        observedSegments
    );
  }

  @Test
  public void testIfFirstSegmentIsInSkipOffset()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(2, new PeriodGranularity(Period.hours(5), null, null))
            .startingAt("2017-12-02T14:00:00Z")
            .withNumPartitions(1)
    );

    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(Period.days(1)).build(),
        timeline
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfFirstSegmentOverlapsSkipOffset()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(5, new PeriodGranularity(Period.hours(5), null, null))
            .startingAt("2017-12-01T23:00:00Z")
            .withNumPartitions(1)
    );

    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withSkipOffsetFromLatest(Period.days(1)).build(),
        timeline
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfSegmentsSkipOffsetWithConfiguredSegmentGranularityEqual()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments().forIntervals(2, Granularities.DAY)
                        .startingAt("2017-12-01")
                        .withNumPartitions(4),
        createSegments().forIntervals(1, Granularities.DAY)
                        .startingAt("2017-10-14")
                        .withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        configBuilder()
            .withSkipOffsetFromLatest(Period.days(1))
            .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))
            .build(),
        timeline
    );

    // We should only get segments in Oct
    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-14/2017-12-02"), Partitions.ONLY_COMPLETE)
    );

    Assert.assertTrue(iterator.hasNext());
    Set<DataSegment> observedSegmentsToCompact = Streams.sequentialStreamFrom(iterator)
                                                        .flatMap(s -> s.getSegments().stream())
                                                        .collect(Collectors.toSet());
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        observedSegmentsToCompact
    );
  }

  @Test
  public void testIfSegmentsSkipOffsetWithConfiguredSegmentGranularityLarger()
  {
    final SegmentTimeline timeline = createTimeline(
        // This contains segment that
        // - Cross between month boundary of latest month (starts in Nov and ends in Dec). This should be skipped
        // - Fully in latest month (starts in Dec and ends in Dec). This should be skipped
        // - Does not overlap latest month (starts in Oct and ends in Oct). This should not be skipped
        createSegments().forIntervals(5, new PeriodGranularity(Period.hours(5), null, null))
                        .startingAt("2017-11-30T23:00:00Z")
                        .withNumPartitions(4),
        createSegments().forIntervals(4, new PeriodGranularity(Period.hours(5), null, null))
                        .startingAt("2017-10-14T04:00:00Z")
                        .withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        configBuilder()
            .withSkipOffsetFromLatest(Period.days(1))
            .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null))
            .build(),
        timeline
    );

    // We should only get segments in Oct
    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-14/P1D"), Partitions.ONLY_COMPLETE)
    );

    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> actual = iterator.next().getSegments();
    Assert.assertEquals(expectedSegmentsToCompact.size(), actual.size());
    Assert.assertEquals(ImmutableSet.copyOf(expectedSegmentsToCompact), ImmutableSet.copyOf(actual));
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfSegmentsSkipOffsetWithConfiguredSegmentGranularitySmaller()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments().forIntervals(5, new PeriodGranularity(Period.hours(5), null, null))
                        .startingAt("2017-12-01T23:00:00Z")
                        .withNumPartitions(4),
        createSegments().forIntervals(4, new PeriodGranularity(Period.hours(5), null, null))
                        .startingAt("2017-10-14")
                        .withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        configBuilder()
            .withSkipOffsetFromLatest(Period.days(1))
            .withGranularitySpec(new UserCompactionTaskGranularityConfig(Granularities.MINUTE, null, null))
            .build(),
        timeline
    );

    // We should only get segments in Oct
    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-14T00:00:00/2017-10-15T00:00:00"), Partitions.ONLY_COMPLETE)
    );

    Assert.assertTrue(iterator.hasNext());
    Set<DataSegment> observedSegmentsToCompact = Streams.sequentialStreamFrom(iterator)
                                                        .flatMap(s -> s.getSegments().stream())
                                                        .collect(Collectors.toSet());
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        observedSegmentsToCompact
    );
  }

  @Test
  public void testWithSkipIntervals()
  {
    final Period segmentPeriod = Period.hours(1);
    final CompactionSegmentIterator iterator = new PriorityBasedCompactionSegmentIterator(
        policy,
        ImmutableMap.of(TestDataSource.WIKI, configBuilder().withSkipOffsetFromLatest(Period.days(1)).build()),
        ImmutableMap.of(
            TestDataSource.WIKI,
            createTimeline(
                createSegments().forIntervals(8, Granularities.HOUR)
                                .startingAt("2017-11-16T20:00:00Z")
                                .withNumPartitions(4),
                createSegments().forIntervals(55, Granularities.HOUR)
                                .startingAt("2017-11-14")
                                .withNumPartitions(4)
            )
        ),
        ImmutableMap.of(
            TestDataSource.WIKI,
            ImmutableList.of(
                Intervals.of("2017-11-16T00:00:00/2017-11-17T00:00:00"),
                Intervals.of("2017-11-15T00:00:00/2017-11-15T20:00:00"),
                Intervals.of("2017-11-13T00:00:00/2017-11-14T01:00:00")
            )
        ),
        statusTracker
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
    final Period segmentPeriod = Period.hours(1);
    final CompactionSegmentIterator iterator = new PriorityBasedCompactionSegmentIterator(
        policy,
        ImmutableMap.of(TestDataSource.WIKI, configBuilder().withSkipOffsetFromLatest(Period.hours(1)).build()),
        ImmutableMap.of(
            TestDataSource.WIKI,
            createTimeline(
                createSegments().forIntervals(1, Granularities.HOUR).startingAt("2017-11-16").withNumPartitions(4)
            )
        ),
        ImmutableMap.of(
            TestDataSource.WIKI,
            ImmutableList.of(
                Intervals.of("2017-11-16T04:00:00/2017-11-16T10:00:00"),
                Intervals.of("2017-11-16T14:00:00/2017-11-16T20:00:00")
            )
        ),
        statusTracker
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

  @Test
  public void testIteratorReturnsSegmentsInConfiguredSegmentGranularity()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(3, Granularities.MONTH)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.MONTH),
        timeline
    );

    // We should get all segments in timeline back since skip offset is P0D.
    // However, we only need to iterator 3 times (once for each month) since the new configured segmentGranularity is MONTH.
    // and hence iterator would return all segments bucketed to the configured segmentGranularity
    // Month of Dec
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-12-01T00:00:00/2017-12-31T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // Month of Nov
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-11-01T00:00:00/2017-12-01T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // Month of Oct
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-11-01T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsInMultipleIntervalIfConfiguredSegmentGranularityCrossBoundary()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments().forIntervals(1, Granularities.WEEK).startingAt("2020-01-01").withNumPartitions(4),
        createSegments().forIntervals(1, Granularities.WEEK).startingAt("2020-01-28").withNumPartitions(4),
        createSegments().forIntervals(1, Granularities.WEEK).startingAt("2020-02-08").withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.MONTH),
        timeline
    );
    // We should get the segment of "2020-01-28/2020-02-03" back twice when the iterator returns for Jan and when the
    // iterator returns for Feb.

    // Month of Feb
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2020-01-28/2020-02-15"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> actual = iterator.next().getSegments();
    Assert.assertEquals(expectedSegmentsToCompact.size(), actual.size());
    Assert.assertEquals(ImmutableSet.copyOf(expectedSegmentsToCompact), ImmutableSet.copyOf(actual));
    // Month of Jan
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2020-01-01/2020-02-03"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertTrue(iterator.hasNext());
    actual = iterator.next().getSegments();
    Assert.assertEquals(expectedSegmentsToCompact.size(), actual.size());
    Assert.assertEquals(ImmutableSet.copyOf(expectedSegmentsToCompact), ImmutableSet.copyOf(actual));
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorDoesNotReturnCompactedInterval()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments().forIntervals(1, Granularities.DAY).startingAt("2017-12-01").withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.MINUTE),
        timeline
    );

    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-12-01T00:00:00/2017-12-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // Iterator should return only once since all the "minute" interval of the iterator contains the same interval
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsAllMixedVersionSegmentsInConfiguredSegmentGranularity()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments().forIntervals(1, Granularities.DAY).startingAt("2017-10-01")
                        .withVersion("v1").withNumPartitions(4),
        createSegments().forIntervals(1, Granularities.HOUR).startingAt("2017-10-01")
                        .withVersion("v2").withNumPartitions(4)
    );

    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.MONTH),
        timeline
    );

    // We should get all segments in timeline back since skip offset is P0D.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsNothingAsSegmentsWasCompactedAndHaveSameSegmentGranularityAndSameTimezone()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .startingAt("2017-10-01")
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
            .withNumPartitions(4),
        createSegments()
            .startingAt("2017-10-02")
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
            .withNumPartitions(4)
    );

    // Auto compaction config sets segmentGranularity=DAY
    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.DAY),
        timeline
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsNothingAsSegmentsWasCompactedAndHaveSameSegmentGranularityInLastCompactionState()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final CompactionState compactionState
        = new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("segmentGranularity", "day"));
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(2, Granularities.DAY)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(compactionState)
    );

    // Auto compaction config sets segmentGranularity=DAY
    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.DAY),
        timeline
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentSegmentGranularity()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final CompactionState compactionState = new CompactionState(partitionsSpec, null, null, null, indexSpec, null);
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(2, Granularities.DAY)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(compactionState)
    );

    // Auto compaction config sets segmentGranularity=YEAR
    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.YEAR),
        timeline
    );
    // We should get all segments in timeline back since skip offset is P0D.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-03T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentSegmentGranularityInLastCompactionState()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final CompactionState compactionState
        = new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("segmentGranularity", "day"));
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(2, Granularities.DAY)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(compactionState)
    );

    // Auto compaction config sets segmentGranularity=YEAR
    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.YEAR),
        timeline
    );
    // We should get all segments in timeline back since skip offset is P0D.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-03T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentTimezone()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
    );

    // Duration of new segmentGranularity is the same as before (P1D),
    // but we changed the timezone from UTC to Bangkok in the auto compaction spec
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(
                new PeriodGranularity(
                    Period.days(1),
                    null,
                    DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Bangkok"))
                ),
                null,
                null
            )
        ).build(),
        timeline
    );
    // We should get all segments in timeline back since skip offset is P0D.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-03T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentOrigin()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
    );

    // Duration of new segmentGranularity is the same as before (P1D), but we changed the origin in the autocompaction spec
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(
                new PeriodGranularity(
                    Period.days(1),
                    DateTimes.of("2012-01-02T00:05:00.000Z"),
                    DateTimeZone.UTC
                ),
                null,
                null
            )
        ).build(),
        timeline
    );
    // We should get all segments in timeline back since skip offset is P0D.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-03T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentRollup()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have
    // rollup=false for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // rollup=true for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // and rollup=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (queryGranularity was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("rollup", "false"))),
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("rollup", "true"))),
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-03")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of()))
    );

    // Auto compaction config sets rollup=true
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(null, null, true)
        ).build(),
        timeline
    );
    // We should get interval 2017-10-01T00:00:00/2017-10-02T00:00:00 and interval 2017-10-03T00:00:00/2017-10-04T00:00:00.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentQueryGranularity()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have
    // queryGranularity=DAY for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // queryGranularity=MINUTE for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // and queryGranularity=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (queryGranularity was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("queryGranularity", "day"))),
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("queryGranularity", "minute"))),
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-03")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of()))
    );

    // Auto compaction config sets queryGranularity=MINUTE
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(null, Granularities.MINUTE, null)
        ).build(),
        timeline
    );
    // We should get interval 2017-10-01T00:00:00/2017-10-02T00:00:00 and interval 2017-10-03T00:00:00/2017-10-04T00:00:00.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentDimensions()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have
    // Dimensions=["foo", "bar"] for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // Dimensions=["foo"] for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // Dimensions=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (dimensions was not set during last compaction)
    // and dimensionsSpec=null for interval 2017-10-04T00:00:00/2017-10-05T00:00:00 (dimensionsSpec was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(partitionsSpec, new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))), null, null, indexSpec, null)
            ),
        createSegments()
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(partitionsSpec, new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))), null, null, indexSpec, null)
            ),
        createSegments()
            .startingAt("2017-10-03")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, DimensionsSpec.EMPTY, null, null, indexSpec, null)),
        createSegments()
            .startingAt("2017-10-04")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
    );

    // Auto compaction config sets Dimensions=["foo"]
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withDimensionsSpec(
            new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo")))
        ).build(),
        timeline
    );
    // We should get interval 2017-10-01T00:00:00/2017-10-02T00:00:00, interval 2017-10-04T00:00:00/2017-10-05T00:00:00, and interval 2017-10-03T00:00:00/2017-10-04T00:00:00.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-04T00:00:00/2017-10-05T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());

    // Auto compaction config sets Dimensions=null
    iterator = createIterator(
        configBuilder().withDimensionsSpec(
            new UserCompactionTaskDimensionsConfig(null)
        ).build(),
        timeline
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorDoesNotReturnsSegmentsWhenPartitionDimensionsPrefixed()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Set range partitions spec with dimensions ["dim2", "dim4"] -- the same as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = new DimensionRangePartitionsSpec(
        null,
        Integer.MAX_VALUE,
        ImmutableList.of("dim2", "dim4"),
        false
    );

    // Create segments that were compacted (CompactionState != null) and have
    // Dimensions=["dim2", "dim4", "dim3", "dim1"] with ["dim2", "dim4"] as partition dimensions for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // Dimensions=["dim2", "dim4", "dim1", "dim3"] with ["dim2", "dim4"] as partition dimensions for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(partitionsSpec, new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim2", "dim4", "dim3", "dim1"))), null, null, indexSpec, null)
            ),
        createSegments()
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(partitionsSpec, new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim2", "dim4", "dim1", "dim3"))), null, null, indexSpec, null)
            )
    );

    // Auto compaction config sets Dimensions=["dim1", "dim2", "dim3", "dim4"] and partition dimensions as ["dim2", "dim4"]
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withDimensionsSpec(
                           new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3", "dim4")))
                       )
                       .withTuningConfig(
                           new UserCompactionTaskQueryTuningConfig(
                               null,
                               null,
                               null,
                               1000L,
                               null,
                               partitionsSpec,
                               IndexSpec.DEFAULT,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null,
                               null
                           )
                       )
                       .build(),
        timeline
    );
    // We should get only interval 2017-10-01T00:00:00/2017-10-02T00:00:00 since 2017-10-02T00:00:00/2017-10-03T00:00:00
    // has dimension order as expected post reordering of partition dimensions.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentFilter() throws Exception
  {
    NullHandling.initializeForTests();
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have
    // filter=SelectorDimFilter("dim1", "foo", null) for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // filter=SelectorDimFilter("dim1", "bar", null) for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // filter=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (filter was not set during last compaction)
    // and transformSpec=null for interval 2017-10-04T00:00:00/2017-10-05T00:00:00 (transformSpec was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(
                    partitionsSpec,
                    null,
                    null,
                    mapper.readValue(mapper.writeValueAsString(new TransformSpec(new SelectorDimFilter("dim1", "foo", null), null)), new TypeReference<>() {}),
                    indexSpec,
                    null
                )
            ),
        createSegments()
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(
                    partitionsSpec,
                    null,
                    null,
                    mapper.readValue(mapper.writeValueAsString(new TransformSpec(new SelectorDimFilter("dim1", "bar", null), null)), new TypeReference<>() {}),
                    indexSpec,
                    null
                )
            ),
        createSegments()
            .startingAt("2017-10-03")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(
                    partitionsSpec,
                    null,
                    null,
                    mapper.readValue(mapper.writeValueAsString(new TransformSpec(null, null)), new TypeReference<>() {}),
                    indexSpec,
                    null
                )
            ),
        createSegments()
            .startingAt("2017-10-04")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
    );

    // Auto compaction config sets filter=SelectorDimFilter("dim1", "bar", null)
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withTransformSpec(
            new UserCompactionTaskTransformConfig(new SelectorDimFilter("dim1", "bar", null))
        ).build(),
        timeline
    );
    // We should get interval 2017-10-01T00:00:00/2017-10-02T00:00:00, interval 2017-10-04T00:00:00/2017-10-05T00:00:00, and interval 2017-10-03T00:00:00/2017-10-04T00:00:00.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-04T00:00:00/2017-10-05T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());

    // Auto compaction config sets filter=null
    iterator = createIterator(
        configBuilder().withTransformSpec(
            new UserCompactionTaskTransformConfig(null)
        ).build(),
        timeline
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentMetricsSpec()
  {
    NullHandling.initializeForTests();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
    );
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have
    // metricsSpec={CountAggregatorFactory("cnt")} for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // metricsSpec={CountAggregatorFactory("cnt"), LongSumAggregatorFactory("val", "val")} for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // metricsSpec=[] for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (filter was not set during last compaction)
    // and metricsSpec=null for interval 2017-10-04T00:00:00/2017-10-05T00:00:00 (transformSpec was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(
                    partitionsSpec,
                    null,
                    mapper.convertValue(new AggregatorFactory[] {new CountAggregatorFactory("cnt")}, new TypeReference<>() {}),
                    null,
                    indexSpec,
                    null
                )
            ),
        createSegments()
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(
                    partitionsSpec,
                    null,
                    mapper.convertValue(new AggregatorFactory[] {new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val")}, new TypeReference<>() {}),
                    null,
                    indexSpec,
                    null
                )
            ),
        createSegments()
            .startingAt("2017-10-03")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(
                    partitionsSpec,
                    null,
                    mapper.convertValue(new AggregatorFactory[] {}, new TypeReference<>() {}),
                    null,
                    indexSpec,
                    null
                )
            ),
        createSegments()
            .startingAt("2017-10-04")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, indexSpec, null))
    );

    // Auto compaction config sets metricsSpec={CountAggregatorFactory("cnt"), LongSumAggregatorFactory("val", "val")}
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withMetricsSpec(
            new AggregatorFactory[]{new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val")}
        ).build(),
        timeline
    );
    // We should get interval 2017-10-01T00:00:00/2017-10-02T00:00:00, interval 2017-10-04T00:00:00/2017-10-05T00:00:00, and interval 2017-10-03T00:00:00/2017-10-04T00:00:00.
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-04T00:00:00/2017-10-05T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    Assert.assertTrue(iterator.hasNext());
    expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());

    // Auto compaction config sets metricsSpec=null
    iterator = createIterator(
        configBuilder().build(),
        timeline
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsSmallerSegmentGranularityCoveringMultipleSegmentsInTimeline()
  {
    final SegmentTimeline timeline = createTimeline(
        createSegments().forIntervals(1, Granularities.DAY).startingAt("2017-10-01")
                        .withNumPartitions(4).withVersion("v1"),
        createSegments().forIntervals(1, Granularities.HOUR).startingAt("2017-10-01")
                        .withNumPartitions(4).withVersion("v2")
    );

    final CompactionSegmentIterator iterator = createIterator(
        createConfigWithSegmentGranularity(Granularities.HOUR),
        timeline
    );

    // We should get all segments in timeline back since skip offset is P0D.
    // Although the first iteration only covers the last hour of 2017-10-01 (2017-10-01T23:00:00/2017-10-02T00:00:00),
    // the iterator will returns all segment as the umbrella interval the DAY segment (2017-10-01T00:00:00/2017-10-02T00:00:00)
    // also convers the HOUR segment (2017-10-01T01:00:00/2017-10-01T02:00:00)
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsCompactionStateChangedWithCompactedStateHasSameSegmentGranularity()
  {
    // Different indexSpec as what is set in the auto compaction config
    IndexSpec newIndexSpec = IndexSpec.builder().withBitmapSerdeFactory(new ConciseBitmapSerdeFactory()).build();
    Map<String, Object> newIndexSpecMap = mapper.convertValue(newIndexSpec, new TypeReference<>() {});
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-02")
            .withNumPartitions(4)
            .withCompactionState(new CompactionState(partitionsSpec, null, null, null, newIndexSpecMap, null))
    );

    // Duration of new segmentGranularity is the same as before (P1D)
    final CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(
                new PeriodGranularity(
                    Period.days(1),
                    null,
                    DateTimeZone.UTC
                ),
                null,
                null
            )
        ).build(),
        timeline
    );
    // We should get all segments in timeline back since indexSpec changed
    Assert.assertTrue(iterator.hasNext());
    List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-01T00:00:00/2017-10-03T00:00:00"), Partitions.ONLY_COMPLETE)
    );
    Assert.assertEquals(
        ImmutableSet.copyOf(expectedSegmentsToCompact),
        ImmutableSet.copyOf(iterator.next().getSegments())
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorDoesNotReturnSegmentWithChangingAppendableIndexSpec()
  {
    NullHandling.initializeForTests();
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null));
    final SegmentTimeline timeline = createTimeline(
        createSegments()
            .forIntervals(1, Granularities.DAY)
            .startingAt("2017-10-01")
            .withNumPartitions(4)
            .withCompactionState(
                new CompactionState(partitionsSpec, null, null, null, IndexSpec.DEFAULT.asMap(mapper), null)
            )
    );

    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withTuningConfig(
            new UserCompactionTaskQueryTuningConfig(
                null,
                new OnheapIncrementalIndex.Spec(true),
                null,
                1000L,
                null,
                partitionsSpec,
                IndexSpec.DEFAULT,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ).build(),
        timeline
    );
    Assert.assertFalse(iterator.hasNext());

    iterator = createIterator(
        configBuilder().withTuningConfig(
            new UserCompactionTaskQueryTuningConfig(
                null,
                new OnheapIncrementalIndex.Spec(false),
                null,
                1000L,
                null,
                partitionsSpec,
                IndexSpec.DEFAULT,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        ).build(),
        timeline
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipAllGranularityToDefault()
  {
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().build(),
        SegmentTimeline.forSegments(ImmutableSet.of(
                                        new DataSegment(
                                            TestDataSource.WIKI,
                                            Intervals.ETERNITY,
                                            "0",
                                            new HashMap<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new NumberedShardSpec(0, 0),
                                            0,
                                            100
                                        )
                                    )
        )
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipFirstHalfEternityToDefault()
  {
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().build(),
        SegmentTimeline.forSegments(ImmutableSet.of(
                                        new DataSegment(
                                            TestDataSource.WIKI,
                                            new Interval(DateTimes.MIN, DateTimes.of("2024-01-01")),
                                            "0",
                                            new HashMap<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new NumberedShardSpec(0, 0),
                                            0,
                                            100
                                        )
                                    )
        )
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipSecondHalfOfEternityToDefault()
  {
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().build(),
        SegmentTimeline.forSegments(ImmutableSet.of(
                                        new DataSegment(
                                            TestDataSource.WIKI,
                                            new Interval(DateTimes.of("2024-01-01"), DateTimes.MAX),
                                            "0",
                                            new HashMap<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new NumberedShardSpec(0, 0),
                                            0,
                                            100
                                        )
                                    )
        )
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipAllToAllGranularity()
  {
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.ALL, null, null)
        ).build(),
        SegmentTimeline.forSegments(ImmutableSet.of(
                                        new DataSegment(
                                            TestDataSource.WIKI,
                                            Intervals.ETERNITY,
                                            "0",
                                            new HashMap<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new NumberedShardSpec(0, 0),
                                            0,
                                            100
                                        )
                                    )
        )
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipAllToFinerGranularity()
  {
    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
        ).build(),
        SegmentTimeline.forSegments(ImmutableSet.of(
                                        new DataSegment(
                                            TestDataSource.WIKI,
                                            Intervals.ETERNITY,
                                            "0",
                                            new HashMap<>(),
                                            new ArrayList<>(),
                                            new ArrayList<>(),
                                            new NumberedShardSpec(0, 0),
                                            0,
                                            100
                                        )
                                    )
        )
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipCompactionForIntervalsContainingSingleTombstone()
  {
    final DataSegment tombstone2023 = new DataSegment(
        TestDataSource.WIKI,
        Intervals.of("2023/2024"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    final DataSegment dataSegment2023 = new DataSegment(
        TestDataSource.WIKI,
        Intervals.of("2023/2024"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new NumberedShardSpec(1, 0),
        0,
        100);
    final DataSegment tombstone2024 = new DataSegment(
        TestDataSource.WIKI,
        Intervals.of("2024/2025"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);

    CompactionSegmentIterator iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
        ).build(),
        SegmentTimeline.forSegments(ImmutableSet.of(tombstone2023, dataSegment2023, tombstone2024))
    );

    // Skips 2024/2025 since it has a single tombstone and no data.
    // Return all segments in 2023/2024 since at least one of them has data despite there being a tombstone.
    Assert.assertEquals(
        ImmutableList.of(tombstone2023, dataSegment2023),
        iterator.next().getSegments()
    );

    final DataSegment tombstone2025Jan = new DataSegment(
        TestDataSource.WIKI,
        Intervals.of("2025-01-01/2025-02-01"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    final DataSegment tombstone2025Feb = new DataSegment(
        TestDataSource.WIKI,
        Intervals.of("2025-02-01/2025-03-01"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    final DataSegment tombstone2025Mar = new DataSegment(
        TestDataSource.WIKI,
        Intervals.of("2025-03-01/2025-04-01"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    iterator = createIterator(
        configBuilder().withGranularitySpec(
            new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
        ).build(),
        SegmentTimeline.forSegments(ImmutableSet.of(
            tombstone2023,
            dataSegment2023,
            tombstone2024,
            tombstone2025Jan,
            tombstone2025Feb,
            tombstone2025Mar
        ))
    );
    // Does not skip the tombstones in 2025 since there are multiple of them which could potentially be condensed to one
    Assert.assertEquals(
        ImmutableList.of(tombstone2025Jan, tombstone2025Feb, tombstone2025Mar),
        iterator.next().getSegments()
    );
  }

  @Test
  public void testPriorityDatasource()
  {
    final List<DataSegment> wikiSegments
        = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                            .forIntervals(1, Granularities.DAY)
                            .startingAt("2012-01-01")
                            .withNumPartitions(10)
                            .eachOfSizeInMb(100);
    final List<DataSegment> koalaSegments
        = CreateDataSegments.ofDatasource(TestDataSource.KOALA)
                            .forIntervals(1, Granularities.DAY)
                            .startingAt("2013-01-01")
                            .withNumPartitions(10)
                            .eachOfSizeInMb(100);

    // Setup policy and iterator with priorityDatasource = WIKI
    final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy(TestDataSource.WIKI);
    CompactionSegmentIterator iterator = new PriorityBasedCompactionSegmentIterator(
        policy,
        ImmutableMap.of(
            TestDataSource.WIKI, configBuilder().forDataSource(TestDataSource.WIKI).build(),
            TestDataSource.KOALA, configBuilder().forDataSource(TestDataSource.KOALA).build()
        ),
        ImmutableMap.of(
            TestDataSource.WIKI, SegmentTimeline.forSegments(wikiSegments),
            TestDataSource.KOALA, SegmentTimeline.forSegments(koalaSegments)
        ),
        Collections.emptyMap(),
        statusTracker
    );

    // Verify that the segments of WIKI are preferred even though they are older
    Assert.assertTrue(iterator.hasNext());
    CompactionCandidate next = iterator.next();
    Assert.assertEquals(TestDataSource.WIKI, next.getDataSource());
    Assert.assertEquals(Intervals.of("2012-01-01/P1D"), next.getUmbrellaInterval());

    Assert.assertTrue(iterator.hasNext());
    next = iterator.next();
    Assert.assertEquals(TestDataSource.KOALA, next.getDataSource());
    Assert.assertEquals(Intervals.of("2013-01-01/P1D"), next.getUmbrellaInterval());
  }

  private CompactionSegmentIterator createIterator(DataSourceCompactionConfig config, SegmentTimeline timeline)
  {
    return new PriorityBasedCompactionSegmentIterator(
        policy,
        Collections.singletonMap(TestDataSource.WIKI, config),
        Collections.singletonMap(TestDataSource.WIKI, timeline),
        Collections.emptyMap(),
        statusTracker
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
      final List<DataSegment> segments = iterator.next().getSegments();

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

  private static CreateDataSegments createSegments()
  {
    return CreateDataSegments.ofDatasource(TestDataSource.WIKI);
  }

  private static SegmentTimeline createTimeline(
      CreateDataSegments... segmentBuilders
  )
  {
    final SegmentTimeline timeline = new SegmentTimeline();
    for (CreateDataSegments builder : segmentBuilders) {
      timeline.addSegments(builder.eachOfSizeInMb(100).iterator());
    }

    return timeline;
  }

  private static DataSourceCompactionConfig createConfigWithSegmentGranularity(
      Granularity segmentGranularity 
  )
  {
    return configBuilder().withGranularitySpec(
        new UserCompactionTaskGranularityConfig(segmentGranularity, null, null)
    ).build();
  }

  private static DataSourceCompactionConfig.Builder configBuilder()
  {
    return DataSourceCompactionConfig.builder()
                                     .forDataSource(TestDataSource.WIKI)
                                     .withSkipOffsetFromLatest(Period.seconds(0));
  }
}
