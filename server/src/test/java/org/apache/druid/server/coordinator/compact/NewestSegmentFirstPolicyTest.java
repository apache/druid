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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.transform.TransformSpec;
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
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.apache.druid.utils.Streams;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
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
  private static final String DATA_SOURCE = "dataSource";
  private static final long DEFAULT_SEGMENT_SIZE = 1000;
  private static final int DEFAULT_NUM_SEGMENTS_PER_SHARD = 4;
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final NewestSegmentFirstPolicy policy = new NewestSegmentFirstPolicy(mapper);

  @Test
  public void testLargeOffsetAndSmallSegmentInterval()
  {
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P2D"), null)),
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("PT1M"), null)),
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("PT1H1M"), null)),
        ImmutableMap.of(
            DATA_SOURCE,
            createTimeline(
                new SegmentGenerateSpec(Intervals.of("2017-11-16T20:00:00/2017-11-17T04:00:00"), segmentPeriod),
                // larger gap than SegmentCompactionUtil.LOOKUP_PERIOD (1 day)
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P1D"), null)),
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
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(800000, new Period("P1D"), null)),
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
  public void testSkipUnknownDataSource()
  {
    final String unknownDataSource = "unknown";
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(
            unknownDataSource,
            createCompactionConfig(10000, new Period("P2D"), null),
            DATA_SOURCE,
            createCompactionConfig(10000, new Period("P2D"), null)
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
    final SegmentTimeline timeline = createTimeline(
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(inputSegmentSizeBytes, new Period("P0D"), null)),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
                                                     .map(SegmentsToCompact::getSegments)
                                                     .collect(Collectors.toSet());
    Assert.assertEquals(
        observedSegments,
        ImmutableSet.of(expectedSegmentsToCompact, expectedSegmentsToCompact2)
    );
  }

  @Test
  public void testIfFirstSegmentIsInSkipOffset()
  {
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-12-02T14:00:00/2017-12-03T00:00:00"),
            new Period("PT5H"),
            40000,
            1
        )
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"), null)),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfFirstSegmentOverlapsSkipOffset()
  {
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-12-01T23:00:00/2017-12-03T00:00:00"),
            new Period("PT5H"),
            40000,
            1
        )
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"), null)),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIfSegmentsSkipOffsetWithConfiguredSegmentGranularityEqual()
  {
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(Intervals.of("2017-11-30T23:00:00/2017-12-03T00:00:00"), new Period("P1D")),
        new SegmentGenerateSpec(Intervals.of("2017-10-14T00:00:00/2017-10-15T00:00:00"), new Period("P1D"))
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"), new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    // We should only get segments in Oct
    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-14T00:00:00/2017-12-02T00:00:00"), Partitions.ONLY_COMPLETE)
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
        new SegmentGenerateSpec(Intervals.of("2017-11-30T23:00:00/2017-12-03T00:00:00"), new Period("PT5H")),
        new SegmentGenerateSpec(Intervals.of("2017-10-14T00:00:00/2017-10-15T00:00:00"), new Period("PT5H"))
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"), new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );

    // We should only get segments in Oct
    final List<DataSegment> expectedSegmentsToCompact = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.of("2017-10-14T00:00:00/2017-10-15T00:00:00"), Partitions.ONLY_COMPLETE)
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
        new SegmentGenerateSpec(Intervals.of("2017-12-01T23:00:00/2017-12-03T00:00:00"), new Period("PT5H")),
        new SegmentGenerateSpec(Intervals.of("2017-10-14T00:00:00/2017-10-15T00:00:00"), new Period("PT5H"))
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P1D"), new UserCompactionTaskGranularityConfig(Granularities.MINUTE, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    final Period segmentPeriod = new Period("PT1H");
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("P1D"), null)),
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
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(10000, new Period("PT1H"), null)),
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

  @Test
  public void testIteratorReturnsSegmentsInConfiguredSegmentGranularity()
  {
    final SegmentTimeline timeline = createTimeline(
        // Segments with day interval from Oct to Dec
        new SegmentGenerateSpec(Intervals.of("2017-10-01T00:00:00/2017-12-31T00:00:00"), new Period("P1D"))
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
        new SegmentGenerateSpec(Intervals.of("2020-01-01/2020-01-08"), new Period("P7D")),
        new SegmentGenerateSpec(Intervals.of("2020-01-28/2020-02-03"), new Period("P7D")),
        new SegmentGenerateSpec(Intervals.of("2020-02-08/2020-02-15"), new Period("P7D"))
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
        new SegmentGenerateSpec(Intervals.of("2017-12-01T00:00:00/2017-12-02T00:00:00"), new Period("P1D"))
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(40000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.MINUTE, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
        new SegmentGenerateSpec(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), new Period("P1D"), "1994-04-29T00:00:00.000Z", null),
        new SegmentGenerateSpec(Intervals.of("2017-10-01T01:00:00/2017-10-01T02:00:00"), new Period("PT1H"), "1994-04-30T00:00:00.000Z", null)
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.MONTH, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Auto compaction config sets segmentGranularity=DAY
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsNothingAsSegmentsWasCompactedAndHaveSameSegmentGranularityInLastCompactionState()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("segmentGranularity", "day"))
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("segmentGranularity", "day"))
        )
    );

    // Auto compaction config sets segmentGranularity=DAY
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsAsSegmentsWasCompactedAndHaveDifferentSegmentGranularity()
  {
    // Same indexSpec as what is set in the auto compaction config
    Map<String, Object> indexSpec = IndexSpec.DEFAULT.asMap(mapper);
    // Same partitionsSpec as what is set in the auto compaction config
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Auto compaction config sets segmentGranularity=YEAR
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("segmentGranularity", "day"))
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("segmentGranularity", "day"))
        )
    );

    // Auto compaction config sets segmentGranularity=YEAR
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Duration of new segmentGranularity is the same as before (P1D),
    // but we changed the timezone from UTC to Bangkok in the auto compaction spec
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(
                            130000,
                            new Period("P0D"),
                            new UserCompactionTaskGranularityConfig(
                                new PeriodGranularity(
                                    new Period("P1D"),
                                    null,
                                    DateTimeZone.forTimeZone(TimeZone.getTimeZone("Asia/Bangkok"))
                                ),
                                null,
                                null
                            )
                        )
        ),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Duration of new segmentGranularity is the same as before (P1D), but we changed the origin in the autocompaction spec
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(
                            130000,
                            new Period("P0D"),
                            new UserCompactionTaskGranularityConfig(
                                new PeriodGranularity(
                                    new Period("P1D"),
                                    DateTimes.of("2012-01-02T00:05:00.000Z"),
                                    DateTimeZone.UTC
                                ),
                                null,
                                null
                            )
                        )
        ),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have
    // rollup=false for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // rollup=true for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // and rollup=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (queryGranularity was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("rollup", "false"))
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("rollup", "true"))
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of())
        )
    );

    // Auto compaction config sets rollup=true
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(null, null, true))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have
    // queryGranularity=DAY for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // queryGranularity=MINUTE for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // and queryGranularity=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (queryGranularity was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("queryGranularity", "day"))
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of("queryGranularity", "minute"))
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, ImmutableMap.of())
        )
    );

    // Auto compaction config sets queryGranularity=MINUTE
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(null, Granularities.MINUTE, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have
    // Dimensions=["foo", "bar"] for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // Dimensions=["foo"] for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // Dimensions=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (dimensions was not set during last compaction)
    // and dimensionsSpec=null for interval 2017-10-04T00:00:00/2017-10-05T00:00:00 (dimensionsSpec was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))), null, null, indexSpec, null)
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))), null, null, indexSpec, null)
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, DimensionsSpec.EMPTY, null, null, indexSpec, null)
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-04T00:00:00/2017-10-05T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Auto compaction config sets Dimensions=["foo"]
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            new UserCompactionTaskDimensionsConfig(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))),
            null,
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            new UserCompactionTaskDimensionsConfig(null),
            null,
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have
    // filter=SelectorDimFilter("dim1", "foo", null) for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // filter=SelectorDimFilter("dim1", "bar", null) for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // filter=null for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (filter was not set during last compaction)
    // and transformSpec=null for interval 2017-10-04T00:00:00/2017-10-05T00:00:00 (transformSpec was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                null,
                mapper.readValue(mapper.writeValueAsString(new TransformSpec(new SelectorDimFilter("dim1", "foo", null), null)), new TypeReference<Map<String, Object>>() {}),
                indexSpec,
                null
            )
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                null,
                mapper.readValue(mapper.writeValueAsString(new TransformSpec(new SelectorDimFilter("dim1", "bar", null), null)), new TypeReference<Map<String, Object>>() {}),
                indexSpec,
                null
            )
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                null,
                mapper.readValue(mapper.writeValueAsString(new TransformSpec(null, null)), new TypeReference<Map<String, Object>>() {}),
                indexSpec,
                null
            )
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-04T00:00:00/2017-10-05T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Auto compaction config sets filter=SelectorDimFilter("dim1", "bar", null)
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            null,
            new UserCompactionTaskTransformConfig(new SelectorDimFilter("dim1", "bar", null)),
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            null,
            new UserCompactionTaskTransformConfig(null),
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have
    // metricsSpec={CountAggregatorFactory("cnt")} for interval 2017-10-01T00:00:00/2017-10-02T00:00:00,
    // metricsSpec={CountAggregatorFactory("cnt"), LongSumAggregatorFactory("val", "val")} for interval 2017-10-02T00:00:00/2017-10-03T00:00:00,
    // metricsSpec=[] for interval 2017-10-03T00:00:00/2017-10-04T00:00:00 (filter was not set during last compaction)
    // and metricsSpec=null for interval 2017-10-04T00:00:00/2017-10-05T00:00:00 (transformSpec was not set during last compaction)
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                mapper.convertValue(new AggregatorFactory[] {new CountAggregatorFactory("cnt")}, new TypeReference<List<Object>>() {}),
                null,
                indexSpec,
                null
            )
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                mapper.convertValue(new AggregatorFactory[] {new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val")}, new TypeReference<List<Object>>() {}),
                null,
                indexSpec,
                null
            )
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-03T00:00:00/2017-10-04T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                mapper.convertValue(new AggregatorFactory[] {}, new TypeReference<List<Object>>() {}),
                null,
                indexSpec,
                null
            )
        ),
        new SegmentGenerateSpec(
            Intervals.of("2017-10-04T00:00:00/2017-10-05T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, indexSpec, null)
        )
    );

    // Auto compaction config sets metricsSpec={CountAggregatorFactory("cnt"), LongSumAggregatorFactory("val", "val")}
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            null,
            null,
            new AggregatorFactory[] {new CountAggregatorFactory("cnt"), new LongSumAggregatorFactory("val", "val")}
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            null,
            null,
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );
    // No more
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorReturnsSegmentsSmallerSegmentGranularityCoveringMultipleSegmentsInTimeline()
  {
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"), new Period("P1D"), "1994-04-29T00:00:00.000Z", null),
        new SegmentGenerateSpec(Intervals.of("2017-10-01T01:00:00/2017-10-01T02:00:00"), new Period("PT1H"), "1994-04-30T00:00:00.000Z", null)
    );

    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(130000, new Period("P0D"), new UserCompactionTaskGranularityConfig(Granularities.HOUR, null, null))),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    Map<String, Object> newIndexSpecMap = mapper.convertValue(newIndexSpec, new TypeReference<Map<String, Object>>() {});
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));

    // Create segments that were compacted (CompactionState != null) and have segmentGranularity=DAY
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-02T00:00:00/2017-10-03T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(partitionsSpec, null, null, null, newIndexSpecMap, null)
        )
    );

    // Duration of new segmentGranularity is the same as before (P1D)
    final CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(
                            130000,
                            new Period("P0D"),
                            new UserCompactionTaskGranularityConfig(
                                new PeriodGranularity(
                                    new Period("P1D"),
                                    null,
                                    DateTimeZone.UTC
                                ),
                                null,
                                null
                            )
                        )
        ),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
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
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(ClientCompactionTaskQueryTuningConfig.from(null, null, null));
    final SegmentTimeline timeline = createTimeline(
        new SegmentGenerateSpec(
            Intervals.of("2017-10-01T00:00:00/2017-10-02T00:00:00"),
            new Period("P1D"),
            null,
            new CompactionState(
                partitionsSpec,
                null,
                null,
                null,
                IndexSpec.DEFAULT.asMap(mapper),
                null
            )
        )
    );

    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            null,
            null,
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
            ),
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );
    Assert.assertFalse(iterator.hasNext());

    iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE, createCompactionConfig(
            130000,
            new Period("P0D"),
            null,
            null,
            null,
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
            ),
            null
        )),
        ImmutableMap.of(DATA_SOURCE, timeline),
        Collections.emptyMap()
    );
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipAllGranularityToDefault()
  {
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               null
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(
                                            new DataSegment(
                                                DATA_SOURCE,
                                                Intervals.ETERNITY,
                                                "0",
                                                new HashMap<>(),
                                                new ArrayList<>(),
                                                new ArrayList<>(),
                                                new NumberedShardSpec(0, 0),
                                                0,
                                                100)
                                        )
            )
        ),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipFirstHalfEternityToDefault()
  {
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               null
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(
                                            new DataSegment(
                                                DATA_SOURCE,
                                                new Interval(DateTimes.MIN, DateTimes.of("2024-01-01")),
                                                "0",
                                                new HashMap<>(),
                                                new ArrayList<>(),
                                                new ArrayList<>(),
                                                new NumberedShardSpec(0, 0),
                                                0,
                                                100)
                                        )
            )
        ),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipSecondHalfOfEternityToDefault()
  {
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               null
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(
                                            new DataSegment(
                                                DATA_SOURCE,
                                                new Interval(DateTimes.of("2024-01-01"), DateTimes.MAX),
                                                "0",
                                                new HashMap<>(),
                                                new ArrayList<>(),
                                                new ArrayList<>(),
                                                new NumberedShardSpec(0, 0),
                                                0,
                                                100)
                                        )
            )
        ),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipAllToAllGranularity()
  {
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               new UserCompactionTaskGranularityConfig(Granularities.ALL, null, null)
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(
                                            new DataSegment(
                                                DATA_SOURCE,
                                                Intervals.ETERNITY,
                                                "0",
                                                new HashMap<>(),
                                                new ArrayList<>(),
                                                new ArrayList<>(),
                                                new NumberedShardSpec(0, 0),
                                                0,
                                                100)
                                        )
            )
        ),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipAllToFinerGranularity()
  {
    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               new UserCompactionTaskGranularityConfig(Granularities.DAY, null, null)
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(
                                            new DataSegment(
                                                DATA_SOURCE,
                                                Intervals.ETERNITY,
                                                "0",
                                                new HashMap<>(),
                                                new ArrayList<>(),
                                                new ArrayList<>(),
                                                new NumberedShardSpec(0, 0),
                                                0,
                                                100)
                                        )
            )
        ),
        Collections.emptyMap()
    );

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSkipCompactionForIntervalsContainingSingleTombstone()
  {
    final DataSegment tombstone2023 = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2023/2024"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    final DataSegment dataSegment2023 = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2023/2024"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new NumberedShardSpec(1, 0),
        0,
        100);
    final DataSegment tombstone2024 = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2024/2025"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);

    CompactionSegmentIterator iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(tombstone2023, dataSegment2023, tombstone2024))
        ),
        Collections.emptyMap()
    );

    // Skips 2024/2025 since it has a single tombstone and no data.
    // Return all segments in 2023/2024 since at least one of them has data despite there being a tombstone.
    Assert.assertEquals(
        ImmutableList.of(tombstone2023, dataSegment2023),
        iterator.next().getSegments()
    );

    final DataSegment tombstone2025Jan = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2025-01-01/2025-02-01"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    final DataSegment tombstone2025Feb = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2025-02-01/2025-03-01"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    final DataSegment tombstone2025Mar = new DataSegment(
        DATA_SOURCE,
        Intervals.of("2025-03-01/2025-04-01"),
        "0",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        TombstoneShardSpec.INSTANCE,
        0,
        1);
    iterator = policy.reset(
        ImmutableMap.of(DATA_SOURCE,
                        createCompactionConfig(10000,
                                               new Period("P0D"),
                                               new UserCompactionTaskGranularityConfig(Granularities.YEAR, null, null)
                        )
        ),
        ImmutableMap.of(
            DATA_SOURCE,
            SegmentTimeline.forSegments(ImmutableSet.of(
                tombstone2023,
                dataSegment2023,
                tombstone2024,
                tombstone2025Jan,
                tombstone2025Feb,
                tombstone2025Mar
            ))
        ),
        Collections.emptyMap()
    );
    // Does not skip the tombstones in 2025 since there are multiple of them which could potentially be condensed to one
    Assert.assertEquals(
        ImmutableList.of(tombstone2025Jan, tombstone2025Feb, tombstone2025Mar),
        iterator.next().getSegments()
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

  private static SegmentTimeline createTimeline(SegmentGenerateSpec... specs)
  {
    List<DataSegment> segments = new ArrayList<>();
    final String version = DateTimes.nowUtc().toString();

    final List<SegmentGenerateSpec> orderedSpecs = Arrays.asList(specs);
    orderedSpecs.sort(Comparator.comparing(s -> s.totalInterval, Comparators.intervalsByStartThenEnd().reversed()));

    for (SegmentGenerateSpec spec : orderedSpecs) {
      Interval remainingInterval = spec.totalInterval;

      while (!Intervals.isEmpty(remainingInterval)) {
        final Interval segmentInterval;
        if (remainingInterval.toDuration().isLongerThan(spec.segmentPeriod.toStandardDuration())) {
          segmentInterval = new Interval(spec.segmentPeriod, remainingInterval.getEnd());
        } else {
          segmentInterval = remainingInterval;
        }

        for (int i = 0; i < spec.numSegmentsPerShard; i++) {
          final ShardSpec shardSpec = new NumberedShardSpec(i, spec.numSegmentsPerShard);
          final DataSegment segment = new DataSegment(
              DATA_SOURCE,
              segmentInterval,
              spec.version == null ? version : spec.version,
              null,
              ImmutableList.of(),
              ImmutableList.of(),
              shardSpec,
              spec.lastCompactionState,
              0,
              spec.segmentSize
          );
          segments.add(segment);
        }

        remainingInterval = removeIntervalFromEnd(remainingInterval, segmentInterval);
      }
    }

    return SegmentTimeline.forSegments(segments);
  }

  /**
   * Returns an interval [largeInterval.start - smallInterval.start) given that
   * the end of both intervals is the same.
   */
  private static Interval removeIntervalFromEnd(Interval largeInterval, Interval smallInterval)
  {
    Preconditions.checkArgument(
        largeInterval.getEnd().equals(smallInterval.getEnd()),
        "end should be same. largeInterval[%s] smallInterval[%s]",
        largeInterval,
        smallInterval
    );
    return new Interval(largeInterval.getStart(), smallInterval.getStart());
  }

  private DataSourceCompactionConfig createCompactionConfig(
      long inputSegmentSizeBytes,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec
  )
  {
    return createCompactionConfig(inputSegmentSizeBytes, skipOffsetFromLatest, granularitySpec, null, null, null, null);
  }

  private DataSourceCompactionConfig createCompactionConfig(
      long inputSegmentSizeBytes,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      UserCompactionTaskDimensionsConfig dimensionsSpec,
      UserCompactionTaskTransformConfig transformSpec,
      AggregatorFactory[] metricsSpec
  )
  {
    return createCompactionConfig(inputSegmentSizeBytes, skipOffsetFromLatest, granularitySpec, dimensionsSpec, transformSpec, null, metricsSpec);
  }

  private DataSourceCompactionConfig createCompactionConfig(
      long inputSegmentSizeBytes,
      Period skipOffsetFromLatest,
      UserCompactionTaskGranularityConfig granularitySpec,
      UserCompactionTaskDimensionsConfig dimensionsSpec,
      UserCompactionTaskTransformConfig transformSpec,
      UserCompactionTaskQueryTuningConfig tuningConfig,
      AggregatorFactory[] metricsSpec
  )
  {
    return new DataSourceCompactionConfig(
        DATA_SOURCE,
        0,
        inputSegmentSizeBytes,
        null,
        skipOffsetFromLatest,
        tuningConfig,
        granularitySpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
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
    private final String version;
    private final CompactionState lastCompactionState;

    SegmentGenerateSpec(Interval totalInterval, Period segmentPeriod)
    {
      this(totalInterval, segmentPeriod, null, null);
    }

    SegmentGenerateSpec(Interval totalInterval, Period segmentPeriod, String version, CompactionState lastCompactionState)
    {
      this(totalInterval, segmentPeriod, DEFAULT_SEGMENT_SIZE, DEFAULT_NUM_SEGMENTS_PER_SHARD, version, lastCompactionState);
    }

    SegmentGenerateSpec(Interval totalInterval, Period segmentPeriod, long segmentSize, int numSegmentsPerShard)
    {
      this(totalInterval, segmentPeriod, segmentSize, numSegmentsPerShard, null, null);
    }

    SegmentGenerateSpec(Interval totalInterval, Period segmentPeriod, long segmentSize, int numSegmentsPerShard, String version, CompactionState lastCompactionState)
    {
      Preconditions.checkArgument(numSegmentsPerShard >= 1);
      this.totalInterval = totalInterval;
      this.segmentPeriod = segmentPeriod;
      this.segmentSize = segmentSize;
      this.numSegmentsPerShard = numSegmentsPerShard;
      this.version = version;
      this.lastCompactionState = lastCompactionState;
    }
  }
}
