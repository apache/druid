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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HighScoreSegmentFirstIteratorTest
{
  private final float timeWeight = DataSourceCompactionConfig.DEFAULT_COMPACTION_TIME_WEIGHT_FOR_RECENT_DAYS;
  private final Period recentDaysForTimeWeight = DataSourceCompactionConfig.DEFAULT_COMPACTION_RECENT_DAYS;
  private final float segmentsNumWeight = DataSourceCompactionConfig.DEFAULT_COMPACTION_SEGMENT_NUM_WEIGHT;

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
    final List<Interval> skipIntervals = NewestSegmentFirstIterator.filterSkipIntervals(
        totalInterval,
        Lists.newArrayList(
            Intervals.of("2017-12-01/2018-01-15"),
            Intervals.of("2018-03-02/2018-07-23"),
            Intervals.of("2018-10-01/2018-10-02"),
            Intervals.of("2018-12-25/2018-12-31")
        )
    );

    Assert.assertEquals(expectedSkipIntervals, skipIntervals);
  }

  @Test
  public void testAddSkipIntervalFromLatestAndSort()
  {
    final List<Interval> expectedIntervals = ImmutableList.of(
        Intervals.of("2018-12-24/2018-12-25"),
        Intervals.of("2018-12-29/2019-01-01")
    );
    final List<Interval> fullSkipIntervals = NewestSegmentFirstIterator.sortAndAddSkipIntervalFromLatest(
        DateTimes.of("2019-01-01"),
        new Period(72, 0, 0, 0),
        ImmutableList.of(
            Intervals.of("2018-12-30/2018-12-31"),
            Intervals.of("2018-12-24/2018-12-25")
        )
    );

    Assert.assertEquals(expectedIntervals, fullSkipIntervals);
  }

  @Test
  public void testHighScoreForNewTimeFirst()
  {
    final long inputSegmentSizeBytes = 8000000;
    final VersionedIntervalTimeline<String, DataSegment> timeline = NewestSegmentFirstPolicyTest.createTimeline(
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-01T00:00:00/2021-01-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2,
            2
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-03T00:00:00/2021-01-04T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2,
            2
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-05T00:00:00/2021-01-06T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2, // first
            2
        )
    );
    List<Interval> searchInterval = new ArrayList<>();
    searchInterval.add(Intervals.of("2021-01-01/2021-02-01"));
    List<TimelineObjectHolder<String, DataSegment>> holders = searchInterval
        .stream()
        .flatMap(interval -> timeline
            .lookup(interval)
            .stream()
            // 过滤掉interval下所有segment大小全为0的interval.
            .filter(holder -> HighScoreSegmentFirstIterator.isCompactibleHolder(interval, holder))
        )
        .collect(Collectors.toList());

    List<Interval> computIntervals = new ArrayList<>();
    final List<HighScoreSegmentFirstIterator.Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tuple2s
        = HighScoreSegmentFirstIterator.computeTimelineScore(
        holders,
        timeWeight,
        recentDaysForTimeWeight,
        segmentsNumWeight
    );
    tuple2s.forEach(t -> System.out.println(t._1 + "," + t._2.getInterval()));
    tuple2s.forEach(t -> computIntervals.add(t._2.getInterval()));

    Assert.assertEquals(ImmutableList.of(
        Intervals.of("2021-01-01T00:00:00.000Z/2021-01-02T00:00:00.000Z"),
        Intervals.of("2021-01-03T00:00:00.000Z/2021-01-04T00:00:00.000Z"),
        Intervals.of("2021-01-05T00:00:00.000Z/2021-01-06T00:00:00.000Z")
    ), computIntervals);
  }

  @Test
  public void testHighScoreForSmallSizeFirst()
  {
    final long inputSegmentSizeBytes = 8000000;
    final VersionedIntervalTimeline<String, DataSegment> timeline = NewestSegmentFirstPolicyTest.createTimeline(
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-01T00:00:00/2021-01-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2,
            2
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-03T00:00:00/2021-01-04T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 50, // first
            4
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-05T00:00:00/2021-01-06T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2 + 1,
            2
        )
    );
    List<Interval> searchInterval = new ArrayList<>();
    searchInterval.add(Intervals.of("2021-01-01/2021-02-01"));
    List<TimelineObjectHolder<String, DataSegment>> holders = searchInterval
        .stream()
        .flatMap(interval -> timeline
            .lookup(interval)
            .stream()
            // 过滤掉interval下所有segment大小全为0的interval.
            .filter(holder -> HighScoreSegmentFirstIterator.isCompactibleHolder(interval, holder))
        )
        .collect(Collectors.toList());

    List<Interval> computIntervals = new ArrayList<>();
    final List<HighScoreSegmentFirstIterator.Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tuple2s
        = HighScoreSegmentFirstIterator.computeTimelineScore(
        holders,
        timeWeight,
        recentDaysForTimeWeight,
        segmentsNumWeight
    );
    tuple2s.forEach(t -> System.out.println(t._1 + "," + t._2.getInterval()));

    tuple2s.forEach(t -> computIntervals.add(t._2.getInterval()));

    Assert.assertEquals(ImmutableList.of(
        Intervals.of("2021-01-01T00:00:00.000Z/2021-01-02T00:00:00.000Z"),
        Intervals.of("2021-01-05T00:00:00.000Z/2021-01-06T00:00:00.000Z"),
        Intervals.of("2021-01-03T00:00:00.000Z/2021-01-04T00:00:00.000Z")
    ), computIntervals);
  }

  @Test
  public void testHighScoreForMaxSegmentNumShardFirst()
  {
    final long inputSegmentSizeBytes = 8000000;
    final VersionedIntervalTimeline<String, DataSegment> timeline = NewestSegmentFirstPolicyTest.createTimeline(
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-01T00:00:00/2021-01-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2 + 1,
            2
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-03T00:00:00/2021-01-04T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10, // first
            10
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-05T00:00:00/2021-01-06T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 2,
            2
        )
    );
    List<Interval> searchInterval = new ArrayList<>();
    searchInterval.add(Intervals.of("2021-01-01/2021-02-01"));
    List<TimelineObjectHolder<String, DataSegment>> holders = searchInterval
        .stream()
        .flatMap(interval -> timeline
            .lookup(interval)
            .stream()
            // 过滤掉interval下所有segment大小全为0的interval.
            .filter(holder -> HighScoreSegmentFirstIterator.isCompactibleHolder(interval, holder))
        )
        .collect(Collectors.toList());

    List<Interval> computIntervals = new ArrayList<>();
    final List<HighScoreSegmentFirstIterator.Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tuple2s
        = HighScoreSegmentFirstIterator.computeTimelineScore(
        holders,
        timeWeight,
        recentDaysForTimeWeight,
        segmentsNumWeight
    );
    tuple2s.forEach(t -> System.out.println(t._1 + "," + t._2.getInterval()));

    tuple2s.forEach(t -> computIntervals.add(t._2.getInterval()));

    Assert.assertEquals(ImmutableList.of(
        Intervals.of("2021-01-01T00:00:00.000Z/2021-01-02T00:00:00.000Z"),
        Intervals.of("2021-01-05T00:00:00.000Z/2021-01-06T00:00:00.000Z"),
        Intervals.of("2021-01-03T00:00:00.000Z/2021-01-04T00:00:00.000Z")
    ), computIntervals);
  }


  @Test
  public void testIntervalScoreForMinorAndMajorCompaction()
  {
    final long inputSegmentSizeBytes = 8000000;
    final VersionedIntervalTimeline<String, DataSegment> timeline = NewestSegmentFirstPolicyTest.createTimeline(
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2000-01-01T00:00:00/2000-01-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10 + 1, // minor last
            30
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2020-10-01T00:00:00/2020-10-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10 + 1,
            25
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-01T00:00:00/2021-01-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 8, // major first
            25
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-03T00:00:00/2021-01-04T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10 + 3,
            2
        )
    );

    List<Interval> searchInterval = new ArrayList<>();
    searchInterval.add(Intervals.of("2000-01-01/2021-02-01"));
    List<TimelineObjectHolder<String, DataSegment>> holders = searchInterval
        .stream()
        .flatMap(interval -> timeline
            .lookup(interval)
            .stream()
            // 过滤掉interval下所有segment大小全为0的interval.
            .filter(holder -> HighScoreSegmentFirstIterator.isCompactibleHolder(interval, holder))
        )
        .collect(Collectors.toList());

    // minor compaction
    List<Interval> computIntervals = new ArrayList<>();
    final List<HighScoreSegmentFirstIterator.Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tuple2s
        = HighScoreSegmentFirstIterator.computeTimelineScore(
        holders,
        timeWeight,
        recentDaysForTimeWeight,
        segmentsNumWeight
    );
    tuple2s.forEach(t -> System.out.println(t._1 + "," + t._2.getInterval()));

    tuple2s.forEach(t -> computIntervals.add(t._2.getInterval()));

    Assert.assertEquals(Intervals.of("2000-01-01T00:00:00.000Z/2000-01-02T00:00:00.000Z"), computIntervals.get(0));

    // major compaction
    float majorTimeWeight = timeWeight * HighScoreSegmentFirstIterator.MAJOR_COMPACTION_IGNORE_TIME_LEVEL;
    List<Interval> computIntervals2 = new ArrayList<>();
    final List<HighScoreSegmentFirstIterator.Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tuple2s2
        = HighScoreSegmentFirstIterator.computeTimelineScore(
        holders,
        majorTimeWeight,
        recentDaysForTimeWeight,
        segmentsNumWeight
    );
    tuple2s2.forEach(t -> System.out.println(t._1 + "," + t._2.getInterval()));

    tuple2s2.forEach(t -> computIntervals2.add(t._2.getInterval()));

    Assert.assertEquals(Intervals.of("2000-01-01T00:00:00.000Z/2000-01-02T00:00:00.000Z"), computIntervals2.get(0));
    Assert.assertEquals(
        Intervals.of("2021-01-01T00:00:00.000Z/2021-01-02T00:00:00.000Z"),
        computIntervals2.get(computIntervals2.size() - 1)
    );
  }

  @Test
  public void testHighScoreForLowTimePriority()
  {
    final float timeWeight = 0.1f;
    List<Interval> computIntervals = computeIntervalList(timeWeight);

    Assert.assertEquals(ImmutableList.of(
        Intervals.of("2021-01-03T00:00:00.000Z/2021-01-04T00:00:00.000Z"),
        Intervals.of("2021-01-02T00:00:00.000Z/2021-01-03T00:00:00.000Z"),
        Intervals.of("2021-01-01T00:00:00.000Z/2021-01-02T00:00:00.000Z")
    ), computIntervals);
  }

  @Test
  public void testHighScoreForHighTimePriority()
  {
    final float timeWeight = 1f;
    List<Interval> computIntervals = computeIntervalList(timeWeight);

    Assert.assertEquals(ImmutableList.of(
        Intervals.of("2021-01-01T00:00:00.000Z/2021-01-02T00:00:00.000Z"),
        Intervals.of("2021-01-02T00:00:00.000Z/2021-01-03T00:00:00.000Z"),
        Intervals.of("2021-01-03T00:00:00.000Z/2021-01-04T00:00:00.000Z")
    ), computIntervals);
  }

  private List<Interval> computeIntervalList(float timeWeight)
  {
    final long inputSegmentSizeBytes = 8000000;
    final VersionedIntervalTimeline<String, DataSegment> timeline = NewestSegmentFirstPolicyTest.createTimeline(
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-01T00:00:00/2021-01-02T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10 + 1000,
            20
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-02T00:00:00/2021-01-03T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10 + 2000, // first
            12
        ),
        new NewestSegmentFirstPolicyTest.SegmentGenerateSpec(
            Intervals.of("2021-01-03T00:00:00/2021-01-04T00:00:00"),
            new Period("P1D"),
            inputSegmentSizeBytes / 10 + 3000,
            2
        )
    );
    List<Interval> searchInterval = new ArrayList<>();
    searchInterval.add(Intervals.of("2021-01-01/2021-02-01"));
    List<TimelineObjectHolder<String, DataSegment>> holders = searchInterval
        .stream()
        .flatMap(interval -> timeline
            .lookup(interval)
            .stream()
            // 过滤掉interval下所有segment大小全为0的interval.
            .filter(holder -> HighScoreSegmentFirstIterator.isCompactibleHolder(interval, holder))
        )
        .collect(Collectors.toList());

    List<Interval> computIntervals = new ArrayList<>();
    final List<HighScoreSegmentFirstIterator.Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tuple2s
        = HighScoreSegmentFirstIterator.computeTimelineScore(
        holders,
        timeWeight,
        recentDaysForTimeWeight,
        segmentsNumWeight
    );
    tuple2s.forEach(t -> System.out.println(t._1 + "," + t._2.getInterval()));

    tuple2s.forEach(t -> computIntervals.add(t._2.getInterval()));
    return computIntervals;
  }
}
