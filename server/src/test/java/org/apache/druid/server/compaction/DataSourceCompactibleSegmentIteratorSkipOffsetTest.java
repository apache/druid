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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

public class DataSourceCompactibleSegmentIteratorSkipOffsetTest
{
  @Test
  public void test_computeEarliestSkipInterval_withoutGranularity()
  {
    final DateTime earliestTimestamp = DateTimes.of("2018-01-01");
    final Period skipOffset = Period.days(7);

    final Interval skipInterval = DataSourceCompactibleSegmentIterator.computeEarliestSkipInterval(
        null,
        earliestTimestamp,
        skipOffset
    );

    // Without granularity: [2018-01-01, 2018-01-08)
    Assert.assertEquals(Intervals.of("2018-01-01/2018-01-08"), skipInterval);
  }

  @Test
  public void test_computeEarliestSkipInterval_withGranularity()
  {
    final DateTime earliestTimestamp = DateTimes.of("2018-01-01");
    final Period skipOffset = Period.days(7);

    final Interval skipInterval = DataSourceCompactibleSegmentIterator.computeEarliestSkipInterval(
        Granularities.DAY,
        earliestTimestamp,
        skipOffset
    );

    // With DAY granularity, skip offset bucket start: [2018-01-01, 2018-01-08)
    Assert.assertEquals(Intervals.of("2018-01-01/2018-01-08"), skipInterval);
  }

  @Test
  public void test_computeEarliestSkipInterval_zeroOffset()
  {
    final DateTime earliestTimestamp = DateTimes.of("2018-01-01");
    final Period skipOffset = Period.ZERO;

    final Interval skipInterval = DataSourceCompactibleSegmentIterator.computeEarliestSkipInterval(
        null,
        earliestTimestamp,
        skipOffset
    );

    // Zero offset results in an empty interval (start == end)
    Assert.assertEquals(Intervals.of("2018-01-01/2018-01-01"), skipInterval);
  }

  @Test
  public void test_sortAndAddSkipIntervals_bothOffsets()
  {
    final Interval latestSkipInterval = Intervals.of("2018-12-25/2019-01-01");
    final Interval earliestSkipInterval = Intervals.of("2018-01-01/2018-01-08");

    final java.util.List<Interval> skipIntervals = DataSourceCompactibleSegmentIterator.sortAndAddSkipIntervals(
        latestSkipInterval,
        earliestSkipInterval,
        null
    );

    // Should have both intervals, sorted
    Assert.assertEquals(2, skipIntervals.size());
    Assert.assertEquals(earliestSkipInterval, skipIntervals.get(0));
    Assert.assertEquals(latestSkipInterval, skipIntervals.get(1));
  }

  @Test
  public void test_sortAndAddSkipIntervals_withExistingIntervals()
  {
    final Interval latestSkipInterval = Intervals.of("2018-12-25/2019-01-01");
    final Interval earliestSkipInterval = Intervals.of("2018-01-01/2018-01-08");
    final java.util.List<Interval> existingSkipIntervals = java.util.List.of(
        Intervals.of("2018-06-01/2018-06-15")
    );

    final java.util.List<Interval> skipIntervals = DataSourceCompactibleSegmentIterator.sortAndAddSkipIntervals(
        latestSkipInterval,
        earliestSkipInterval,
        existingSkipIntervals
    );

    // Should have all three intervals, sorted
    Assert.assertEquals(3, skipIntervals.size());
    Assert.assertEquals(earliestSkipInterval, skipIntervals.get(0));
    Assert.assertEquals(Intervals.of("2018-06-01/2018-06-15"), skipIntervals.get(1));
    Assert.assertEquals(latestSkipInterval, skipIntervals.get(2));
  }

  @Test
  public void test_sortAndAddSkipIntervals_emptyEarliestInterval()
  {
    final Interval latestSkipInterval = Intervals.of("2018-12-25/2019-01-01");
    final Interval earliestSkipInterval = Intervals.of("2018-01-01/2018-01-01"); // Empty interval

    final java.util.List<Interval> skipIntervals = DataSourceCompactibleSegmentIterator.sortAndAddSkipIntervals(
        latestSkipInterval,
        earliestSkipInterval,
        null
    );

    // Empty earliest interval should not be added
    Assert.assertEquals(1, skipIntervals.size());
    Assert.assertEquals(latestSkipInterval, skipIntervals.get(0));
  }
}
