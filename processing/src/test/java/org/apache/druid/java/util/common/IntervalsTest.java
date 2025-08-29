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

package org.apache.druid.java.util.common;

import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class IntervalsTest
{

  @Test
  public void testFindOverlappingInterval()
  {
    final Interval[] sortedIntervals = new Interval[]{
        Intervals.of("2019/2020"),
        Intervals.of("2021/2022"),
        Intervals.of("2021-04-01/2021-05-01"),
        Intervals.of("2022/2023")
    };
    Arrays.sort(sortedIntervals, Comparators.intervalsByStartThenEnd());

    // Search interval outside the bounds of the sorted intervals
    Assert.assertNull(
        Intervals.findOverlappingInterval(Intervals.of("2018/2019"), sortedIntervals)
    );
    Assert.assertNull(
        Intervals.findOverlappingInterval(Intervals.of("2023/2024"), sortedIntervals)
    );

    // Search interval within bounds, overlap exists
    // Fully overlapping interval
    Assert.assertEquals(
        Intervals.of("2021/2022"),
        Intervals.findOverlappingInterval(Intervals.of("2021/2022"), sortedIntervals)
    );

    // Partially overlapping interval
    Assert.assertEquals(
        Intervals.of("2022/2023"),
        Intervals.findOverlappingInterval(Intervals.of("2022-01-01/2022-01-02"), sortedIntervals)
    );

    Assert.assertEquals(
        Intervals.of("2021/2022"),
        Intervals.findOverlappingInterval(Intervals.of("2021-06-01/2021-07-01"), sortedIntervals)
    );

    // Overlap with multiple intervals, "smallest" one is returned
    Assert.assertEquals(
        Intervals.of("2021/2022"),
        Intervals.findOverlappingInterval(Intervals.of("2021-03-01/2021-04-01"), sortedIntervals)
    );

    // Search interval within bounds, no overlap
    Assert.assertNull(
        Intervals.findOverlappingInterval(Intervals.of("2020-01-02/2020-03-03"), sortedIntervals)
    );
  }

  @Test
  public void testInvalidInterval()
  {
    DruidExceptionMatcher.invalidInput().assertThrowsAndMatches(
        () -> Intervals.of("invalid string")
    );
  }

  @Test
  public void testComplementOf()
  {
    Assert.assertEquals(
        List.of(),
        Intervals.complementOf(Intervals.ETERNITY)
    );

    testComplementOf("2020/P1Y");
    testComplementOf("2001/2001-01");
    testComplementOf("2001-01-02/2001-02");
  }

  private void testComplementOf(String interval)
  {
    final Interval testInterval = Intervals.of(interval);
    final List<Interval> complement = List.of(
        new Interval(DateTimes.MIN, testInterval.getStart()),
        new Interval(testInterval.getEnd(), DateTimes.MAX)
    );
    Assert.assertEquals(
        complement,
        Intervals.complementOf(testInterval)
    );
    Assert.assertEquals(
        Intervals.ONLY_ETERNITY,
        JodaUtils.condenseIntervals(
            List.of(complement.get(0), complement.get(1), testInterval)
        )
    );
  }
}
