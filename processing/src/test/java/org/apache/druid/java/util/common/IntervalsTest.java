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
  public void testValidIntervalStrings()
  {
    final String[] intervalStringRepresentations = new String[]{
        // Tests that use does not fallback to Intervals.of()
        // Zulu with millis
        "2022-01-01T00:00:00.000Z/2022-01-02T00:00:00.000Z",
        "2021-03-14T12:34:56.789Z/2021-03-15T12:34:56.789Z",

        // Offset with colon
        "2022-01-01T00:00:00.000+05:30/2022-01-01T01:00:00.000+05:30",
        "2022-01-01T07:00:00.000-07:00/2022-01-01T08:00:00.000-07:00",

        // Basic offset without colon
        "2022-01-01T00:00:00.000+0530/2022-01-01T01:00:00.000+0530",

        // Tests that fallback to Intervals.of()
        // Zulu without millis
        "2022-01-01T00:00:00Z/2022-01-02T00:00:00Z",
        // Date-only
        "2022-01-01/2022-01-02",
        // start/period
        "2022-01-01T00:00:00.000Z/P1D",
        "2022-01-01T12:00:00Z/PT6H",
        "2022-01-01T00:00:00Z/P2DT3H4M5S",
        // period/end
        "P1D/2022-01-02T00:00:00.000Z",
        "PT6H/2022-01-01T18:00:00Z",
        "P2DT3H4M5S/2022-01-03T03:04:05Z"
    };

    for (String s : intervalStringRepresentations) {
      Interval expected = Intervals.of(s);
      Interval actual = Intervals.fromString(s);
      Assert.assertEquals("Mismatch for: " + s, expected, actual);
    }
  }

  @Test
  public void testInvalidInterval()
  {
    DruidExceptionMatcher.invalidInput().assertThrowsAndMatches(
        () -> Intervals.of("invalid string")
    );
  }
}
