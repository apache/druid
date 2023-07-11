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

package org.apache.druid.java.util.common.guava;

import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

/**
 */
public class ComparatorsTest
{
  @Test
  public void testIntervalsByStartThenEnd()
  {
    Comparator<Interval> comp = Comparators.intervalsByStartThenEnd();

    Assert.assertEquals(0, comp.compare(Intervals.of("P1d/2011-04-02"), Intervals.of("2011-04-01/2011-04-02")));
    Assert.assertEquals(-1, comp.compare(Intervals.of("2011-03-31/2011-04-02"), Intervals.of("2011-04-01/2011-04-02")));
    Assert.assertEquals(1, comp.compare(Intervals.of("2011-04-01/2011-04-02"), Intervals.of("2011-03-31/2011-04-02")));
    Assert.assertEquals(1, comp.compare(Intervals.of("2011-04-01/2011-04-03"), Intervals.of("2011-04-01/2011-04-02")));
    Assert.assertEquals(-1, comp.compare(Intervals.of("2011-04-01/2011-04-03"), Intervals.of("2011-04-01/2011-04-04")));

    Interval[] intervals = new Interval[]{
        Intervals.of("2011-04-01T18/2011-04-02T13"),
        Intervals.of("2011-04-01/2011-04-03"),
        Intervals.of("2011-04-01/2011-04-04"),
        Intervals.of("2011-04-02/2011-04-04"),
        Intervals.of("2011-04-01/2011-04-02"),
        Intervals.of("2011-04-02/2011-04-03"),
        Intervals.of("2011-04-02/2011-04-03T06")
    };
    Arrays.sort(intervals, comp);

    Assert.assertArrayEquals(
        new Interval[]{
            Intervals.of("2011-04-01/2011-04-02"),
            Intervals.of("2011-04-01/2011-04-03"),
            Intervals.of("2011-04-01/2011-04-04"),
            Intervals.of("2011-04-01T18/2011-04-02T13"),
            Intervals.of("2011-04-02/2011-04-03"),
            Intervals.of("2011-04-02/2011-04-03T06"),
            Intervals.of("2011-04-02/2011-04-04"),
        },
        intervals
    );
  }

  @Test
  public void testIntervalsByEndThenStart()
  {
    Comparator<Interval> comp = Comparators.intervalsByEndThenStart();

    Assert.assertEquals(0, comp.compare(Intervals.of("P1d/2011-04-02"), Intervals.of("2011-04-01/2011-04-02")));
    Assert.assertEquals(-1, comp.compare(Intervals.of("2011-04-01/2011-04-03"), Intervals.of("2011-04-01/2011-04-04")));
    Assert.assertEquals(1, comp.compare(Intervals.of("2011-04-01/2011-04-02"), Intervals.of("2011-04-01/2011-04-01")));
    Assert.assertEquals(-1, comp.compare(Intervals.of("2011-04-01/2011-04-03"), Intervals.of("2011-04-02/2011-04-03")));
    Assert.assertEquals(1, comp.compare(Intervals.of("2011-04-01/2011-04-03"), Intervals.of("2011-03-31/2011-04-03")));

    Interval[] intervals = new Interval[]{
        Intervals.of("2011-04-01T18/2011-04-02T13"),
        Intervals.of("2011-04-01/2011-04-03"),
        Intervals.of("2011-04-01/2011-04-04"),
        Intervals.of("2011-04-02/2011-04-04"),
        Intervals.of("2011-04-01/2011-04-02"),
        Intervals.of("2011-04-02/2011-04-03"),
        Intervals.of("2011-04-02/2011-04-03T06")
    };
    Arrays.sort(intervals, comp);

    Assert.assertArrayEquals(
        new Interval[]{
            Intervals.of("2011-04-01/2011-04-02"),
            Intervals.of("2011-04-01T18/2011-04-02T13"),
            Intervals.of("2011-04-01/2011-04-03"),
            Intervals.of("2011-04-02/2011-04-03"),
            Intervals.of("2011-04-02/2011-04-03T06"),
            Intervals.of("2011-04-01/2011-04-04"),
            Intervals.of("2011-04-02/2011-04-04")
            },
        intervals
    );
  }
}
