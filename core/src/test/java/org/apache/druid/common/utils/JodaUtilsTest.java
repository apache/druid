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

package org.apache.druid.common.utils;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class JodaUtilsTest
{
  @Test
  public void testUmbrellaIntervalsSimple()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-01-01/2011-01-03"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-05/2011-03-06"),
        Intervals.of("2011-02-01/2011-02-02")
    );

    Assert.assertEquals(
        Intervals.of("2011-01-01/2011-03-06"),
        JodaUtils.umbrellaInterval(intervals)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUmbrellaIntervalsNull()
  {
    JodaUtils.umbrellaInterval(Collections.emptyList());
  }

  @Test
  public void testCondenseIntervalsSimple()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06")
    );

    List<Interval> expected = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-03"),
        Intervals.of("2011-02-01/2011-02-08"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06")
    );

    List<Interval> actual = JodaUtils.condenseIntervals(intervals);

    Assert.assertEquals(
        expected,
        actual
    );

  }


  @Test
  public void testCondenseIntervalsSimpleSortedIterator()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06")
    );
    intervals.sort(Comparators.intervalsByStartThenEnd());

    List<Interval> actual = ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()));
    Assert.assertEquals(
        Arrays.asList(
            Intervals.of("2011-01-01/2011-01-03"),
            Intervals.of("2011-02-01/2011-02-08"),
            Intervals.of("2011-03-01/2011-03-02"),
            Intervals.of("2011-03-03/2011-03-04"),
            Intervals.of("2011-03-05/2011-03-06")
        ),
        actual
    );

  }

  @Test
  public void testCondenseIntervalsSimpleSortedIteratorOverlapping()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-02-01/2011-03-10"),
        Intervals.of("2011-01-02/2011-02-03"),
        Intervals.of("2011-01-07/2015-01-19"),
        Intervals.of("2011-01-15/2011-01-19"),
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-02-01/2011-03-10")
    );

    intervals.sort(Comparators.intervalsByStartThenEnd());

    Assert.assertEquals(
        Collections.singletonList(
            Intervals.of("2011-01-01/2015-01-19")
        ),
        ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()))
    );
  }

  @Test(expected = IAE.class)
  public void testCondenseIntervalsSimplSortedIteratorOverlappingWithNullsShouldThrow()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-02/2011-02-03"),
        Intervals.of("2011-02-01/2011-03-10"),
        null,
        Intervals.of("2011-03-07/2011-04-19"),
        Intervals.of("2011-04-01/2015-01-19"),
        null
    );
    ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()));
  }

  @Test(expected = IAE.class)
  public void testCondenseIntervalsSimplSortedIteratorOverlappingWithNullFirstAndLastshouldThrow()
  {
    List<Interval> intervals = Arrays.asList(
        null,
        Intervals.of("2011-01-02/2011-02-03"),
        Intervals.of("2011-02-01/2011-03-10"),
        Intervals.of("2011-03-07/2011-04-19"),
        Intervals.of("2011-04-01/2015-01-19"),
        null
    );
    ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCondenseIntervalsSimpleUnsortedIterator()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06")
    );
    ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCondenseIntervalsSimpleUnsortedIteratorSmallestAtEnd()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-02-01/2011-02-04"),
        Intervals.of("2011-03-01/2011-03-04"),
        Intervals.of("2010-01-01/2010-03-04")
    );
    ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()));
  }

  @Test
  public void testCondenseIntervalsIteratorWithDups()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-02-04/2011-02-05"),
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06")
    );
    intervals.sort(Comparators.intervalsByStartThenEnd());

    Assert.assertEquals(
        Arrays.asList(
            Intervals.of("2011-01-01/2011-01-03"),
            Intervals.of("2011-02-01/2011-02-08"),
            Intervals.of("2011-03-01/2011-03-02"),
            Intervals.of("2011-03-03/2011-03-04"),
            Intervals.of("2011-03-05/2011-03-06")
        ),
        ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.iterator()))
    );
  }

  @Test
  public void testCondenseIntervalsMixedUp()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-01-01/2011-01-02"),
        Intervals.of("2011-01-02/2011-01-03"),
        Intervals.of("2011-02-01/2011-02-05"),
        Intervals.of("2011-02-01/2011-02-02"),
        Intervals.of("2011-02-03/2011-02-08"),
        Intervals.of("2011-03-01/2011-03-02"),
        Intervals.of("2011-03-03/2011-03-04"),
        Intervals.of("2011-03-05/2011-03-06"),
        Intervals.of("2011-04-01/2011-04-05"),
        Intervals.of("2011-04-02/2011-04-03"),
        Intervals.of("2011-05-01/2011-05-05"),
        Intervals.of("2011-05-02/2011-05-07")
    );

    for (int i = 0; i < 20; ++i) {
      Collections.shuffle(intervals);
      Assert.assertEquals(
          Arrays.asList(
              Intervals.of("2011-01-01/2011-01-03"),
              Intervals.of("2011-02-01/2011-02-08"),
              Intervals.of("2011-03-01/2011-03-02"),
              Intervals.of("2011-03-03/2011-03-04"),
              Intervals.of("2011-03-05/2011-03-06"),
              Intervals.of("2011-04-01/2011-04-05"),
              Intervals.of("2011-05-01/2011-05-07")
          ),
          JodaUtils.condenseIntervals(intervals)
      );
    }
  }

  @Test
  public void testMinMaxInterval()
  {
    Assert.assertEquals(Long.MAX_VALUE, Intervals.ETERNITY.toDuration().getMillis());
  }

  @Test
  public void testMinMaxDuration()
  {
    final Duration duration = Intervals.ETERNITY.toDuration();
    Assert.assertEquals(Long.MAX_VALUE, duration.getMillis());
    Assert.assertEquals("PT9223372036854775.807S", duration.toString());
  }

  // new Period(Long.MAX_VALUE) throws ArithmeticException
  @Test(expected = ArithmeticException.class)
  public void testMinMaxPeriod()
  {
    final Period period = Intervals.ETERNITY.toDuration().toPeriod();
    Assert.assertEquals(Long.MAX_VALUE, period.getMinutes());
  }

  @Test
  public void testShouldContainOverlappingIntervals()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-02-01/2011-03-10"),
        Intervals.of("2011-03-25/2011-04-03"),
        Intervals.of("2011-04-01/2015-01-19"),
        Intervals.of("2016-01-15/2016-01-19")
    );
    Assert.assertTrue(JodaUtils.containOverlappingIntervals(intervals));
  }


  @Test
  public void testShouldNotContainOverlappingIntervals()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-02-01/2011-03-10"),
        Intervals.of("2011-03-10/2011-04-03"),
        Intervals.of("2011-04-04/2015-01-14"),
        Intervals.of("2016-01-15/2016-01-19")
    );
    Assert.assertFalse(JodaUtils.containOverlappingIntervals(intervals));
  }

  @Test(expected = IAE.class)
  public void testOverlappingIntervalsContainsNull()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-02-01/2011-03-10"),
        null,
        Intervals.of("2011-04-04/2015-01-14"),
        Intervals.of("2016-01-15/2016-01-19")
    );
    JodaUtils.containOverlappingIntervals(intervals);
  }

  @Test(expected = IAE.class)
  public void testOverlappingIntervalsContainsUnsorted()
  {
    List<Interval> intervals = Arrays.asList(
        Intervals.of("2011-02-01/2011-03-10"),
        Intervals.of("2011-03-10/2011-04-03"),
        Intervals.of("2016-01-15/2016-01-19"),
        Intervals.of("2011-04-04/2015-01-14")
    );
    JodaUtils.containOverlappingIntervals(intervals);
  }


}
