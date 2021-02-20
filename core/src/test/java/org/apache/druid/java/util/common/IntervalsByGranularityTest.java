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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class IntervalsByGranularityTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testTrivialIntervalExplosion()
  {
    Interval first = Intervals.of("2013-01-01T00Z/2013-02-01T00Z");
    Interval second = Intervals.of("2012-01-01T00Z/2012-02-01T00Z");
    Interval third = Intervals.of("2002-01-01T00Z/2003-01-01T00Z");

    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first, second, third),
        Granularities.DAY
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = verifyIteratorAndReturnIntervalCount(granularityIntervals);
    Assert.assertEquals(62 + 365, count);

    granularityIntervals = intervals.granularityIntervalsIterator();
    count = getCountWithNoHasNext(granularityIntervals);
    Assert.assertEquals(62 + 365, count);
  }


  @Test
  public void testDups()
  {
    Interval first = Intervals.of("2013-01-01T00Z/2013-02-01T00Z");
    Interval second = Intervals.of("2012-04-01T00Z/2012-05-01T00Z");
    Interval third = Intervals.of("2013-01-01T00Z/2013-02-01T00Z"); // dup

    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first, second, third),
        Granularities.DAY
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = verifyIteratorAndReturnIntervalCount(granularityIntervals);
    Assert.assertEquals(61, count);
  }


  @Test
  public void testCondenseForManyIntervals()
  {
    // This method attempts to test that there are no issues when condensed is called
    // with an iterator pointing to millions of intervals (since the version of condensed
    // used here takes an interval iterator and does not materialize intervals)
    Interval first = Intervals.of("2012-01-01T00Z/P1Y");
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first),
        Granularities.SECOND
    );
    Assert.assertEquals(
        ImmutableList.of(Intervals.of("2012-01-01T00Z/2013-01-01T00Z")),
        ImmutableList.copyOf(JodaUtils.condensedIntervalsIterator(intervals.granularityIntervalsIterator()))
    );
  }

  /**
   * This test iterates huge intervals (2.5 years) with the SECOND granularity.
   * The motivation behind this test is ensuring that IntervalsByGranularity can handle
   * these huge intervals with a tiny granularity. However, this test takes a long time
   * to populate all intervals based on the SECOND granularity (more than 1 min), so
   * is ignored by default. We should make this test not a unit test, but a load test.
   */
  @Ignore
  @Test
  public void testIterateHugeIntervalsWithTinyGranularity()
  {
    Interval first = Intervals.of("2012-01-01T00Z/2012-12-31T00Z");
    Interval second = Intervals.of("2002-01-01T00Z/2002-12-31T00Z");
    Interval third = Intervals.of("2021-01-01T00Z/2021-06-30T00Z");
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first, second, third),
        Granularities.SECOND
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = verifyIteratorAndReturnIntervalCount(granularityIntervals);
    Assert.assertEquals(78537600, count);
  }

  @Test
  public void testSimpleEliminateRepeated()
  {
    final List<Interval> inputIntervals = ImmutableList.of(
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z")
    );
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        inputIntervals,
        Granularities.MONTH
    );

    Assert.assertEquals(
        ImmutableList.of(Intervals.of("2012-01-01T00Z/2012-02-01T00Z")),
        ImmutableList.copyOf(intervals.granularityIntervalsIterator())
    );
  }

  @Test
  public void testALittleMoreComplexEliminateRepeated()
  {
    final List<Interval> inputIntervals = ImmutableList.of(
        Intervals.of("2015-01-08T00Z/2015-01-11T00Z"),
        Intervals.of("2012-01-08T00Z/2012-01-11T00Z"),
        Intervals.of("2012-01-07T00Z/2012-01-08T00Z"),
        Intervals.of("2012-01-03T00Z/2012-01-04T00Z"),
        Intervals.of("2012-01-01T00Z/2012-01-03T00Z"),
        Intervals.of("2007-03-08T00Z/2007-04-11T00Z")
    );
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        inputIntervals,
        Granularities.MONTH
    );

    Assert.assertEquals(
        ImmutableList.of(
            Intervals.of("2007-03-01T00Z/2007-04-01T00Z"),
            Intervals.of("2007-04-01T00Z/2007-05-01T00Z"),
            Intervals.of("2012-01-01T00Z/2012-02-01T00Z"),
            Intervals.of("2015-01-01T00Z/2015-02-01T00Z")
        ),
        ImmutableList.copyOf(intervals.granularityIntervalsIterator())
    );
  }

  @Test
  public void testOverlappingShouldThrow()
  {
    List<Interval> inputIntervals = ImmutableList.of(
        Intervals.of("2013-01-01T00Z/2013-01-11T00Z"),
        Intervals.of("2013-01-05T00Z/2013-01-08T00Z"),
        Intervals.of("2013-01-07T00Z/2013-01-15T00Z")
    );

    IntervalsByGranularity intervals = new IntervalsByGranularity(
        inputIntervals,
        Granularities.DAY
    );

    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = verifyIteratorAndReturnIntervalCount(granularityIntervals);
    Assert.assertEquals(14, count);
  }

  @Test
  public void testWithGranularity()
  {
    List<Interval> inputIntervals = ImmutableList.of(
        Intervals.of("2013-01-01T00Z/2013-01-10T00Z"),
        Intervals.of("2013-01-15T00Z/2013-01-20T00Z"),
        Intervals.of("2013-02-07T00Z/2013-02-15T00Z")
    );

    IntervalsByGranularity intervals = new IntervalsByGranularity(
        inputIntervals,
        Granularities.MONTH
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = verifyIteratorAndReturnIntervalCount(granularityIntervals);
    Assert.assertEquals(2, count);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRemoveThrowsException()
  {
    final List<Interval> inputIntervals = ImmutableList.of(
        Intervals.of("2015-01-08T00Z/2015-01-11T00Z")
    );
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        inputIntervals,
        Granularities.MONTH
    );
    intervals.granularityIntervalsIterator().remove();
  }

  @Test
  public void testEmptyInput()
  {
    final List<Interval> inputIntervals = Collections.emptyList();
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        inputIntervals,
        Granularities.MONTH
    );
    Assert.assertFalse(intervals.granularityIntervalsIterator().hasNext());
  }

  private long verifyIteratorAndReturnIntervalCount(Iterator<Interval> granularityIntervalIterator)
  {
    long count = 0;
    Interval previous = null;
    Interval current;
    while (granularityIntervalIterator.hasNext()) {
      current = granularityIntervalIterator.next();
      if (previous != null) {
        Assert.assertTrue(previous + "," + current, previous.getEndMillis() <= current.getStartMillis());
      }
      previous = current;
      count++;
    }
    return count;
  }

  private long getCountWithNoHasNext(Iterator<Interval> granularityIntervalIterator)
  {
    long count = 0;
    Interval previous = null;
    Interval current;

    while (true) {
      try {
        current = granularityIntervalIterator.next();
      }
      catch (NoSuchElementException e) {
        // done
        break;
      }
      if (previous != null) {
        Assert.assertTrue(previous.getEndMillis() <= current.getStartMillis());
      }
      previous = current;
      count++;
    }

    return count;
  }

}
