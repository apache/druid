/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.segment.indexing.granularity;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.IntervalsByGranularity;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class IntervalsByGranularityTest
{
  private static final long SECONDS_IN_YEAR = 31536000;


  @Test
  public void testTrivialIntervalExplosion()
  {
    Interval first = Intervals.of("2013-01-01T00Z/2013-02-01T00Z");
    Interval second = Intervals.of("2012-01-01T00Z/2012-02-01T00Z");
    Interval third = Intervals.of("2002-01-01T00Z/2003-01-01T00Z");

    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first, second, third),
        Granularity.fromString("DAY")
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = getCount(granularityIntervals);
    Assert.assertTrue(count == 62 + 365);

    granularityIntervals = intervals.granularityIntervalsIterator();
    count = getCountWithNoHasNext(granularityIntervals);
    Assert.assertTrue(count == 62 + 365);
  }


  @Test
  public void testDups()
  {
    Interval first = Intervals.of("2013-01-01T00Z/2013-02-01T00Z");
    Interval second = Intervals.of("2012-04-01T00Z/2012-05-01T00Z");
    Interval third = Intervals.of("2013-01-01T00Z/2013-02-01T00Z"); // dup

    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first, second, third),
        Granularity.fromString("DAY")
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = getCount(granularityIntervals);
    Assert.assertTrue(count == 61);
  }

  @Test
  public void testIntervalExplosion()
  {
    Interval first = Intervals.of("2012-01-01T00Z/2012-12-31T00Z");
    Interval second = Intervals.of("2002-01-01T00Z/2002-12-31T00Z");
    Interval third = Intervals.of("2021-01-01T00Z/2021-06-30T00Z");
    IntervalsByGranularity intervals = new IntervalsByGranularity(
        ImmutableList.of(first, second, third),
        Granularity.fromString("SECOND")
    );

    // get count:
    Iterator<Interval> granularityIntervals = intervals.granularityIntervalsIterator();
    long count = getCount(granularityIntervals);
    Assert.assertTrue(count == 78537600);


  }
  
  private long getCount(Iterator<Interval> granularityIntervalIterator)
  {
    long count = 0;
    Interval previous = null;
    Interval current;
    while (granularityIntervalIterator.hasNext()) {
      current = granularityIntervalIterator.next();
      if (previous != null) {
        Assert.assertTrue(previous.getEndMillis() <= current.getStartMillis());
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
