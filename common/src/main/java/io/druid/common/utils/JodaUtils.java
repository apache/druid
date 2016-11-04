/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.common.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.druid.java.util.common.guava.Comparators;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

/**
 */
public class JodaUtils
{
  // limit intervals such that duration millis fits in a long
  public static final long MAX_INSTANT = Long.MAX_VALUE / 2;
  public static final long MIN_INSTANT = Long.MIN_VALUE / 2;
  public static final Interval ETERNITY = new Interval(MIN_INSTANT, MAX_INSTANT);

  public static ArrayList<Interval> condenseIntervals(Iterable<Interval> intervals)
  {
    ArrayList<Interval> retVal = Lists.newArrayList();

    TreeSet<Interval> sortedIntervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());
    for (Interval interval : intervals) {
      sortedIntervals.add(interval);
    }

    if (sortedIntervals.isEmpty()) {
      return Lists.newArrayList();
    }

    Iterator<Interval> intervalsIter = sortedIntervals.iterator();
    Interval currInterval = intervalsIter.next();
    while (intervalsIter.hasNext()) {
      Interval next = intervalsIter.next();

      if (currInterval.overlaps(next) || currInterval.abuts(next)) {
        currInterval = new Interval(currInterval.getStart(), next.getEnd());
      } else {
        retVal.add(currInterval);
        currInterval = next;
      }
    }
    retVal.add(currInterval);

    return retVal;
  }

  public static Interval umbrellaInterval(Iterable<Interval> intervals)
  {
    ArrayList<DateTime> startDates = Lists.newArrayList();
    ArrayList<DateTime> endDates = Lists.newArrayList();

    for (Interval interval : intervals) {
      startDates.add(interval.getStart());
      endDates.add(interval.getEnd());
    }

    DateTime minStart = minDateTime(startDates.toArray(new DateTime[]{}));
    DateTime maxEnd = maxDateTime(endDates.toArray(new DateTime[]{}));

    if (minStart == null || maxEnd == null) {
      throw new IllegalArgumentException("Empty list of intervals");
    }
    return new Interval(minStart, maxEnd);
  }

  public static boolean overlaps(final Interval i, Iterable<Interval> intervals)
  {
    return Iterables.any(
        intervals, new Predicate<Interval>()
    {
      @Override
      public boolean apply(Interval input)
      {
        return input.overlaps(i);
      }
    }
    );

  }

  public static DateTime minDateTime(DateTime... times)
  {
    if (times == null) {
      return null;
    }

    switch (times.length) {
      case 0:
        return null;
      case 1:
        return times[0];
      default:
        DateTime min = times[0];
        for (int i = 1; i < times.length; ++i) {
          min = min.isBefore(times[i]) ? min : times[i];
        }
        return min;
    }
  }

  public static DateTime maxDateTime(DateTime... times)
  {
    if (times == null) {
      return null;
    }

    switch (times.length) {
      case 0:
        return null;
      case 1:
        return times[0];
      default:
        DateTime max = times[0];
        for (int i = 1; i < times.length; ++i) {
          max = max.isAfter(times[i]) ? max : times[i];
        }
        return max;
    }
  }
}
