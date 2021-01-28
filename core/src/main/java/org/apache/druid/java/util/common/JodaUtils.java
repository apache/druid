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
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 */
public class JodaUtils
{
  // limit intervals such that duration millis fits in a long
  public static final long MAX_INSTANT = Long.MAX_VALUE / 2;
  public static final long MIN_INSTANT = Long.MIN_VALUE / 2;

  /**
   * This method will not materialize the input intervals if they represent
   * a SortedSet (i.e. implement that interface). If not, the method internally
   * creates a sorted set and populates it with them thus materializing the
   * intervals in the input.
   *
   * @param intervals The Iterable object containing the intervals to condense
   * @return The condensed intervals
   */
  public static List<Interval> condenseIntervals(Iterable<Interval> intervals)
  {
    final SortedSet<Interval> sortedIntervals;

    if (intervals instanceof SortedSet) {
      sortedIntervals = (SortedSet<Interval>) intervals;
    } else {
      sortedIntervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
      for (Interval interval : intervals) {
        sortedIntervals.add(interval);
      }
    }
    return ImmutableList.copyOf(condensedIntervalsIterator(sortedIntervals.iterator()));
  }

  /**
   * This method does not materialize the intervals represented by the
   * sortedIntervals iterator. However, caller needs to insure that sortedIntervals
   * is already sorted in ascending order (use the Comparators.intervalsByStartThenEnd()).
   * It avoids materialization by incrementally condensing the intervals by
   * starting from the first and looking for "adjacent" intervals. This is
   * possible since intervals in the Iterator are in ascending order (as
   * guaranteed by the caller).
   * <p>
   * *
   *
   * @param sortedIntervals The iterator object containing the intervals to condense
   * @return An iterator for the condensed intervals. By construction the condensed intervals are sorted
   * in ascending order and contain no repeated elements. The iterator can contain nulls,
   * they will be skipped if it does.
   * @throws IAE if an element is null or if sortedIntervals is not sorted in ascending order
   */
  public static Iterator<Interval> condensedIntervalsIterator(Iterator<Interval> sortedIntervals)
  {

    if (sortedIntervals == null || !sortedIntervals.hasNext()) {
      return Collections.emptyIterator();
    }

    final PeekingIterator<Interval> peekingIterator = Iterators.peekingIterator(sortedIntervals);
    return new Iterator<Interval>()
    {
      private Interval previous;

      @Override
      public boolean hasNext()
      {
        return peekingIterator.hasNext();
      }

      @Override
      public Interval next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        Interval currInterval = peekingIterator.next();
        if (currInterval == null) {
          throw new IAE("Element of intervals is null");
        }

        // check sorted ascending:
        verifyAscendingSortOrder(previous, currInterval);

        previous = currInterval;

        while (hasNext()) {
          Interval next = peekingIterator.peek();
          if (next == null) {
            throw new IAE("Element of intervals is null");
          }

          if (currInterval.abuts(next)) {
            currInterval = new Interval(currInterval.getStart(), next.getEnd());
            peekingIterator.next();
          } else if (currInterval.overlaps(next)) {
            DateTime nextEnd = next.getEnd();
            DateTime currEnd = currInterval.getEnd();
            currInterval = new Interval(
                currInterval.getStart(),
                nextEnd.isAfter(currEnd) ? nextEnd : currEnd
            );
            peekingIterator.next();
          } else {
            break;
          }
        }
        return currInterval;
      }
    };
  }

  /**
   * Verify whether an iterable of intervals contains overlapping intervals
   *
   * @param intervals An interval iterable sorted using Comparators.intervalsByStartThenEnd()
   * @return true if the iterable contains at least two overlapping intervals, false otherwise.
   * @throws IAE when at least an element is null or when the iterable is not sorted
   */
  public static boolean containOverlappingIntervals(Iterable<Interval> intervals)
  {
    if (intervals == null) {
      return false;
    }
    boolean retVal = false;
    Interval previous = null;
    for (Interval current : intervals) {
      if (current == null) {
        throw new IAE("Intervals should not contain nulls");
      }
      verifyAscendingSortOrder(previous, current);
      if (previous != null && previous.overlaps(current)) {
        retVal = true;
        break;
      }
      previous = current;
    }
    return retVal;
  }

  private static void verifyAscendingSortOrder(Interval previous, Interval current)
  {
    if (previous != null && previous.isAfter(current)) {
      throw new IAE("Adjacent intervals are not sorted [%s,%s]", previous, current);
    }
  }

  public static Interval umbrellaInterval(Iterable<Interval> intervals)
  {
    ArrayList<DateTime> startDates = new ArrayList<>();
    ArrayList<DateTime> endDates = new ArrayList<>();

    for (Interval interval : intervals) {
      startDates.add(interval.getStart());
      endDates.add(interval.getEnd());
    }

    DateTime minStart = minDateTime(startDates.toArray(new DateTime[0]));
    DateTime maxEnd = maxDateTime(endDates.toArray(new DateTime[0]));

    if (minStart == null || maxEnd == null) {
      throw new IllegalArgumentException("Empty list of intervals");
    }
    return new Interval(minStart, maxEnd);
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
