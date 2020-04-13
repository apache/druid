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

package org.apache.druid.sql.calcite.filtration;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

public class RangeSets
{
  public static <T extends Comparable<T>> RangeSet<T> of(final Range<T> range)
  {
    return unionRanges(ImmutableList.of(range));
  }

  /**
   * Unions a set of ranges, or returns null if the set is empty.
   */
  public static <T extends Comparable<T>> RangeSet<T> unionRanges(final Iterable<Range<T>> ranges)
  {
    RangeSet<T> rangeSet = null;
    for (Range<T> range : ranges) {
      if (rangeSet == null) {
        rangeSet = TreeRangeSet.create();
      }
      rangeSet.add(range);
    }
    return rangeSet;
  }

  /**
   * Unions a set of rangeSets, or returns null if the set is empty.
   */
  public static <T extends Comparable<T>> RangeSet<T> unionRangeSets(final Iterable<RangeSet<T>> rangeSets)
  {
    final RangeSet<T> rangeSet = TreeRangeSet.create();
    for (RangeSet<T> set : rangeSets) {
      rangeSet.addAll(set);
    }
    return rangeSet;
  }

  /**
   * Intersects a set of ranges, or returns null if the set is empty.
   */
  public static <T extends Comparable<T>> RangeSet<T> intersectRanges(final Iterable<Range<T>> ranges)
  {
    RangeSet<T> rangeSet = null;
    for (final Range<T> range : ranges) {
      if (rangeSet == null) {
        rangeSet = TreeRangeSet.create();
        rangeSet.add(range);
      } else {
        rangeSet = TreeRangeSet.create(rangeSet.subRangeSet(range));
      }
    }
    return rangeSet;
  }

  /**
   * Intersects a set of rangeSets, or returns null if the set is empty.
   */
  public static <T extends Comparable<T>> RangeSet<T> intersectRangeSets(final Iterable<RangeSet<T>> rangeSets)
  {
    RangeSet<T> rangeSet = null;
    for (final RangeSet<T> set : rangeSets) {
      if (rangeSet == null) {
        rangeSet = TreeRangeSet.create();
        rangeSet.addAll(set);
      } else {
        rangeSet.removeAll(set.complement());
      }
    }
    return rangeSet;
  }

  public static RangeSet<Long> fromIntervals(final Iterable<Interval> intervals)
  {
    final RangeSet<Long> retVal = TreeRangeSet.create();
    for (Interval interval : intervals) {
      retVal.add(Range.closedOpen(interval.getStartMillis(), interval.getEndMillis()));
    }
    return retVal;
  }

  public static List<Interval> toIntervals(final RangeSet<Long> rangeSet)
  {
    final List<Interval> retVal = new ArrayList<>();

    for (Range<Long> range : rangeSet.asRanges()) {
      final long start;
      final long end;

      if (range.hasLowerBound()) {
        final long millis = range.lowerEndpoint();
        start = millis + (range.lowerBoundType() == BoundType.OPEN ? 1 : 0);
      } else {
        start = Filtration.eternity().getStartMillis();
      }

      if (range.hasUpperBound()) {
        final long millis = range.upperEndpoint();
        end = millis + (range.upperBoundType() == BoundType.OPEN ? 0 : 1);
      } else {
        end = Filtration.eternity().getEndMillis();
      }

      retVal.add(Intervals.utc(start, end));
    }

    return retVal;
  }
}
