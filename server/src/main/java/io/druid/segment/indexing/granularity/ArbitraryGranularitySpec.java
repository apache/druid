/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;
import io.druid.granularity.QueryGranularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class ArbitraryGranularitySpec implements GranularitySpec
{
  private final TreeSet<Interval> intervals;

  @JsonCreator
  public ArbitraryGranularitySpec(
      @JsonProperty("intervals") List<Interval> inputIntervals
  )
  {
    intervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());

    // Insert all intervals
    for (final Interval inputInterval : inputIntervals) {
      intervals.add(inputInterval);
    }

    // Ensure intervals are non-overlapping (but they may abut each other)
    final PeekingIterator<Interval> intervalIterator = Iterators.peekingIterator(intervals.iterator());
    while (intervalIterator.hasNext()) {
      final Interval currentInterval = intervalIterator.next();

      if (intervalIterator.hasNext()) {
        final Interval nextInterval = intervalIterator.peek();
        if (currentInterval.overlaps(nextInterval)) {
          throw new IllegalArgumentException(
              String.format(
                  "Overlapping intervals: %s, %s",
                  currentInterval,
                  nextInterval
              )
          );
        }
      }
    }
  }

  @Override
  @JsonProperty("intervals")
  public Optional<SortedSet<Interval>> bucketIntervals()
  {
    return Optional.of((SortedSet<Interval>) intervals);
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    // First interval with start time ≤ dt
    final Interval interval = intervals.floor(new Interval(dt, new DateTime(Long.MAX_VALUE)));

    if (interval != null && interval.contains(dt)) {
      return Optional.of(interval);
    } else {
      return Optional.absent();
    }
  }

  @Override
  public Granularity getSegmentGranularity()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryGranularity getQueryGranularity()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public GranularitySpec withQueryGranularity(QueryGranularity queryGranularity)
  {
    throw new UnsupportedOperationException();
  }
}
