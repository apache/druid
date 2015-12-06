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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;
import io.druid.common.utils.JodaUtils;
import io.druid.granularity.QueryGranularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

public class ArbitraryGranularitySpec implements GranularitySpec
{
  private final TreeSet<Interval> intervals;
  private final QueryGranularity queryGranularity;

  @JsonCreator
  public ArbitraryGranularitySpec(
      @JsonProperty("queryGranularity") QueryGranularity queryGranularity,
      @JsonProperty("intervals") List<Interval> inputIntervals
  )
  {
    this.queryGranularity = queryGranularity;
    this.intervals = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());

    if (inputIntervals == null) {
      inputIntervals = Lists.newArrayList();
    }

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
    final Interval interval = intervals.floor(new Interval(dt, new DateTime(JodaUtils.MAX_INSTANT)));

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
  @JsonProperty("queryGranularity")
  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ArbitraryGranularitySpec that = (ArbitraryGranularitySpec) o;

    if (!intervals.equals(that.intervals)) {
      return false;
    }
    return !(queryGranularity != null
             ? !queryGranularity.equals(that.queryGranularity)
             : that.queryGranularity != null);

  }

  @Override
  public int hashCode()
  {
    int result = intervals.hashCode();
    result = 31 * result + (queryGranularity != null ? queryGranularity.hashCode() : 0);
    return result;
  }
}
