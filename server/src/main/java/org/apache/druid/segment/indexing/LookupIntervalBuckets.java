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

package org.apache.druid.segment.indexing;

import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Comparators;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.TreeSet;

public class LookupIntervalBuckets
{
  private final Iterable<Interval> intervalIterable;
  private final TreeSet<Interval> intervals;

  public LookupIntervalBuckets(Iterable<Interval> intervalIterable)
  {
    this.intervalIterable = intervalIterable;
    // The tree set will be materialized on demand (see below) to avoid client code
    // blowing up when constructing this data structure and when the
    // number of intervals is very large...
    this.intervals = new TreeSet<>(Comparators.intervalsByStartThenEnd());
  }

  public Optional<Interval> bucketInterval(DateTime dt)
  {
    final Interval interval = materializedIntervals().floor(new Interval(dt, DateTimes.MAX));
    if (interval != null && interval.contains(dt)) {
      return Optional.of(interval);
    } else {
      return Optional.absent();
    }
  }

  public Iterator<Interval> iterator()
  {
    return materializedIntervals().iterator();
  }

  private TreeSet<Interval> materializedIntervals()
  {
    if (intervalIterable != null && intervalIterable.iterator().hasNext() && intervals.isEmpty()) {
      Iterators.addAll(intervals, intervalIterable.iterator());
    }
    return intervals;
  }
}
