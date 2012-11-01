/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexer.granularity;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Comparators;

public class UniformGranularitySpec implements GranularitySpec
{
  final private Granularity granularity;
  final private List<Interval> intervals;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("gran") Granularity granularity,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.granularity = granularity;
    this.intervals = intervals;
  }

  @Override
  public SortedSet<Interval> bucketIntervals()
  {
    final TreeSet<Interval> retVal = Sets.newTreeSet(Comparators.intervals());

    for (Interval interval : intervals) {
      for (Interval segmentInterval : granularity.getIterable(interval)) {
        retVal.add(segmentInterval);
      }
    }

    return retVal;
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return Optional.of(granularity.bucket(dt));
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public Iterable<Interval> getIntervals()
  {
    return intervals;
  }
}
