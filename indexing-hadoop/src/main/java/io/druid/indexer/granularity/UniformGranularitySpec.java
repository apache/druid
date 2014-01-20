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

package io.druid.indexer.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;

public class UniformGranularitySpec implements GranularitySpec
{
  final private Granularity granularity;
  final private List<Interval> inputIntervals;
  final private ArbitraryGranularitySpec wrappedSpec;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("gran") Granularity granularity,
      @JsonProperty("intervals") List<Interval> inputIntervals
  )
  {
    List<Interval> granularIntervals = Lists.newArrayList();

    for (Interval inputInterval : inputIntervals) {
      Iterables.addAll(granularIntervals,  granularity.getIterable(inputInterval));
    }

    this.granularity = granularity;
    this.inputIntervals = ImmutableList.copyOf(inputIntervals);
    this.wrappedSpec = new ArbitraryGranularitySpec(granularIntervals);
  }

  @Override
  public SortedSet<Interval> bucketIntervals()
  {
    return wrappedSpec.bucketIntervals();
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return wrappedSpec.bucketInterval(dt);
  }

  @Override
  @JsonProperty("gran")
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty("intervals")
  public Iterable<Interval> getIntervals()
  {
    return inputIntervals;
  }
}
