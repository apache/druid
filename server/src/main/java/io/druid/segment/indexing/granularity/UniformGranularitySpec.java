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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.granularity.QueryGranularity;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.SortedSet;

public class UniformGranularitySpec implements GranularitySpec
{
  private static final Granularity defaultSegmentGranularity = Granularity.DAY;
  private static final QueryGranularity defaultQueryGranularity = QueryGranularity.NONE;

  private final Granularity segmentGranularity;
  private final QueryGranularity queryGranularity;
  private final List<Interval> inputIntervals;
  private final ArbitraryGranularitySpec wrappedSpec;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") QueryGranularity queryGranularity,
      @JsonProperty("intervals") List<Interval> inputIntervals,
      // Backwards compatible
      @JsonProperty("gran") Granularity granularity

  )
  {
    if (segmentGranularity != null) {
      this.segmentGranularity = segmentGranularity;
    } else if (granularity != null) { // backwards compatibility
      this.segmentGranularity = granularity;
    } else {
      this.segmentGranularity = defaultSegmentGranularity;
    }
    this.queryGranularity = queryGranularity == null ? defaultQueryGranularity : queryGranularity;

    if (inputIntervals != null) {
      List<Interval> granularIntervals = Lists.newArrayList();
      for (Interval inputInterval : inputIntervals) {
        Iterables.addAll(granularIntervals, this.segmentGranularity.getIterable(inputInterval));
      }
      this.inputIntervals = ImmutableList.copyOf(inputIntervals);
      this.wrappedSpec = new ArbitraryGranularitySpec(granularIntervals);
    } else {
      this.inputIntervals = null;
      this.wrappedSpec = null;
    }
  }

  @Override
  public Optional<SortedSet<Interval>> bucketIntervals()
  {
    if (wrappedSpec == null) {
      return Optional.absent();
    } else {
      return wrappedSpec.bucketIntervals();
    }
  }

  @Override
  public Optional<Interval> bucketInterval(DateTime dt)
  {
    return wrappedSpec.bucketInterval(dt);
  }

  @Override
  @JsonProperty("segmentGranularity")
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Override
  @JsonProperty("queryGranularity")
  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @Override
  public GranularitySpec withQueryGranularity(QueryGranularity queryGranularity)
  {
    return new UniformGranularitySpec(
        segmentGranularity,
        queryGranularity,
        inputIntervals,
        segmentGranularity
    );
  }

  @JsonProperty("intervals")
  public Optional<List<Interval>> getIntervals()
  {
    return Optional.fromNullable(inputIntervals);
  }
}
