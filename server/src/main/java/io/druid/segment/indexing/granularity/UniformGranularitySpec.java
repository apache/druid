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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.Granularity;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.SortedSet;

public class UniformGranularitySpec implements GranularitySpec
{
  private static final Granularity DEFAULT_SEGMENT_GRANULARITY = Granularity.DAY;
  private static final QueryGranularity DEFAULT_QUERY_GRANULARITY = QueryGranularities.NONE;

  private final Granularity segmentGranularity;
  private final QueryGranularity queryGranularity;
  private final Boolean rollup;
  private final List<Interval> inputIntervals;
  private final ArbitraryGranularitySpec wrappedSpec;
  private final String timezone;

  @JsonCreator
  public UniformGranularitySpec(
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("queryGranularity") QueryGranularity queryGranularity,
      @JsonProperty("rollup") Boolean rollup,
      @JsonProperty("intervals") List<Interval> inputIntervals,
      @JsonProperty("timezone") String timezone

  )
  {
    this.segmentGranularity = segmentGranularity == null ? DEFAULT_SEGMENT_GRANULARITY : segmentGranularity;
    this.queryGranularity = queryGranularity == null ? DEFAULT_QUERY_GRANULARITY : queryGranularity;
    this.rollup = rollup == null ? Boolean.TRUE : rollup;
    this.timezone = timezone;
    final DateTimeZone timeZone = DateTimeZone.forID(this.timezone);

    if (inputIntervals != null) {
      List<Interval> granularIntervals = Lists.newArrayList();
      for (Interval inputInterval : inputIntervals) {
        if (this.timezone != null) {
          inputInterval = new Interval(inputInterval.getStartMillis(), inputInterval.getEndMillis(), timeZone);
        }

        Iterables.addAll(granularIntervals, this.segmentGranularity.getIterable(inputInterval));
      }
      this.inputIntervals = ImmutableList.copyOf(inputIntervals);
      this.wrappedSpec = new ArbitraryGranularitySpec(queryGranularity, rollup, granularIntervals, timezone);
    } else {
      this.inputIntervals = null;
      this.wrappedSpec = null;
    }
  }

  public UniformGranularitySpec(
      Granularity segmentGranularity,
      QueryGranularity queryGranularity,
      List<Interval> inputIntervals
  )
  {
    this(segmentGranularity, queryGranularity, true, inputIntervals, null);
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
  @JsonProperty("rollup")
  public boolean isRollup()
  {
    return rollup;
  }

  @Override
  @JsonProperty("queryGranularity")
  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty("intervals")
  public Optional<List<Interval>> getIntervals()
  {
    return Optional.fromNullable(inputIntervals);
  }

  @Override
  @JsonProperty("timezone")
  public String getTimezone()
  {
    return timezone;
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

    UniformGranularitySpec that = (UniformGranularitySpec) o;

    if (segmentGranularity != that.segmentGranularity) {
      return false;
    }
    if (!queryGranularity.equals(that.queryGranularity)) {
      return false;
    }
    if (!rollup.equals(that.rollup)) {
      return false;
    }
    if (timezone != null ? !timezone.equals(that.timezone): that.timezone != null) {
      return false;
    }
    if (inputIntervals != null ? !inputIntervals.equals(that.inputIntervals) : that.inputIntervals != null) {
      return false;
    }
    return !(wrappedSpec != null ? !wrappedSpec.equals(that.wrappedSpec) : that.wrappedSpec != null);

  }

  @Override
  public int hashCode()
  {
    int result = segmentGranularity.hashCode();
    result = 31 * result + queryGranularity.hashCode();
    result = 31 * result + rollup.hashCode();
    result = 31 * result + (timezone != null ? timezone.hashCode() : 0);
    result = 31 * result + (inputIntervals != null ? inputIntervals.hashCode() : 0);
    result = 31 * result + (wrappedSpec != null ? wrappedSpec.hashCode() : 0);
    return result;
  }
}
