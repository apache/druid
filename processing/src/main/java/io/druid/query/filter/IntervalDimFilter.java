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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import com.google.common.primitives.Longs;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.ordering.StringComparators;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IntervalDimFilter implements DimFilter
{
  private final List<Interval> intervals;
  private final List<Pair<Long, Long>> intervalLongs;
  private final String dimension;
  private final ExtractionFn extractionFn;
  private final OrDimFilter convertedFilter;

  @JsonCreator
  public IntervalDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("intervals") List<Interval> intervals,
      @JsonProperty("extractionFn") ExtractionFn extractionFn
  )
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkNotNull(intervals, "intervals can not be null");
    Preconditions.checkArgument(intervals.size() > 0, "must specify at least one interval");
    this.dimension = dimension;
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
    this.extractionFn = extractionFn;
    this.intervalLongs = makeIntervalLongs();
    this.convertedFilter = new OrDimFilter(makeBoundDimFilters());
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);

    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();
    int intervalsBytesSize = intervalLongs.size() * Longs.BYTES * 2 + intervalLongs.size();

    ByteBuffer filterCacheKey = ByteBuffer.allocate(3
                                                    + dimensionBytes.length
                                                    + intervalsBytesSize
                                                    + extractionFnBytes.length)
                                          .put(DimFilterUtils.INTERVAL_CACHE_ID)
                                          .put(dimensionBytes)
                                          .put(DimFilterUtils.STRING_SEPARATOR)
                                          .put(extractionFnBytes)
                                          .put(DimFilterUtils.STRING_SEPARATOR);
    for (Pair<Long, Long> interval : intervalLongs) {
      filterCacheKey.put(Longs.toByteArray(interval.lhs))
                    .put(Longs.toByteArray(interval.rhs))
                    .put((byte) 0xFF);
    }
    return filterCacheKey.array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return convertedFilter.toFilter();
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return convertedFilter.getDimensionRangeSet(dimension);
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

    IntervalDimFilter that = (IntervalDimFilter) o;

    if (!getIntervals().equals(that.getIntervals())) {
      return false;
    }
    if (!getDimension().equals(that.getDimension())) {
      return false;
    }
    return getExtractionFn() != null
           ? getExtractionFn().equals(that.getExtractionFn())
           : that.getExtractionFn() == null;

  }

  @Override
  public int hashCode()
  {
    int result = getIntervals().hashCode();
    result = 31 * result + getDimension().hashCode();
    result = 31 * result + (getExtractionFn() != null ? getExtractionFn().hashCode() : 0);
    return result;
  }

  private List<Pair<Long, Long>> makeIntervalLongs()
  {
    List<Pair<Long, Long>> intervalLongs = new ArrayList<>();
    for (Interval interval : intervals) {
      intervalLongs.add(new Pair<Long, Long>(interval.getStartMillis(), interval.getEndMillis()));
    }
    return intervalLongs;
  }

  private List<DimFilter> makeBoundDimFilters()
  {
    List<DimFilter> boundDimFilters = new ArrayList<>();
    for (Pair<Long, Long> interval : intervalLongs) {
      BoundDimFilter boundDimFilter = new BoundDimFilter(
          dimension,
          String.valueOf(interval.lhs),
          String.valueOf(interval.rhs),
          false,
          true,
          null,
          extractionFn,
          StringComparators.NUMERIC
      );
      boundDimFilters.add(boundDimFilter);
    }
    return boundDimFilters;
  }
}
