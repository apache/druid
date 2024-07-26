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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class IntervalDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final List<Interval> intervals;
  private final List<Pair<Long, Long>> intervalLongs;
  private final String dimension;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final OrDimFilter convertedFilter;

  @JsonCreator
  public IntervalDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("intervals") List<Interval> intervals,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    Preconditions.checkNotNull(intervals, "intervals can not be null");
    Preconditions.checkArgument(intervals.size() > 0, "must specify at least one interval");
    this.dimension = dimension;
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.intervalLongs = makeIntervalLongs();
    this.convertedFilter = new OrDimFilter(makeBoundDimFilters());
  }

  @VisibleForTesting
  public IntervalDimFilter(String dimension, List<Interval> intervals, @Nullable ExtractionFn extractionFn)
  {
    this(dimension, intervals, extractionFn, null);
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

  @Nullable
  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);

    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();
    int intervalsBytesSize = intervalLongs.size() * Long.BYTES * 2 + intervalLongs.size();

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
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
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
    return intervals.equals(that.intervals) &&
           dimension.equals(that.dimension) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(intervals, dimension, extractionFn, filterTuning);
  }

  @Override
  public String toString()
  {
    return convertedFilter.toString();
  }

  private List<Pair<Long, Long>> makeIntervalLongs()
  {
    List<Pair<Long, Long>> intervalLongs = new ArrayList<>();
    for (Interval interval : intervals) {
      intervalLongs.add(new Pair<>(interval.getStartMillis(), interval.getEndMillis()));
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
          StringComparators.NUMERIC,
          filterTuning
      );
      boundDimFilters.add(boundDimFilter);
    }
    return boundDimFilters;
  }
}
