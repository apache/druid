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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.filter.ColumnComparisonFilter;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 */
public class ColumnComparisonDimFilter implements DimFilter
{
  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  private final List<DimensionSpec> dimensions;
  private final FilterTuning filterTuning;

  @JsonCreator
  public ColumnComparisonDimFilter(
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("filterTuning") FilterTuning filterTuning
  )
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions");
    Preconditions.checkArgument(dimensions.size() >= 2, "dimensions must have a least 2 dimensions");
    this.filterTuning = filterTuning;
  }

  @VisibleForTesting
  public ColumnComparisonDimFilter(
      List<DimensionSpec> dimensions
  )
  {
    this(dimensions, null);
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(DimFilterUtils.COLUMN_COMPARISON_CACHE_ID)
        // Since a = b is the same as b = a we can ignore the order here.
        .appendCacheablesIgnoringOrder(dimensions)
        .build();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public Filter toFilter()
  {
    return new ColumnComparisonFilter(dimensions, filterTuning);
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public String toString()
  {
    return "ColumnComparisonDimFilter{" +
           "dimensions=[" + COMMA_JOINER.join(dimensions) + "]" +
           ", filterTuning=" + filterTuning +
           "}";
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
    ColumnComparisonDimFilter that = (ColumnComparisonDimFilter) o;
    return dimensions.equals(that.dimensions) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimensions, filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public HashSet<String> getRequiredColumns()
  {
    return Sets.newHashSet(dimensions.stream()
        .map(DimensionSpec::getDimension)
        .collect(Collectors.toSet())
    );
  }
}
