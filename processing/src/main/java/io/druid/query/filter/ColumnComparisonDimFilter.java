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
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.filter.ColumnComparisonFilter;

import java.util.List;

/**
 */
public class ColumnComparisonDimFilter implements DimFilter
{
  private static final Joiner COMMA_JOINER = Joiner.on(", ");

  private final List<DimensionSpec> dimensions;

  @JsonCreator
  public ColumnComparisonDimFilter(
      @JsonProperty("dimensions") List<DimensionSpec> dimensions
  )
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "dimensions");
    Preconditions.checkArgument(dimensions.size() >= 2, "dimensions must have a least 2 dimensions");
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
    return new ColumnComparisonFilter(dimensions);
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @Override
  public String toString()
  {
    return "ColumnComparisonDimFilter{" +
           "dimensions=[" + COMMA_JOINER.join(dimensions) + "]" +
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

    return dimensions.equals(that.dimensions);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public int hashCode()
  {
    return 31 * dimensions.hashCode();
  }
}
