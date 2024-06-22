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
import org.apache.druid.collections.spatial.search.Bound;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.filter.SpatialFilter;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

/**
 */
public class SpatialDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final String dimension;
  private final Bound bound;
  @Nullable
  private final FilterTuning filterTuning;

  @JsonCreator
  public SpatialDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bound") Bound bound,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(bound != null, "bound must not be null");

    this.dimension = dimension;
    this.bound = bound;
    this.filterTuning = filterTuning;
  }

  @VisibleForTesting
  public SpatialDimFilter(String dimension, Bound bound)
  {
    this(dimension, bound, null);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimBytes = StringUtils.toUtf8(dimension);
    byte[] boundBytes = bound.getCacheKey();

    return ByteBuffer.allocate(2 + dimBytes.length + boundBytes.length)
                     .put(DimFilterUtils.SPATIAL_CACHE_ID)
                     .put(dimBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(boundBytes)
                     .array();
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public Bound getBound()
  {
    return bound;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public Filter toFilter()
  {
    return new SpatialFilter(dimension, bound, filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public String toString()
  {
    return "SpatialDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", bound=" + bound +
           ", filterTuning=" + filterTuning +
           '}';
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
    SpatialDimFilter that = (SpatialDimFilter) o;
    return dimension.equals(that.dimension) &&
           bound.equals(that.bound) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, bound, filterTuning);
  }
}
