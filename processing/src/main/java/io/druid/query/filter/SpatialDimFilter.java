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
import io.druid.collections.spatial.search.Bound;
import io.druid.data.input.impl.NewSpatialDimensionSchema;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.filter.SpatialFilter;

import java.nio.ByteBuffer;

/**
 */
public class SpatialDimFilter implements DimFilter
{
  private final String dimension;
  private final String delimiter;
  private final Bound bound;

  @JsonCreator
  public SpatialDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("delimiter") String delimiter,
      @JsonProperty("bound") Bound bound
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(bound != null, "bound must not be null");

    this.dimension = dimension;
    this.bound = bound;
    this.delimiter = delimiter == null ? NewSpatialDimensionSchema.DEFAULT_DELIMITER : delimiter;
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

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getDelimiter()
  {
    return delimiter;
  }

  @JsonProperty
  public Bound getBound()
  {
    return bound;
  }

  @Override
  public Filter toFilter()
  {
    return new SpatialFilter(dimension, delimiter, bound);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
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

    if (!bound.equals(that.bound)) {
      return false;
    }
    if (!dimension.equals(that.dimension)) {
      return false;
    }

    return delimiter.equals(that.delimiter);
  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + bound.hashCode();
    result = 31 * result + delimiter.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "SpatialDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", delimiter='" + delimiter + '\'' +
           ", bound=" + bound +
           '}';
  }
}
