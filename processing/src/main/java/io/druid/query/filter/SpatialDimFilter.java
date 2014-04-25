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
package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.collections.spatial.search.Bound;

import java.nio.ByteBuffer;

/**
 */
public class SpatialDimFilter implements DimFilter
{
  private final String dimension;
  private final Bound bound;

  @JsonCreator
  public SpatialDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("bound") Bound bound
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(bound != null, "bound must not be null");

    this.dimension = dimension;
    this.bound = bound;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimBytes = dimension.getBytes();
    byte[] boundBytes = bound.getCacheKey();

    return ByteBuffer.allocate(1 + dimBytes.length + boundBytes.length)
                     .put(DimFilterCacheHelper.SPATIAL_CACHE_ID)
                     .put(dimBytes)
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

    if (bound != null ? !bound.equals(that.bound) : that.bound != null) {
      return false;
    }
    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (bound != null ? bound.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "SpatialDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", bound=" + bound +
           '}';
  }
}
