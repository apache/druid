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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.query.extraction.DimExtractionFn;

import java.nio.ByteBuffer;

/**
 */
public class DefaultDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x0;
  private final String dimension;
  private final String outputName;

  @JsonCreator
  public DefaultDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName
  )
  {
    this.dimension = dimension;

    // Do null check for legacy backwards compatibility, callers should be setting the value.
    this.outputName = outputName == null ? dimension : outputName;
  }

  @Override
  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public DimExtractionFn getDimExtractionFn()
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = dimension.getBytes();

    return ByteBuffer.allocate(1 + dimensionBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "DefaultDimensionSpec{" +
           "dimension='" + dimension + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DefaultDimensionSpec that = (DefaultDimensionSpec) o;

    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) return false;
    if (outputName != null ? !outputName.equals(that.outputName) : that.outputName != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (outputName != null ? outputName.hashCode() : 0);
    return result;
  }
}
