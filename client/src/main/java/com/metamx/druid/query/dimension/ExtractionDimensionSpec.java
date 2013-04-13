/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.query.extraction.DimExtractionFn;

import java.nio.ByteBuffer;

/**
 */
public class ExtractionDimensionSpec implements DimensionSpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String dimension;
  private final DimExtractionFn dimExtractionFn;
  private final String outputName;

  @JsonCreator
  public ExtractionDimensionSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("outputName") String outputName,
      @JsonProperty("dimExtractionFn") DimExtractionFn dimExtractionFn
  )
  {
    this.dimension = dimension;
    this.dimExtractionFn = dimExtractionFn;

    // Do null check for backwards compatibility
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
  @JsonProperty
  public DimExtractionFn getDimExtractionFn()
  {
    return dimExtractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = dimension.getBytes();
    byte[] dimExtractionFnBytes = dimExtractionFn.getCacheKey();

    return ByteBuffer.allocate(1 + dimensionBytes.length + dimExtractionFnBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(dimExtractionFnBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "ExtractionDimensionSpec{" +
           "dimension='" + dimension + '\'' +
           ", dimExtractionFn=" + dimExtractionFn +
           ", outputName='" + outputName + '\'' +
           '}';
  }
}
