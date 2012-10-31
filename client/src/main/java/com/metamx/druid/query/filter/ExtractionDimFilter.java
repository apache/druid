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

package com.metamx.druid.query.filter;

import java.nio.ByteBuffer;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import com.metamx.druid.query.extraction.DimExtractionFn;

/**
 */
public class ExtractionDimFilter implements DimFilter
{
  private static final byte CACHE_TYPE_ID = 0x4;

  private final String dimension;
  private final String value;
  private final DimExtractionFn dimExtractionFn;

  @JsonCreator
  public ExtractionDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("dimExtractionFn") DimExtractionFn dimExtractionFn
  )
  {
    this.dimension = dimension.toLowerCase();
    this.value = value;
    this.dimExtractionFn = dimExtractionFn;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @JsonProperty
  public DimExtractionFn getDimExtractionFn()
  {
    return dimExtractionFn;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = dimension.getBytes();
    byte[] valueBytes = value.getBytes();

    return ByteBuffer.allocate(1 + dimensionBytes.length + valueBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(dimensionBytes)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return String.format("%s(%s) = %s", dimExtractionFn, dimension, value);
  }
}
