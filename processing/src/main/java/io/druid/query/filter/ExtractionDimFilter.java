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
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import io.druid.query.extraction.DimExtractionFn;

import java.nio.ByteBuffer;

/**
 */
public class ExtractionDimFilter implements DimFilter
{
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
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(value != null, "value must not be null");
    Preconditions.checkArgument(dimExtractionFn != null, "extraction function must not be null");

    this.dimension = dimension;
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
    byte[] dimensionBytes = dimension.getBytes(Charsets.UTF_8);
    byte[] valueBytes = value.getBytes(Charsets.UTF_8);

    return ByteBuffer.allocate(1 + dimensionBytes.length + valueBytes.length)
                     .put(DimFilterCacheHelper.EXTRACTION_CACHE_ID)
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
