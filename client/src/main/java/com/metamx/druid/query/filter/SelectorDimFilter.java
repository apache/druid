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

/**
 */
public class SelectorDimFilter implements DimFilter
{
  private static final byte CACHE_TYPE_ID = 0x0;

  private final String dimension;
  private final String value;

  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value
  )
  {
    this.dimension = dimension.toLowerCase();
    this.value = value;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SelectorDimFilter that = (SelectorDimFilter) o;

    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }
    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return String.format("%s = %s", dimension, value);
  }
}
