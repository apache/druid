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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;

/**
 */
public class InsensitiveContainsSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String value;

  @JsonCreator
  public InsensitiveContainsSearchQuerySpec(
      @JsonProperty("value") String value
  )
  {
    this.value = value.toLowerCase();
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Override
  public boolean accept(String dimVal)
  {
    if (dimVal == null) {
      return false;
    }
    return dimVal.toLowerCase().contains(value);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] valueBytes = value.getBytes();

    return ByteBuffer.allocate(1 + valueBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "InsensitiveContainsSearchQuerySpec{" +
           "value=" + value +
           "}";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    InsensitiveContainsSearchQuerySpec that = (InsensitiveContainsSearchQuerySpec) o;

    if (value != null ? !value.equals(that.value) : that.value != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }
}
