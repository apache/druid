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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 */
public class PagingSpec
{
  private final LinkedHashMap<String, Integer> pagingIdentifiers;
  private final int threshold;

  @JsonCreator
  public PagingSpec(
      @JsonProperty("pagingIdentifiers") LinkedHashMap<String, Integer> pagingIdentifiers,
      @JsonProperty("threshold") int threshold
  )
  {
    this.pagingIdentifiers = pagingIdentifiers == null ? new LinkedHashMap<String, Integer>() : pagingIdentifiers;
    this.threshold = threshold;
  }

  @JsonProperty
  public Map<String, Integer> getPagingIdentifiers()
  {
    return pagingIdentifiers;
  }

  @JsonProperty
  public int getThreshold()
  {
    return threshold;
  }

  public byte[] getCacheKey()
  {
    final byte[][] pagingKeys = new byte[pagingIdentifiers.size()][];
    final byte[][] pagingValues = new byte[pagingIdentifiers.size()][];

    int index = 0;
    int pagingKeysSize = 0;
    int pagingValuesSize = 0;
    for (Map.Entry<String, Integer> entry : pagingIdentifiers.entrySet()) {
      pagingKeys[index] = entry.getKey().getBytes(Charsets.UTF_8);
      pagingValues[index] = ByteBuffer.allocate(Ints.BYTES).putInt(entry.getValue()).array();
      pagingKeysSize += pagingKeys[index].length;
      pagingValuesSize += Ints.BYTES;
      index++;
    }

    final byte[] thresholdBytes = ByteBuffer.allocate(Ints.BYTES).putInt(threshold).array();

    final ByteBuffer queryCacheKey = ByteBuffer.allocate(pagingKeysSize + pagingValuesSize + thresholdBytes.length);

    for (byte[] pagingKey : pagingKeys) {
      queryCacheKey.put(pagingKey);
    }

    for (byte[] pagingValue : pagingValues) {
      queryCacheKey.put(pagingValue);
    }

    queryCacheKey.put(thresholdBytes);

    return queryCacheKey.array();
  }

  @Override
  public String toString()
  {
    return "PagingSpec{" +
           "pagingIdentifiers=" + pagingIdentifiers +
           ", threshold=" + threshold +
           '}';
  }
}
