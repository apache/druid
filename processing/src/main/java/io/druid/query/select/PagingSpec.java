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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 */
public class PagingSpec
{
  public static PagingSpec newSpec(int threshold)
  {
    return new PagingSpec(null, threshold);
  }

  public static Map<String, Integer> merge(Iterable<Map<String, Integer>> cursors)
  {
    Map<String, Integer> next = Maps.newHashMap();
    for (Map<String, Integer> cursor : cursors) {
      for (Map.Entry<String, Integer> entry : cursor.entrySet()) {
        next.put(entry.getKey(), entry.getValue());
      }
    }
    return next;
  }

  public static Map<String, Integer> next(Map<String, Integer> cursor, boolean descending)
  {
    for (Map.Entry<String, Integer> entry : cursor.entrySet()) {
      entry.setValue(descending ? entry.getValue() - 1 : entry.getValue() + 1);
    }
    return cursor;
  }

  private final Map<String, Integer> pagingIdentifiers;
  private final int threshold;
  private final boolean fromNext;

  @JsonCreator
  public PagingSpec(
      @JsonProperty("pagingIdentifiers") Map<String, Integer> pagingIdentifiers,
      @JsonProperty("threshold") int threshold,
      @JsonProperty("fromNext") boolean fromNext
  )
  {
    this.pagingIdentifiers = pagingIdentifiers == null ? Maps.<String, Integer>newHashMap() : pagingIdentifiers;
    this.threshold = threshold;
    this.fromNext = fromNext;
  }

  public PagingSpec(Map<String, Integer> pagingIdentifiers, int threshold)
  {
    this(pagingIdentifiers, threshold, false);
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

  @JsonProperty
  public boolean isFromNext()
  {
    return fromNext;
  }

  public byte[] getCacheKey()
  {
    final byte[][] pagingKeys = new byte[pagingIdentifiers.size()][];
    final byte[][] pagingValues = new byte[pagingIdentifiers.size()][];

    int index = 0;
    int pagingKeysSize = 0;
    int pagingValuesSize = 0;
    for (Map.Entry<String, Integer> entry : pagingIdentifiers.entrySet()) {
      pagingKeys[index] = StringUtils.toUtf8(entry.getKey());
      pagingValues[index] = ByteBuffer.allocate(Ints.BYTES).putInt(entry.getValue()).array();
      pagingKeysSize += pagingKeys[index].length;
      pagingValuesSize += Ints.BYTES;
      index++;
    }

    final byte[] thresholdBytes = ByteBuffer.allocate(Ints.BYTES).putInt(threshold).array();

    final ByteBuffer queryCacheKey = ByteBuffer.allocate(pagingKeysSize + pagingValuesSize + thresholdBytes.length + 1);

    for (byte[] pagingKey : pagingKeys) {
      queryCacheKey.put(pagingKey);
    }

    for (byte[] pagingValue : pagingValues) {
      queryCacheKey.put(pagingValue);
    }

    queryCacheKey.put(thresholdBytes);
    queryCacheKey.put(isFromNext() ? (byte) 0x01 : 0x00);

    return queryCacheKey.array();
  }

  public PagingOffset getOffset(String identifier, boolean descending)
  {
    Integer offset = pagingIdentifiers.get(identifier);
    if (offset == null) {
      offset = PagingOffset.toOffset(0, descending);
    } else if (fromNext) {
      offset = descending ? offset - 1 : offset + 1;
    }
    return PagingOffset.of(offset, threshold);
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

    PagingSpec that = (PagingSpec) o;

    if (fromNext != that.fromNext) {
      return false;
    }
    if (threshold != that.threshold) {
      return false;
    }
    if (!pagingIdentifiers.equals(that.pagingIdentifiers)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = pagingIdentifiers.hashCode();
    result = 31 * result + threshold;
    result = 31 * result + (fromNext ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "PagingSpec{" +
           "pagingIdentifiers=" + pagingIdentifiers +
           ", threshold=" + threshold +
           ", fromNext=" + fromNext +
           '}';
  }
}
