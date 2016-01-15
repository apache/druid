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
import com.google.common.primitives.Ints;
import com.metamx.common.StringUtils;

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
      pagingKeys[index] = StringUtils.toUtf8(entry.getKey());
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

  public PagingOffset getOffset(String identifier, boolean descending)
  {
    Integer offset = pagingIdentifiers.get(identifier);
    if (offset == null) {
      offset = PagingOffset.toOffset(0, descending);
    }
    return PagingOffset.of(offset, threshold);
  }

}
