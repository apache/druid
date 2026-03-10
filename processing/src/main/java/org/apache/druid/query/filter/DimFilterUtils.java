/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.filter;

import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class DimFilterUtils
{
  static final byte SELECTOR_CACHE_ID = 0x0;
  static final byte AND_CACHE_ID = 0x1;
  static final byte OR_CACHE_ID = 0x2;
  static final byte NOT_CACHE_ID = 0x3;
  static final byte EXTRACTION_CACHE_ID = 0x4;
  static final byte REGEX_CACHE_ID = 0x5;
  static final byte SEARCH_QUERY_TYPE_ID = 0x6;
  static final byte JAVASCRIPT_CACHE_ID = 0x7;
  static final byte SPATIAL_CACHE_ID = 0x8;
  static final byte IN_CACHE_ID = 0x9;
  static final byte BOUND_CACHE_ID = 0xA;
  static final byte INTERVAL_CACHE_ID = 0xB;
  static final byte LIKE_CACHE_ID = 0xC;
  static final byte COLUMN_COMPARISON_CACHE_ID = 0xD;
  static final byte EXPRESSION_CACHE_ID = 0xE;
  static final byte TRUE_CACHE_ID = 0xF;
  static final byte FALSE_CACHE_ID = 0x11;
  public static final byte BLOOM_DIM_FILTER_CACHE_ID = 0x10;
  static final byte NULL_CACHE_ID = 0x12;
  static final byte EQUALS_CACHE_ID = 0x13;
  static final byte RANGE_CACHE_ID = 0x14;
  static final byte IS_FILTER_BOOLEAN_FILTER_CACHE_ID = 0x15;
  static final byte ARRAY_CONTAINS_CACHE_ID = 0x16;
  static final byte TYPED_IN_CACHE_ID = 0x17;


  public static final byte STRING_SEPARATOR = (byte) 0xFF;

  static byte[] computeCacheKey(byte cacheIdKey, List<DimFilter> filters)
  {
    if (filters.size() == 1) {
      return filters.get(0).getCacheKey();
    }

    byte[][] cacheKeys = new byte[filters.size()][];
    int totalSize = 0;
    int index = 0;
    for (DimFilter field : filters) {
      cacheKeys[index] = field.getCacheKey();
      totalSize += cacheKeys[index].length;
      ++index;
    }

    ByteBuffer retVal = ByteBuffer.allocate(1 + totalSize);
    retVal.put(cacheIdKey);
    for (byte[] cacheKey : cacheKeys) {
      retVal.put(cacheKey);
    }
    return retVal.array();
  }
}
