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

package io.druid.query.filter;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.Sets;
import io.druid.timeline.partition.ShardSpec;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DimFilterUtils
{
  static final byte NOOP_CACHE_ID = -0x4;
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

  /**
   * Filter the given iterable of objects by removing any object whose ShardSpec, obtained from the converter function,
   * does not fit in the RangeSet of the dimFilter {@link DimFilter#getDimensionRangeSet(String)}. The returned set
   * contains the filtered objects in the same order as they appear in input.
   *
   * If you plan to call this multiple times with the same dimFilter, consider using
   * {@link #filterShards(DimFilter, Iterable, Function, Map)} instead with a cached map
   *
   * @param dimFilter The filter to use
   * @param input The iterable of objects to be filtered
   * @param converter The function to convert T to ShardSpec that can be filtered by
   * @param <T> This can be any type, as long as transform function is provided to convert this to ShardSpec
   * @return The set of filtered object, in the same order as input
   */
  public static <T> Set<T> filterShards(DimFilter dimFilter, Iterable<T> input, Function<T, ShardSpec> converter)
  {
    return filterShards(dimFilter, input, converter, new HashMap<String, Optional<RangeSet<String>>>());
  }

  /**
   * Filter the given iterable of objects by removing any object whose ShardSpec, obtained from the converter function,
   * does not fit in the RangeSet of the dimFilter {@link DimFilter#getDimensionRangeSet(String)}. The returned set
   * contains the filtered objects in the same order as they appear in input.
   *
   * DimensionRangedCache stores the RangeSets of different dimensions for the dimFilter. It should be re-used
   * between calls with the same dimFilter to save redundant calls of {@link DimFilter#getDimensionRangeSet(String)}
   * on same dimensions.
   *
   * @param dimFilter The filter to use
   * @param input The iterable of objects to be filtered
   * @param converter The function to convert T to ShardSpec that can be filtered by
   * @param dimensionRangeCache The cache of RangeSets of different dimensions for the dimFilter
   * @param <T> This can be any type, as long as transform function is provided to convert this to ShardSpec
   * @return The set of filtered object, in the same order as input
   */
  public static <T> Set<T> filterShards(DimFilter dimFilter, Iterable<T> input, Function<T, ShardSpec> converter,
                                        Map<String, Optional<RangeSet<String>>> dimensionRangeCache)
  {
    Set<T> retSet = Sets.newLinkedHashSet();

    for (T obj : input) {
      ShardSpec shard = converter.apply(obj);
      boolean include = true;

      if (dimFilter != null && shard != null) {
        Map<String, Range<String>> domain = shard.getDomain();
        for (Map.Entry<String, Range<String>> entry : domain.entrySet()) {
          String dimension = entry.getKey();
          Optional<RangeSet<String>> optFilterRangeSet = dimensionRangeCache
              .computeIfAbsent(dimension, d-> Optional.fromNullable(dimFilter.getDimensionRangeSet(d)));
          if (optFilterRangeSet.isPresent() && optFilterRangeSet.get().subRangeSet(entry.getValue()).isEmpty()) {
            include = false;
          }
        }
      }

      if (include) {
        retSet.add(obj);
      }
    }
    return retSet;
  }
}
