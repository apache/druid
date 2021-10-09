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

package org.apache.druid.sql.calcite.schema;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * A simple cache based on Caffeine to hold string representation of the given keys.
 * This cache can be used to avoid repetitive calls of expensive {@link Object#toString()}
 * or JSON serializations.
 *
 * @param <K> cache key type
 *
 * @see SegmentsTableRow#toObjectArray
 */
public class CaffeineObjectStringCache<K>
{
  private final Function<K, String> serializer;
  private final Cache<K, String> cache;

  public CaffeineObjectStringCache(Function<K, String> serializer, long cacheSizeBytes)
  {
    this.serializer = serializer;
    this.cache = Caffeine
        .newBuilder()
        .maximumWeight(cacheSizeBytes)
        // We consider only value size here since they are the only extra memory pressure.
        // Keys are just references to existing in-memory objects.
        .weigher((K key, String value) -> value.length())
        .build();
  }

  @Nullable
  public String add(@Nullable K item)
  {
    if (item == null) {
      return null;
    }

    return cache.get(item, serializer);
  }
}
