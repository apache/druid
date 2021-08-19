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

import org.apache.druid.common.guava.SettableSupplier;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A simple cache to hold string representation of the given keys.
 * This cache can be used to avoid repetitive calls of expensive {@link Object#toString()}
 * or JSON serializations.
 *
 * This cache uses a simple caching policy that keeps only the first N entries.
 * The cache size is limited by the {@link #cacheSize} constructor parameter.
 *
 * @param <K> cache key type
 *
 * @see SegmentsTableRow#toObjectArray
 */
class ObjectStringCache<K>
{
  private final Function<K, String> serializer;
  private final long cacheSize;
  private final Map<K, String> cache;

  ObjectStringCache(Function<K, String> serializer, long cacheSize)
  {
    this.serializer = serializer;
    this.cacheSize = cacheSize;
    this.cache = new HashMap<>();
  }

  @Nullable
  String add(@Nullable K item)
  {
    if (item == null) {
      return null;
    }

    SettableSupplier<String> resultSupplier = new SettableSupplier<>();
    cache.compute(
        item,
        (k, v) -> {
          if (v != null) {
            resultSupplier.set(v);
            return v;
          }

          String serialized = serializer.apply(k);
          resultSupplier.set(serialized);

          if (cache.size() >= cacheSize) {
            // Cache is full
            return null;
          } else {
            return serialized;
          }
        }
    );
    return resultSupplier.get();
  }
}
