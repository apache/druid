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

package io.druid.query.dimension;

import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;

import java.util.function.IntFunction;

/**
 * LRU cache for an IntFunction, intended for use with DimensionSelectors.
 *
 * @see io.druid.segment.DimensionSelectorUtils#cacheIfPossible
 */
public class LruCacheIntFunction<T> implements IntFunction<T>
{
  // After a certain point, freeze or ignore the cache, based on hit rate. The idea is that hopefully by then,
  // LRU should have some reasonable values in the cache if there are reasonable values to be found.
  private static final int CACHE_FREEZE_FACTOR = 10;

  enum State
  {
    ACTIVE,
    FROZEN,
    IGNORED
  }

  private final IntFunction<T> function;
  private final int cacheSize;
  private final long lookupsBeforeFreezing;
  private Int2ObjectLinkedOpenHashMap<T> cache;

  private State state = State.IGNORED;
  private long hits;
  private long lookups;

  public LruCacheIntFunction(
      final IntFunction<T> function,
      final int cacheSize
  )
  {
    this.function = function;
    this.cacheSize = cacheSize;
    this.lookupsBeforeFreezing = cacheSize * CACHE_FREEZE_FACTOR;
    this.cache = new Int2ObjectLinkedOpenHashMap<>(cacheSize);
  }

  @Override
  public T apply(final int id)
  {
    if (state == State.IGNORED) {
      return function.apply(id);
    } else if (state == State.FROZEN) {
      final T cachedValue = cache.get(id);
      if (cachedValue != null) {
        return cachedValue;
      } else {
        return function.apply(id);
      }
    }

    lookups++;

    T value = cache.getAndMoveToFirst(id);

    if (value == null) {
      value = function.apply(id);

      while (cache.size() >= cacheSize) {
        cache.removeLast();
      }

      cache.putAndMoveToFirst(id, value);
    } else {
      hits++;
    }

    // After a certain point, freeze or ignore the cache, based on hit rate.
    if (lookups > lookupsBeforeFreezing) {
      if (hits < lookups / 3) {
        // Hit rate < 33%
        state = State.IGNORED;

        // Drop reference to cache to help GC
        cache = null;
      } else {
        state = State.FROZEN;
      }
    }

    return value;
  }
}
