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

import java.util.function.IntFunction;

/**
 * Array cache for an IntFunction, intended for use with DimensionSelectors.
 *
 * @see io.druid.segment.DimensionSelectorUtils#cacheIfPossible
 */
public class ArrayCacheIntFunction<T> implements IntFunction<T>
{
  private final IntFunction<T> function;
  private final Object[] cache;

  public ArrayCacheIntFunction(final IntFunction<T> function, final int cacheSize)
  {
    this.function = function;
    this.cache = new Object[cacheSize];
  }

  @Override
  public T apply(final int id)
  {
    // Will not cache the result if "function" returns null. I'm hoping that this is the right choice, and enabling
    // null caching isn't worth the overhead of using some additional data structures to differentiate between a null
    // result and an uncached result.

    if (cache[id] == null) {
      final T value = function.apply(id);
      cache[id] = value;
      return value;
    } else {
      //noinspection unchecked
      return (T) cache[id];
    }
  }
}
