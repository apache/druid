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

package io.druid.server.lookup.cache.loading;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A semi-persistent mapping from keys to values. Cache entries are added using
 * {@link #get(Object, Callable)} and stored in the cache until either evicted or manually invalidated.
 * <p>
 * <p>Implementations of this interface are expected to be thread-safe, and can be safely accessed
 * by multiple concurrent threads.
 *
 * This interface borrows ideas (and in some cases methods and javadoc) from Guava and JCache cache interface.
 * Thanks Guava and JSR !
 * We elected to make this as close as possible to JSR API like that users can build bridges between all the existing implementations of JSR.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "guava", value = OnHeapLoadingCache.class),
    @JsonSubTypes.Type(name = "mapDb", value = OffHeapLoadingCache.class)
})
public interface LoadingCache<K, V> extends Closeable
{
  /**
   * @param key must not be null
   * Returns the value associated with {@code key} in this cache, or {@code null} if there is no
   * cached value for {@code key}.
   * a cache miss should be counted if the key is missing.
   */
  @Nullable
  V getIfPresent(K key);

  /**
   * Returns a map of the values associated with {@code keys} in this cache. The returned map will
   * only contain entries which are already present in the cache.
   */

  Map<K, V> getAllPresent(Iterable<K> keys);

  /**
   * Returns the value associated with {@code key} in this cache, obtaining that value from
   * {@code valueLoader} if necessary. No observable state associated with this cache is modified
   * until loading completes.
   *
   * <p><b>Warning:</b> as with {@link CacheLoader#load}, {@code valueLoader} <b>must not</b> return
   * {@code null}; it may either return a non-null value or throw an exception.
   *
   * @throws ExecutionException if a checked exception was thrown while loading the value
   * @throws UncheckedExecutionException if an unchecked exception was thrown while loading the
   *     value
   * @throws ExecutionError if an error was thrown while loading the value
   *
   */
  V get(K key, Callable<? extends V> valueLoader) throws ExecutionException;

  /**
   * Copies all of the mappings from the specified map to the cache. This method is used for bulk put.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */

  void putAll(Map<? extends K, ? extends V> m);

  /**
   * Discards any cached value for key {@code key}. Eviction can be lazy or eager.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  void invalidate(K key);

  /**
   * Discards any cached values for keys {@code keys}. Eviction can be lazy or eager.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  void invalidateAll(Iterable<K> keys);

  /**
   * Clears the contents of the cache.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  void invalidateAll();

  /**
   * @return Stats of the cache.
   *
   * @throws IllegalStateException if the cache is {@link #isClosed()}
   */
  LookupCacheStats getStats();

  /**
   * @return true if the Cache is closed
   */
  boolean isClosed();

  /**
   * Clean the used resources of the cache. Still not sure about cache lifecycle but as an initial design
   * the namespace deletion event should call this method to clean up resources.
   */

  @Override
  void close();
}
