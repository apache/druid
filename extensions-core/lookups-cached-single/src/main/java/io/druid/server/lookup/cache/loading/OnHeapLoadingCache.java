/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package io.druid.server.lookup.cache.loading;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.druid.java.util.common.logger.Logger;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class OnHeapLoadingCache<K, V> implements LoadingCache<K, V>
{
  private final static Logger log = new Logger(OnHeapLoadingCache.class);
  private static final int DEFAULT_INITIAL_CAPACITY = 16;
  //See com.google.common.cache.CacheBuilder#DEFAULT_CONCURRENCY_LEVEL
  private static final int DEFAULT_CONCURRENCY_LEVEL = 4;

  private final Cache<K, V> cache;
  private final AtomicBoolean isClosed = new AtomicBoolean(false);
  @JsonProperty
  private final int concurrencyLevel;
  @JsonProperty
  private final int initialCapacity;
  @JsonProperty
  private final Long maximumSize;
  @JsonProperty
  private final Long expireAfterAccess;
  @JsonProperty
  private final Long expireAfterWrite;


  /**
   * @param concurrencyLevel  default to {@code DEFAULT_CONCURRENCY_LEVEL}
   * @param initialCapacity   default to {@code DEFAULT_INITIAL_CAPACITY}
   * @param maximumSize       Max number of entries that the cache can hold, When set to zero, elements will be evicted immediately after being loaded into the
   *                          cache.
   *                          When set to null, cache maximum size is infinity
   * @param expireAfterAccess Specifies that each entry should be automatically removed from the cache once a fixed duration
   *                          has elapsed after the entry's creation, the most recent replacement of its value, or its last
   *                          access. Access time is reset by all cache read and write operations.
   *                          No read-time-based eviction when set to null.
   * @param expireAfterWrite  Specifies that each entry should be automatically removed from the cache once a fixed duration
   *                          has elapsed after the entry's creation, or the most recent replacement of its value.
   *                          No write-time-based eviction when set to null.
   */
  @JsonCreator
  public OnHeapLoadingCache(
      @JsonProperty("concurrencyLevel") int concurrencyLevel,
      @JsonProperty("initialCapacity") int initialCapacity,
      @JsonProperty("maximumSize") Long maximumSize,
      @JsonProperty("expireAfterAccess") Long expireAfterAccess,
      @JsonProperty("expireAfterWrite") Long expireAfterWrite
  )
  {
    this.concurrencyLevel = concurrencyLevel <= 0 ? DEFAULT_CONCURRENCY_LEVEL : concurrencyLevel;
    this.initialCapacity = initialCapacity <= 0 ? DEFAULT_INITIAL_CAPACITY : initialCapacity;
    this.maximumSize = maximumSize;
    this.expireAfterAccess = expireAfterAccess;
    this.expireAfterWrite = expireAfterWrite;
    CacheBuilder builder = CacheBuilder.newBuilder()
                                       .concurrencyLevel(this.concurrencyLevel)
                                       .initialCapacity(this.initialCapacity)
                                       .recordStats();
    if (this.expireAfterAccess != null) {
      builder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
    }
    if (this.expireAfterWrite != null) {
      builder.expireAfterWrite(this.expireAfterWrite, TimeUnit.MILLISECONDS);
    }
    if (this.maximumSize != null) {
      builder.maximumSize(this.maximumSize);
    }

    this.cache = builder.build();

    if (isClosed.getAndSet(false)) {
      log.info("Guava Based OnHeapCache started with spec [%s]", cache.toString());
    }
  }


  @Override
  public V getIfPresent(K key)
  {
    return cache.getIfPresent(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m)
  {
    cache.putAll(m);
  }


  @Override
  public Map<K, V> getAllPresent(Iterable<K> keys)
  {
    return cache.getAllPresent(keys);
  }

  @Override
  public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException
  {
    return cache.get(key, valueLoader);
  }

  @Override
  public void invalidate(K key)
  {
    cache.invalidate(key);
  }

  @Override
  public void invalidateAll(Iterable<K> keys)
  {
    cache.invalidateAll(keys);
  }

  @Override
  public void invalidateAll()
  {
    cache.invalidateAll();
    cache.cleanUp();
  }

  @Override
  public LookupCacheStats getStats()
  {
    return new LookupCacheStats(
        cache.stats().hitCount(),
        cache.stats().missCount(),
        cache.stats().evictionCount()
    );
  }

  @Override
  public boolean isClosed()
  {
    return isClosed.get();
  }

  @Override
  public void close()
  {
    if (!isClosed.getAndSet(true)) {
      cache.cleanUp();
    }
  }


  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OnHeapLoadingCache)) {
      return false;
    }

    OnHeapLoadingCache<?, ?> that = (OnHeapLoadingCache<?, ?>) o;

    if (concurrencyLevel != that.concurrencyLevel) {
      return false;
    }
    if (initialCapacity != that.initialCapacity) {
      return false;
    }
    if (maximumSize != null ? !maximumSize.equals(that.maximumSize) : that.maximumSize != null) {
      return false;
    }
    if (expireAfterAccess != null
        ? !expireAfterAccess.equals(that.expireAfterAccess)
        : that.expireAfterAccess != null) {
      return false;
    }
    return expireAfterWrite != null ? expireAfterWrite.equals(that.expireAfterWrite) : that.expireAfterWrite == null;

  }

  @Override
  public int hashCode()
  {
    int result = concurrencyLevel;
    result = 31 * result + initialCapacity;
    result = 31 * result + (maximumSize != null ? maximumSize.hashCode() : 0);
    result = 31 * result + (expireAfterAccess != null ? expireAfterAccess.hashCode() : 0);
    result = 31 * result + (expireAfterWrite != null ? expireAfterWrite.hashCode() : 0);
    return result;
  }
}
