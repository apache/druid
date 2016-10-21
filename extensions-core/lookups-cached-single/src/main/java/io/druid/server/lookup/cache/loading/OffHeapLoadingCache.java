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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import io.druid.java.util.common.ISE;

import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class OffHeapLoadingCache<K, V> implements LoadingCache<K, V>
{
  private static final DB DB = DBMaker.newMemoryDirectDB().transactionDisable().closeOnJvmShutdown().make();

  private final HTreeMap<K, V> cache;
  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);
  private final AtomicLong evictionCount = new AtomicLong(0);
  private final AtomicBoolean closed = new AtomicBoolean(true);

  /**
   * Sets store size limit. Disk or memory space consumed be storage should not grow over this space.
   * maximal size of store in GB, if store is larger entries will start expiring
   */
  @JsonProperty
  private final double maxStoreSize;

  /**
   * Sets maximal number of entries in this map.
   * Less used entries will be expired and removed to make collection smaller
   */
  @JsonProperty
  private final long maxEntriesSize;

  /**
   * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
   */
  @JsonProperty
  private final long expireAfterWrite;

  /**
   * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
   */
  @JsonProperty
  private final long expireAfterAccess;

  private final String name = UUID.randomUUID().toString();

  @JsonCreator
  public OffHeapLoadingCache(
      @JsonProperty("maxStoreSize") double maxStoreSize,
      @JsonProperty("maxEntriesSize") long maxEntriesSize,
      @JsonProperty("expireAfterWrite") long expireAfterWrite,
      @JsonProperty("expireAfterAccess") long expireAfterAccess
  )
  {
    this.maxStoreSize = maxStoreSize;
    this.maxEntriesSize = maxEntriesSize;
    this.expireAfterWrite = expireAfterWrite;
    this.expireAfterAccess = expireAfterAccess;
    this.cache = DB.createHashMap(name)
                   .expireStoreSize(this.maxStoreSize)
                   .expireAfterWrite(this.expireAfterWrite, TimeUnit.MILLISECONDS)
                   .expireAfterAccess(this.expireAfterAccess, TimeUnit.MILLISECONDS)
                   .expireMaxSize(this.maxEntriesSize)
                   .make();
    cache.modificationListenerAdd(new Bind.MapListener<K, V>()
    {
      @Override
      public void update(K key, V oldVal, V newVal)
      {
        if (oldVal != null && newVal == null) {
          // eviction or remove call
          evictionCount.getAndIncrement();
        }
      }
    });
    this.closed.set(false);
  }

  @Override
  public V getIfPresent(K key)
  {
    V value = cache.get(key);
    if (value == null) {
      missCount.getAndIncrement();
    } else {
      hitCount.getAndIncrement();
    }
    return value;
  }

  @Override
  public Map<K, V> getAllPresent(final Iterable<K> keys)
  {
    ImmutableMap.Builder builder = ImmutableMap.builder();
    for (K key : keys
        ) {
      V value = getIfPresent(key);
      if (value != null) {
        builder.put(key, value);
      }
    }
    return builder.build();
  }

  @Override
  public V get(K key, final Callable<? extends V> valueLoader) throws ExecutionException
  {
    synchronized (key) {
      V value = cache.get(key);
      if (value != null) {
        return value;
      }
      try {
        value = valueLoader.call();
        cache.put(key, value);
        return value;
      }
      catch (Exception e) {
        throw new ISE(e, "got an exception while loading key [%s]", key);
      }
    }
  }


  @Override
  public void putAll(Map<? extends K, ? extends V> m)
  {
    cache.putAll(m);
  }

  @Override
  public void invalidate(K key)
  {
    cache.remove(key);
  }

  @Override
  public void invalidateAll(Iterable<K> keys)
  {
    for (K key : keys) {
      invalidate(key);
    }
  }

  @Override
  public void invalidateAll()
  {
    cache.clear();
  }

  @Override
  public LookupCacheStats getStats()
  {
    return new LookupCacheStats(hitCount.get(), missCount.get(), evictionCount.get());
  }

  @Override
  public boolean isClosed()
  {
    return closed.get();
  }

  @Override
  public void close()
  {
    if (!closed.getAndSet(true)) {
      DB.delete(String.valueOf(name));
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OffHeapLoadingCache)) {
      return false;
    }

    OffHeapLoadingCache<?, ?> that = (OffHeapLoadingCache<?, ?>) o;

    if (Double.compare(that.maxStoreSize, maxStoreSize) != 0) {
      return false;
    }
    if (maxEntriesSize != that.maxEntriesSize) {
      return false;
    }
    if (expireAfterWrite != that.expireAfterWrite) {
      return false;
    }
    return expireAfterAccess == that.expireAfterAccess;

  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    temp = Double.doubleToLongBits(maxStoreSize);
    result = (int) (temp ^ (temp >>> 32));
    result = 31 * result + (int) (maxEntriesSize ^ (maxEntriesSize >>> 32));
    result = 31 * result + (int) (expireAfterWrite ^ (expireAfterWrite >>> 32));
    result = 31 * result + (int) (expireAfterAccess ^ (expireAfterAccess >>> 32));
    return result;
  }
}
