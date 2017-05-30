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

package io.druid.client.cache;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.Weigher;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.emitter.service.ServiceEmitter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class MapCache implements Cache
{
  public static MapCache create(long sizeInBytes) {
    return create(sizeInBytes, 0, 4);
  }
  public static MapCache create(long sizeInBytes, int initialCapacity, int concurrencyLevel)
  {
    return new MapCache(sizeInBytes, initialCapacity, concurrencyLevel);
  }

  private final com.google.common.cache.Cache<ByteBuffer, byte[]> baseCache;
  private final com.google.common.cache.LoadingCache<String, Integer> namespaceId;
  private final AtomicInteger ids = new AtomicInteger();

  private MapCache(long sizeInBytes, int initialCapacity, int concurrencyLevel)
  {
    this.baseCache = CacheBuilder
        .newBuilder()
        .initialCapacity(initialCapacity)
        .maximumWeight(sizeInBytes)
        .recordStats()
        .concurrencyLevel(concurrencyLevel)
        .weigher(
            new Weigher<ByteBuffer, byte[]>()
            {
              @Override
              public int weigh(ByteBuffer key, byte[] value)
              {
                return key.remaining() + value.length;
              }
            }
        )
        .build();

    this.namespaceId = CacheBuilder
        .newBuilder()
        .concurrencyLevel(concurrencyLevel)
        .build(new CacheLoader<String, Integer>()
        {
          @Override
          public Integer load(String namespace) throws Exception
          {
            return ids.getAndIncrement();
          }
        });
  }

  @Override
  public CacheStats getStats()
  {
    final com.google.common.cache.CacheStats stats = baseCache.stats();
    return new CacheStats(
        stats.hitCount(),
        stats.missCount(),
        baseCache.size(),
        -1,
        stats.evictionCount(),
        0,
        0
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    return baseCache.getIfPresent(computeKey(namespaceId.getUnchecked(key.namespace), key.key));
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    baseCache.put(computeKey(namespaceId.getUnchecked(key.namespace), key.key), value);
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<NamedKey, byte[]> retVal = Maps.newHashMap();
    for (NamedKey key : keys) {
      final byte[] value = get(key);
      if (value != null) {
        retVal.put(key, value);
      }
    }
    return retVal;
  }

  @Override
  public void close(String namespace)
  {
    final Integer id = namespaceId.asMap().remove(namespace);
    if (id != null) {
      List<ByteBuffer> toRemove = Lists.newLinkedList();
      final int unboxed = id;
      for (ByteBuffer next : baseCache.asMap().keySet()) {
        if (next.getInt(0) == unboxed) {
          toRemove.add(next);
        }
      }
      baseCache.invalidateAll(toRemove);
    }
  }

  private ByteBuffer computeKey(int id, byte[] key)
  {
    final ByteBuffer retVal = ByteBuffer
        .allocate(key.length + 4)
        .putInt(id)
        .put(key);
    retVal.rewind();
    return retVal;
  }

  @Override
  public boolean isLocal()
  {
    return true;
  }

  @Override
  public void doMonitor(ServiceEmitter emitter)
  {
    // No special monitoring
  }
}
