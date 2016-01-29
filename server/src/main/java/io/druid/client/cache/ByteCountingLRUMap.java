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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.metamx.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;

/**
 */
class ByteCountingLRUMap
{
  private static final Logger log = new Logger(ByteCountingLRUMap.class);

  private final com.google.common.cache.Cache<ByteBuffer, byte[]> cache;
  private final boolean logEvictions;
  private final int logEvictionCount;
  private final long sizeInBytes;

  private volatile long numBytes;
  private volatile long evictionCount;

  public ByteCountingLRUMap(
      final long sizeInBytes
  )
  {
    this(16, 1, 0, sizeInBytes);
  }

  public ByteCountingLRUMap(
      final int initialSize,
      final int concurrencyLevel,
      final int logEvictionCount,
      final long sizeInBytes
  )
  {
    this.logEvictionCount = logEvictionCount;
    this.sizeInBytes = sizeInBytes;

    logEvictions = logEvictionCount != 0;
    numBytes = 0;
    evictionCount = 0;

    RemovalListener<ByteBuffer, byte[]> listener = new RemovalListener<ByteBuffer, byte[]>()
    {
      @Override
      public void onRemoval(RemovalNotification<ByteBuffer, byte[]> entry)
      {
        ++evictionCount;
        if (logEvictions && evictionCount % logEvictionCount == 0) {
          log.info(
              "Evicting %,dth element.  Size[%,d], numBytes[%,d], averageSize[%,d]",
              evictionCount,
              size(),
              numBytes,
              numBytes / size()
          );
        }
        numBytes -= entry.getKey().remaining() + entry.getValue().length;
      }
    };
    cache = CacheBuilder
        .newBuilder()
        .initialCapacity(initialSize)
        .concurrencyLevel(concurrencyLevel)
        .maximumWeight(sizeInBytes)
        .removalListener(listener)
        .weigher(new Weigher<ByteBuffer, byte[]>()
        {
          public int weigh(ByteBuffer key, byte[] value)
          {
            int weigh = key.remaining() + value.length;
            numBytes += weigh;
            return weigh;
          }
        }).build();
  }

  public long getNumBytes()
  {
    return numBytes;
  }

  public long getEvictionCount()
  {
    return evictionCount;
  }

  public byte[] get(ByteBuffer key)
  {
    return cache.getIfPresent(key);
  }

  public void put(ByteBuffer key, byte[] value)
  {
    cache.put(key, value);
  }

  public byte[] remove(Object key)
  {
    byte[] value = cache.getIfPresent(key);
    cache.invalidate(key);
    return value;
  }

  /**
   * Don't allow key removal using the underlying keySet iterator
   * All removal operations must use ByteCountingLRUMap.remove()
   */
  public Set<ByteBuffer> keySet()
  {
    return Collections.unmodifiableSet(cache.asMap().keySet());
  }

  public void clear()
  {
    cache.invalidateAll();
  }

  public long size()
  {
    return cache.size();
  }
}
