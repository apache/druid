/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.client.cache;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class MapCache implements Cache
{
  /**
   * An interface to limit the operations that can be done on a Cache so that it is easier to reason about what
   * is actually going to be done.
   */
  public interface Cache
  {
    public byte[] get(byte[] key);
    public void put(byte[] key, byte[] value);
    public void close();
  }

  private final Map<ByteBuffer, byte[]> baseMap;
  private final ByteCountingLRUMap byteCountingLRUMap;

  private final Map<String, Cache> cacheCache;
  private final AtomicInteger ids;

  private final Object clearLock = new Object();

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);

  public static com.metamx.druid.client.cache.Cache create(final MapCacheConfig config)
  {
    return new MapCache(
        new ByteCountingLRUMap(
            config.getInitialSize(),
            config.getLogEvictionCount(),
            config.getSizeInBytes()
        )
    );
  }

  MapCache(
      ByteCountingLRUMap byteCountingLRUMap
  )
  {
    this.byteCountingLRUMap = byteCountingLRUMap;

    this.baseMap = Collections.synchronizedMap(byteCountingLRUMap);

    cacheCache = Maps.newHashMap();
    ids = new AtomicInteger();
  }

  @Override
  public CacheStats getStats()
  {
    return new CacheStats(
        hitCount.get(),
        missCount.get(),
        byteCountingLRUMap.size(),
        byteCountingLRUMap.getNumBytes(),
        byteCountingLRUMap.getEvictionCount(),
        0
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    return provideCache(key.namespace).get(key.key);
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    provideCache(key.namespace).put(key.key, value);
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<NamedKey, byte[]> retVal = Maps.newHashMap();

    for(NamedKey key : keys) {
      retVal.put(key, provideCache(key.namespace).get(key.key));
    }
    return retVal;
  }

  @Override
  public void close(String namespace)
  {
    provideCache(namespace).close();
  }

  private Cache provideCache(final String identifier)
  {
    synchronized (cacheCache) {
      final Cache cachedCache = cacheCache.get(identifier);
      if (cachedCache != null) {
        return cachedCache;
      }

      final byte[] myIdBytes = Ints.toByteArray(ids.getAndIncrement());

      final Cache theCache = new Cache()
      {
        volatile boolean open = true;

        @Override
        public byte[] get(byte[] key)
        {
          if (open) {
            final byte[] retVal = baseMap.get(computeKey(key));
            if (retVal == null) {
              missCount.incrementAndGet();
            } else {
              hitCount.incrementAndGet();
            }
            return retVal;
          }
          throw new ISE("Cache for namespace[%s] is closed.", identifier);
        }

        @Override
        public void put(byte[] key, byte[] value)
        {
          synchronized (clearLock) {
            if (open) {
              baseMap.put(computeKey(key), value);
              return;
            }
          }
          throw new ISE("Cache for namespace[%s] is closed.", identifier);
        }

        @Override
        public void close()
        {
          synchronized (cacheCache) {
            cacheCache.remove(identifier);
          }
          synchronized (clearLock) {
            if (open) {
              open = false;

              Iterator<ByteBuffer> iter = baseMap.keySet().iterator();
              while (iter.hasNext()) {
                ByteBuffer next = iter.next();

                if (next.get(0) == myIdBytes[0]
                    && next.get(1) == myIdBytes[1]
                    && next.get(2) == myIdBytes[2]
                    && next.get(3) == myIdBytes[3]) {
                  iter.remove();
                }
              }
            }
          }
        }

        private ByteBuffer computeKey(byte[] key)
        {
          final ByteBuffer retVal = ByteBuffer.allocate(key.length + 4).put(myIdBytes).put(key);
          retVal.rewind();
          return retVal;
        }
      };

      cacheCache.put(identifier, theCache);

      return theCache;
    }
  }
}
