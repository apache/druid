/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.client.cache;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;

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
  public static Cache create(long sizeInBytes)
  {
    return new MapCache(new ByteCountingLRUMap(sizeInBytes));
  }

  private final Map<ByteBuffer, byte[]> baseMap;
  private final ByteCountingLRUMap byteCountingLRUMap;

  private final Map<String, byte[]> namespaceId;
  private final AtomicInteger ids;

  private final Object clearLock = new Object();

  private final AtomicLong hitCount = new AtomicLong(0);
  private final AtomicLong missCount = new AtomicLong(0);

  MapCache(
      ByteCountingLRUMap byteCountingLRUMap
  )
  {
    this.byteCountingLRUMap = byteCountingLRUMap;
    this.baseMap = Collections.synchronizedMap(byteCountingLRUMap);

    namespaceId = Maps.newHashMap();
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
        0,
        0
    );
  }

  @Override
  public byte[] get(NamedKey key)
  {
    final byte[] retVal;
    synchronized (clearLock) {
      retVal = baseMap.get(computeKey(getNamespaceId(key.namespace), key.key));
    }
    if (retVal == null) {
      missCount.incrementAndGet();
    } else {
      hitCount.incrementAndGet();
    }
    return retVal;
  }

  @Override
  public void put(NamedKey key, byte[] value)
  {
    synchronized (clearLock) {
      baseMap.put(computeKey(getNamespaceId(key.namespace), key.key), value);
    }
  }

  @Override
  public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
  {
    Map<NamedKey, byte[]> retVal = Maps.newHashMap();
    for (NamedKey key : keys) {
      retVal.put(key, get(key));
    }
    return retVal;
  }

  @Override
  public void close(String namespace)
  {
    byte[] idBytes;
    synchronized (namespaceId) {
      idBytes = getNamespaceId(namespace);
      if (idBytes == null) {
        return;
      }

      namespaceId.remove(namespace);
    }
    synchronized (clearLock) {
      Iterator<ByteBuffer> iter = baseMap.keySet().iterator();
      while (iter.hasNext()) {
        ByteBuffer next = iter.next();

        if (next.get(0) == idBytes[0]
            && next.get(1) == idBytes[1]
            && next.get(2) == idBytes[2]
            && next.get(3) == idBytes[3]) {
          iter.remove();
        }
      }
    }
  }

  private byte[] getNamespaceId(final String identifier)
  {
    synchronized (namespaceId) {
      byte[] idBytes = namespaceId.get(identifier);
      if (idBytes != null) {
        return idBytes;
      }

      idBytes = Ints.toByteArray(ids.getAndIncrement());
      namespaceId.put(identifier, idBytes);
      return idBytes;
    }
  }

  private ByteBuffer computeKey(byte[] idBytes, byte[] key)
  {
    final ByteBuffer retVal = ByteBuffer.allocate(key.length + 4).put(idBytes).put(key);
    retVal.rewind();
    return retVal;
  }

  public boolean isLocal()
  {
    return true;
  }
}
