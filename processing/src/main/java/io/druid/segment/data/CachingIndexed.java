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

package io.druid.segment.data;

import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class CachingIndexed<T> implements Indexed<T>, Closeable
{
  public static final int INITIAL_CACHE_CAPACITY = 16384;

  private static final Logger log = new Logger(CachingIndexed.class);

  private final GenericIndexed<T>.BufferIndexed delegate;
  private final SizedLRUMap<Integer, T> cachedValues;

  /**
   * Creates a CachingIndexed wrapping the given GenericIndexed with a value lookup cache
   *
   * CachingIndexed objects are not thread safe and should only be used by a single thread at a time.
   * CachingIndexed objects must be closed to release any underlying cache resources.
   *
   * @param delegate the GenericIndexed to wrap with a lookup cache.
   * @param lookupCacheSize maximum size in bytes of the lookup cache if greater than zero
   */
  public CachingIndexed(GenericIndexed<T> delegate, final int lookupCacheSize)
  {
    this.delegate = delegate.singleThreaded();

    if(lookupCacheSize > 0) {
      log.debug("Allocating column cache of max size[%d]", lookupCacheSize);
      cachedValues = new SizedLRUMap<>(INITIAL_CACHE_CAPACITY, lookupCacheSize);
    } else {
      cachedValues = null;
    }
  }

  @Override
  public Class<? extends T> getClazz()
  {
    return delegate.getClazz();
  }

  @Override
  public int size()
  {
    return delegate.size();
  }

  @Override
  public T get(int index)
  {
    if(cachedValues != null) {
      final T cached = cachedValues.getValue(index);
      if (cached != null) {
        return cached;
      }

      final T value = delegate.get(index);
      cachedValues.put(index, value, delegate.getLastValueSize());
      return value;
    } else {
      return delegate.get(index);
    }
  }

  @Override
  public int indexOf(T value)
  {
    return delegate.indexOf(value);
  }

  @Override
  public Iterator<T> iterator()
  {
    return delegate.iterator();
  }

  @Override
  public void close() throws IOException
  {
    if (cachedValues != null) {
      log.debug("Closing column cache");
      cachedValues.clear();
    }
  }

private static class SizedLRUMap<K, V> extends LinkedHashMap<K, Pair<Integer, V>>
  {
    private final int maxBytes;
    private int numBytes = 0;

    public SizedLRUMap(int initialCapacity, int maxBytes)
    {
      super(initialCapacity, 0.75f, true);
      this.maxBytes = maxBytes;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, Pair<Integer, V>> eldest)
    {
      if (numBytes > maxBytes) {
        numBytes -= eldest.getValue().lhs;
        return true;
      }
      return false;
    }

    public void put(K key, V value, int size)
    {
      final int totalSize = size + 48; // add approximate object overhead
      numBytes += totalSize;
      super.put(key, new Pair<>(totalSize, value));
    }

    public V getValue(Object key)
    {
      final Pair<Integer, V> sizeValuePair = super.get(key);
      return sizeValuePair == null ? null : sizeValuePair.rhs;
    }
  }
}
