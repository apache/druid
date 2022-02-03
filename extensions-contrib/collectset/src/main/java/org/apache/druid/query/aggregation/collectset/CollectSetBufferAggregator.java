/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.collectset;

import com.google.common.util.concurrent.Striped;
import gnu.trove.set.hash.THashSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class CollectSetBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Object> selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Set<Object>>> setCache = new IdentityHashMap<>();
  private int limit;

  /** for locking per buffer position (power of 2 to make index computation faster) */
  private static final int NUM_STRIPES = 64;
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(NUM_STRIPES);

  public CollectSetBufferAggregator(
      ColumnValueSelector<Object> selector,
      int limit
  )
  {
    this.selector = selector;
    this.limit = limit;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    putSetIntoCache(buf, position, new THashSet<>());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object value = selector.getObject();
    if (value == null) {
      return;
    }
    final Lock lock = stripedLock.getAt(lockIndex(position)).writeLock();
    lock.lock();
    try {
      final Set<Object> set = setCache.get(buf).get(position);


      if (limit >= 0 && set.size() >= limit) {
        return;
      }

      if (value instanceof Collection) {
        Collection<?> valueCollection = (Collection<?>) value;
        CollectSetUtil.addCollectionWithLimit(set, valueCollection, limit);
      } else {
        set.add(value);
      }
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final Lock lock = stripedLock.getAt(lockIndex(position)).readLock();
    lock.lock();
    try {
      return new THashSet<>(setCache.get(buf).get(position));
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    setCache.clear();
  }

  @Override
  public void relocate(final int oldPosition, final int newPosition, final ByteBuffer oldBuf, final ByteBuffer newBuf)
  {
    Set<Object> set = setCache.get(oldBuf).get(oldPosition);
    putSetIntoCache(newBuf, newPosition, set);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }

  private void putSetIntoCache(final ByteBuffer buf, final int position, final Set<Object> set)
  {
    final Int2ObjectMap<Set<Object>> map = setCache.computeIfAbsent(buf, b -> new Int2ObjectOpenHashMap<>());
    map.put(position, set);
  }

  /**
   * compute lock index to avoid boxing in Striped.get() call
   * @param position
   * @return index
   */
  static int lockIndex(final int position)
  {
    return smear(position) % NUM_STRIPES;
  }

  /**
   * see https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/Striped.java#L536-L548
   * @param hashCode
   * @return smeared hashCode
   */
  private static int smear(int hashCode)
  {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }
}
