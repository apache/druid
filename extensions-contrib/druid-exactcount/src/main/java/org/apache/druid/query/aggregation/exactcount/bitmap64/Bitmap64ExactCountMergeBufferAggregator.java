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

package org.apache.druid.query.aggregation.exactcount.bitmap64;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class Bitmap64ExactCountMergeBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Bitmap64Counter> selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Bitmap64Counter>> counterCache = new IdentityHashMap<>();

  public Bitmap64ExactCountMergeBufferAggregator(ColumnValueSelector<Bitmap64Counter> selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    RoaringBitmap64Counter emptyCounter = new RoaringBitmap64Counter();
    addToCache(buf, position, emptyCounter);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object x = selector.getObject();
    if (x == null) {
      return;
    }
    Bitmap64Counter bitmap64Counter = counterCache.get(buf).get(position);
    bitmap64Counter.fold((RoaringBitmap64Counter) x);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return counterCache.get(buf).get(position);
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
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    counterCache.clear();
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    Bitmap64Counter counter = counterCache.get(oldBuffer).get(oldPosition);
    addToCache(newBuffer, newPosition, counter);
    Int2ObjectMap<Bitmap64Counter> counterMap = counterCache.get(oldBuffer);
    if (counterMap != null) {
      counterMap.remove(oldPosition);
      if (counterMap.isEmpty()) {
        counterCache.remove(oldBuffer);
      }
    }
  }

  private void addToCache(final ByteBuffer buffer, final int position, final Bitmap64Counter counter)
  {
    Int2ObjectMap<Bitmap64Counter> map = counterCache.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, counter);
  }
}
