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

package org.apache.druid.query.aggregation.exact.cardinality.bitmap64;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class Bitmap64ExactCardinalityBuildBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Bitmap64Counter>> collectors = new IdentityHashMap<>();

  public Bitmap64ExactCardinalityBuildBufferAggregator(BaseLongColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    createNewCollector(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final int oldPosition = buf.position();
    try {
      buf.position(position);
      Bitmap64Counter bitmap64Counter = getOrCreateCollector(buf, position);
      bitmap64Counter.add(selector.getLong());
    }
    finally {
      buf.position(oldPosition);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getOrCreateCollector(buf, position);
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

  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    createNewCollector(newBuffer, newPosition);
    Bitmap64Counter collector = collectors.get(oldBuffer).get(oldPosition);
    putCollectors(newBuffer, newPosition, collector);
    Int2ObjectMap<Bitmap64Counter> collectorMap = collectors.get(oldBuffer);
    if (collectorMap != null) {
      collectorMap.remove(oldPosition);
      if (collectorMap.isEmpty()) {
        collectors.remove(oldBuffer);
      }
    }
  }

  private void putCollectors(final ByteBuffer buffer, final int position, final Bitmap64Counter collector)
  {
    Int2ObjectMap<Bitmap64Counter> map = collectors.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, collector);
  }

  private Bitmap64Counter getOrCreateCollector(ByteBuffer buf, int position)
  {
    Int2ObjectMap<Bitmap64Counter> collectMap = collectors.get(buf);
    Bitmap64Counter bitmap64Counter = collectMap != null ? collectMap.get(position) : null;
    if (bitmap64Counter != null) {
      return bitmap64Counter;
    }

    return createNewCollector(buf, position);
  }

  private Bitmap64Counter createNewCollector(ByteBuffer buf, int position)
  {
    buf.position(position);
    Bitmap64Counter bitmap64Counter = new RoaringBitmap64Counter();
    Int2ObjectMap<Bitmap64Counter> collectorMap = collectors.computeIfAbsent(buf, k -> new Int2ObjectOpenHashMap<>());
    collectorMap.put(position, bitmap64Counter);
    return bitmap64Counter;
  }
}
