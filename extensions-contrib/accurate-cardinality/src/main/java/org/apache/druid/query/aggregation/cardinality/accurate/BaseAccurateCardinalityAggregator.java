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

package org.apache.druid.query.aggregation.cardinality.accurate;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public abstract class BaseAccurateCardinalityAggregator<TSelector>
    implements BufferAggregator, Aggregator
{
  protected final TSelector selector;
  protected final IdentityHashMap<ByteBuffer, Int2ObjectMap<LongBitmapCollector>> collectors = new IdentityHashMap<>();

  protected final LongBitmapCollectorFactory longBitmapCollectorFactory;
  private final ByteBuffer defaultByteBuffer;

  public BaseAccurateCardinalityAggregator(
      TSelector selector,
      LongBitmapCollectorFactory longBitmapCollectorFactory,
      boolean onHeap
  )
  {
    this.selector = selector;
    this.longBitmapCollectorFactory = longBitmapCollectorFactory;
    if (onHeap) {
      defaultByteBuffer = ByteBuffer.allocate(1);
    } else {
      defaultByteBuffer = null;
    }
  }

  abstract void collectorAdd(LongBitmapCollector longBitmapCollector);

  @Override
  public void aggregate()
  {
    aggregate(defaultByteBuffer, 0);
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
      LongBitmapCollector longBitmapCollector = getOrCreateCollector(buf, position);
      collectorAdd(longBitmapCollector);
    }
    finally {
      buf.position(oldPosition);
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    return getOrCreateCollector(defaultByteBuffer, 0);
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getLong()");
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("BloomFilterAggregator does not support getFloat()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getDouble()");
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getOrCreateCollector(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getDouble()");
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("AccurateCardinalityAggregator does not support getFloat()");
  }

  @Override
  public void close()
  {

  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    createNewCollector(newBuffer, newPosition);
    LongBitmapCollector collector = collectors.get(oldBuffer).get(oldPosition);
    putCollectors(newBuffer, newPosition, collector);
    Int2ObjectMap<LongBitmapCollector> collectorMap = collectors.get(oldBuffer);
    if (collectorMap != null) {
      collectorMap.remove(oldPosition);
      if (collectorMap.isEmpty()) {
        collectors.remove(oldBuffer);
      }
    }
  }

  private void putCollectors(final ByteBuffer buffer, final int position, final LongBitmapCollector collector)
  {
    Int2ObjectMap<LongBitmapCollector> map = collectors.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, collector);
  }

  private LongBitmapCollector getOrCreateCollector(ByteBuffer buf, int position)
  {
    Int2ObjectMap<LongBitmapCollector> collectMap = collectors.get(buf);
    LongBitmapCollector longBitmapCollector = collectMap != null ? collectMap.get(position) : null;
    if (longBitmapCollector != null) {
      return longBitmapCollector;
    }

    return createNewCollector(buf, position);
  }

  private LongBitmapCollector createNewCollector(ByteBuffer buf, int position)
  {
    buf.position(position);
    LongBitmapCollector longBitmapCollector = longBitmapCollectorFactory.makeEmptyCollector();
    Int2ObjectMap<LongBitmapCollector> collectorMap = collectors.get(buf);
    if (collectorMap == null) {
      collectorMap = new Int2ObjectOpenHashMap<>();
      collectors.put(buf, collectorMap);
    }
    collectorMap.put(position, longBitmapCollector);
    return longBitmapCollector;
  }
}
