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

package io.druid.query.aggregation.unique;

import io.druid.java.util.common.UOE;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

public class UniqueBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Object> selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<MutableRoaringBitmap>> bitmapCache = new IdentityHashMap<>();

  public UniqueBufferAggregator(ColumnValueSelector<Object> selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    // TODO MutableRoaringBitmap 不支持offheap, 先用heap的
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    MutableRoaringBitmap mutableRoaringBitmap = bitmapCache.computeIfAbsent(buf, k -> new Int2ObjectOpenHashMap<>())
                                                           .computeIfAbsent(position, k -> new MutableRoaringBitmap());
    Object value = selector.getObject();
    if (value == null) {
      return;
    }
    if (value instanceof Integer) {
      mutableRoaringBitmap.add((int) value);
    } else if (value instanceof ImmutableRoaringBitmap) {
      ImmutableRoaringBitmap bitmap = (ImmutableRoaringBitmap) value;
      if (bitmap.getCardinality() <= 10) {
        IntIterator iterator = bitmap.getIntIterator();
        while (iterator.hasNext()) {
          mutableRoaringBitmap.add(iterator.next());
        }
      } else {
        mutableRoaringBitmap.or(bitmap);
      }
    } else {
      throw new UOE("Not implemented");
    }
  }

  @Override
  public ImmutableRoaringBitmap get(ByteBuffer buf, int position)
  {
    return bitmapCache.get(buf).get(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UOE("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UOE("Not implemented");
  }

  @Override
  public void close()
  {
    bitmapCache.clear();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
