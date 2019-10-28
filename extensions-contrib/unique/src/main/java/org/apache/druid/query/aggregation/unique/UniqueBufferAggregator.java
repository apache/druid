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

package org.apache.druid.query.aggregation.unique;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

public class UniqueBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Object> selector;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<List<ImmutableRoaringBitmap>>> bitmapCache = new IdentityHashMap<>();
  private final boolean useSortOr;

  public UniqueBufferAggregator(ColumnValueSelector<Object> selector, boolean useSortOr)
  {
    this.selector = selector;
    this.useSortOr = useSortOr;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    bitmapCache.computeIfAbsent(buf, k -> new Int2ObjectOpenHashMap<>())
               .computeIfAbsent(position, k -> new ArrayList<>());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object value = selector.getObject();
    if (value == null) {
      return;
    }

    if (value instanceof ImmutableRoaringBitmap) {
      List<ImmutableRoaringBitmap> bitmaps = bitmapCache.get(buf)
                                                        .get(position);
      ImmutableRoaringBitmap bitmap = (ImmutableRoaringBitmap) value;
      bitmaps.add(bitmap);
    } else {
      throw new UOE("Not implemented");
    }
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    List<ImmutableRoaringBitmap> bitmaps = bitmapCache.get(oldBuffer).get(oldPosition);

    Int2ObjectMap<List<ImmutableRoaringBitmap>> map = bitmapCache.computeIfAbsent(newBuffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(newPosition, bitmaps);

    map = bitmapCache.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      bitmapCache.remove(oldBuffer);
    }
  }

  @Override
  public ImmutableRoaringBitmap get(ByteBuffer buf, int position)
  {
    Int2ObjectMap<List<ImmutableRoaringBitmap>> c = bitmapCache.get(buf);
    if (c == null) {
      return new MutableRoaringBitmap();
    }
    List<ImmutableRoaringBitmap> bitmaps = c.get(position);
    if (bitmaps == null || bitmaps.isEmpty()) {
      return new MutableRoaringBitmap();
    }
    if (useSortOr) {
      return UniqueBuildAggregator.batchOr(bitmaps);
    } else {
      return ImmutableRoaringBitmap.or(bitmaps.toArray(new ImmutableRoaringBitmap[0]));
    }
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
