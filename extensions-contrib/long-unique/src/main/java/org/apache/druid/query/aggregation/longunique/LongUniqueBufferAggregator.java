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

package org.apache.druid.query.aggregation.longunique;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

public class LongUniqueBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Object> selector;
  private final IdentityHashMap<ByteBuffer, Long2ObjectMap<List<Roaring64NavigableMap>>> bitmapCache = new IdentityHashMap<>();

  public LongUniqueBufferAggregator(ColumnValueSelector<Object> selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    bitmapCache.computeIfAbsent(buf, k -> new Long2ObjectOpenHashMap<>())
               .computeIfAbsent(position, k -> new ArrayList<>());
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object value = selector.getObject();
    if (value == null) {
      return;
    }
    List<Roaring64NavigableMap> bitmaps = bitmapCache.get(buf).get(position);
    Roaring64NavigableMap bitmap = (Roaring64NavigableMap) value;
    bitmaps.add(bitmap);
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    List<Roaring64NavigableMap> bitmaps = bitmapCache.get(oldBuffer).get(oldPosition);

    Long2ObjectMap<List<Roaring64NavigableMap>> map =
            bitmapCache.computeIfAbsent(newBuffer, buf -> new Long2ObjectOpenHashMap<>());
    map.put(newPosition, bitmaps);

    map = bitmapCache.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      bitmapCache.remove(oldBuffer);
    }
  }

  @Override
  public Roaring64NavigableMap get(ByteBuffer buf, int position)
  {
    Long2ObjectMap<List<Roaring64NavigableMap>> c = bitmapCache.get(buf);
    Roaring64NavigableMap roaring64NavigableMap = new Roaring64NavigableMap();
    if (c == null) {
      return roaring64NavigableMap;
    }
    List<Roaring64NavigableMap> bitmaps = c.get(position);
    if (bitmaps == null || bitmaps.isEmpty()) {
      return roaring64NavigableMap;
    }
    for (int i = 0; i < bitmaps.size(); i++) {
      roaring64NavigableMap.or(bitmaps.get(i));
    }
    return roaring64NavigableMap;
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
