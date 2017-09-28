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

package io.druid.query.aggregation.distinctcount;

import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.WrappedRoaringBitmap;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.nio.ByteBuffer;

public class DistinctCountBufferAggregator implements BufferAggregator
{
  private final DimensionSelector selector;
  private final Int2ObjectMap<MutableBitmap> mutableBitmapCollection = new Int2ObjectOpenHashMap<>();

  public DistinctCountBufferAggregator(
      DimensionSelector selector
  )
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0L);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    MutableBitmap mutableBitmap = getMutableBitmap(buf, position);
    IndexedInts row = selector.getRow();
    for (int i = 0; i < row.size(); i++) {
      int index = row.get(i);
      mutableBitmap.add(index);
    }
    buf.putLong(position, mutableBitmap.size());
  }

  private MutableBitmap getMutableBitmap(ByteBuffer buf, int position)
  {
    MutableBitmap mutableBitmap = mutableBitmapCollection.get(position);
    if (mutableBitmap == null) {
      mutableBitmap = new WrappedRoaringBitmap();
      mutableBitmapCollection.put(position, mutableBitmap);
    }
    return mutableBitmap;
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getLong(position);
  }

  @Override
  public void close()
  {
    mutableBitmapCollection.clear();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
