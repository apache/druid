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

package org.apache.druid.query.aggregation.distinctcount2;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.collections.bitmap.BitSetBitmapFactory;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;

public class DistinctCount2BufferAggregator implements BufferAggregator
{
  private final BitmapFactory bitmapFactory = new BitSetBitmapFactory();
  private final DimensionSelector selector;
  private final Int2ObjectMap<MutableBitmap> mutableBitmapCollection = new Int2ObjectOpenHashMap<>();

  private final int lgK;
  private final TgtHllType tgtHllType;

  public DistinctCount2BufferAggregator(
      DimensionSelector selector,
      Integer lgK,
      TgtHllType tgtHllType
  )
  {
    this.selector = selector;
    this.lgK = lgK == null ? 12 : lgK;
    this.tgtHllType = tgtHllType == null ? TgtHllType.HLL_4 : tgtHllType;

  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.position(position);
    new RoaringBitmap().serialize(buf);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    MutableBitmap mutableBitmap = getMutableBitmap(position);
    IndexedInts row = selector.getRow();
    for (int i = 0, rowSize = row.size(); i < rowSize; i++) {
      int index = row.get(i);
      mutableBitmap.add(index);
    }
  }


  private MutableBitmap getMutableBitmap(int position)
  {
    MutableBitmap mutableBitmap = mutableBitmapCollection.get(position);
    if (mutableBitmap == null) {
      mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
      mutableBitmapCollection.put(position, mutableBitmap);
    }
    return mutableBitmap;
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    MutableBitmap mutableBitmap = getMutableBitmap(position);
    IntIterator iterator = mutableBitmap.iterator();
    HllSketch sketch = new HllSketch(this.lgK, tgtHllType);
    while (iterator.hasNext()) {
      int i = iterator.next();
      ByteBuffer b = selector.lookupNameUtf8(i);
      sketch.update(b);
    }
    return HllSketchHolder.of(sketch.copy());
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

  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
