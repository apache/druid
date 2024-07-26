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

package org.apache.druid.query.aggregation.datasketches.hll;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;

public class HllSketchMergeBufferAggregatorHelper
{
  private final int lgK;
  private final TgtHllType tgtHllType;
  private final int size;
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<Union>> unions = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();

  /**
   * Used by {@link #initializeEmptyUnion(ByteBuffer, int)}. We initialize by copying a prebuilt empty Union image.
   * {@link HllSketchBuildBufferAggregator} does something similar, but different enough that we don't share code. The
   * "build" flavor uses {@link HllSketch} objects and the "merge" flavor uses {@link Union} objects.
   */
  private final byte[] emptyUnion;

  public HllSketchMergeBufferAggregatorHelper(int lgK, TgtHllType tgtHllType, int size)
  {
    this.lgK = lgK;
    this.tgtHllType = tgtHllType;
    this.size = size;
    this.emptyUnion = new byte[size];

    //noinspection ResultOfObjectAllocationIgnored (Union writes to "emptyUnion" as a side effect of construction)
    new Union(lgK, WritableMemory.writableWrap(emptyUnion));
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#init} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#init}.
   */
  public void init(final ByteBuffer buf, final int position)
  {
    createNewUnion(buf, position, false);
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#get} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#get}.
   */
  public HllSketch get(ByteBuffer buf, int position)
  {
    final WritableMemory mem = WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN).writableRegion(position, size);
    final Union union = Union.writableWrap(mem);
    return union.getResult(tgtHllType);
  }

  public int getLgK()
  {
    return lgK;
  }

  public int getSize()
  {
    return size;
  }

  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    createNewUnion(newBuffer, newPosition, true);
    Int2ObjectMap<Union> unionMap = unions.get(oldBuffer);
    if (unionMap != null) {
      unionMap.remove(oldPosition);
      if (unionMap.isEmpty()) {
        unions.remove(oldBuffer);
        memCache.remove(oldBuffer);
      }
    }
  }

  public Union getOrCreateUnion(ByteBuffer buf, int position)
  {
    Int2ObjectMap<Union> unionMap = unions.get(buf);
    Union union = unionMap != null ? unionMap.get(position) : null;
    if (union != null) {
      return union;
    }
    return createNewUnion(buf, position, true);
  }

  private Union createNewUnion(ByteBuffer buf, int position, boolean isWrapped)
  {
    if (!isWrapped) {
      initializeEmptyUnion(buf, position);
    }

    final WritableMemory mem = getMemory(buf).writableRegion(position, size);
    Union union = Union.writableWrap(mem);

    Int2ObjectMap<Union> unionMap = unions.get(buf);
    if (unionMap == null) {
      unionMap = new Int2ObjectOpenHashMap<>();
      unions.put(buf, unionMap);
    }
    unionMap.put(position, union);
    return union;
  }

  /**
   * Copy prebuilt empty union object into the specified buffer and position
   */
  private void initializeEmptyUnion(ByteBuffer buf, int position)
  {
    final int oldPosition = buf.position();
    try {
      buf.position(position);
      buf.put(emptyUnion);
    }
    finally {
      buf.position(oldPosition);
    }
  }

  public void clear()
  {
    unions.clear();
    memCache.clear();
  }

  private WritableMemory getMemory(ByteBuffer buffer)
  {
    WritableMemory mem = memCache.get(buffer);
    if (mem == null) {
      mem = WritableMemory.writableWrap(buffer, ByteOrder.LITTLE_ENDIAN);
      memCache.put(buffer, mem);
    }
    return mem;
  }
}
