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
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;

public class HllSketchBuildBufferAggregatorHelper
{
  private static final MemoryRequestServer MEM_REQ_SERVER = new DefaultMemoryRequestServer();
  private final int lgK;
  private final int size;
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<HllSketch>> sketchCache = new IdentityHashMap<>();

  /**
   * Used by {@link #init(ByteBuffer, int)}. We initialize by copying a prebuilt empty HllSketch image.
   * {@link HllSketchMergeBufferAggregator} does something similar, but different enough that we don't share code. The
   * "build" flavor uses {@link HllSketch} objects and the "merge" flavor uses {@link org.apache.datasketches.hll.Union} objects.
   */
  private final byte[] emptySketch;

  public HllSketchBuildBufferAggregatorHelper(final int lgK, final TgtHllType tgtHllType, final int size)
  {
    this.lgK = lgK;
    this.size = size;
    this.emptySketch = new byte[size];

    //noinspection ResultOfObjectAllocationIgnored (HllSketch writes to "emptySketch" as a side effect of construction)
    new HllSketch(lgK, tgtHllType, WritableMemory.writableWrap(emptySketch));
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#init} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#init}.
   */
  public void init(final ByteBuffer buf, final int position)
  {
    // Copy prebuilt empty sketch object.

    final int oldPosition = buf.position();
    try {
      buf.position(position);
      buf.put(emptySketch);
    }
    finally {
      buf.position(oldPosition);
    }

    // Add an HllSketch for this chunk to our sketchCache.
    final WritableMemory mem = getMemory(buf).writableRegion(position, size);
    putSketchIntoCache(buf, position, HllSketch.writableWrap(mem));
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#get} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#get}.
   */
  public Object get(ByteBuffer buf, int position)
  {
    return sketchCache.get(buf).get(position).copy();
  }

  /**
   * Helper for implementing {@link org.apache.druid.query.aggregation.BufferAggregator#relocate} and
   * {@link org.apache.druid.query.aggregation.VectorAggregator#relocate}.
   */
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuf, ByteBuffer newBuf)
  {
    HllSketch sketch = sketchCache.get(oldBuf).get(oldPosition);
    final WritableMemory oldMem = getMemory(oldBuf).writableRegion(oldPosition, size);
    if (sketch.isSameResource(oldMem)) { // sketch has not moved
      final WritableMemory newMem = getMemory(newBuf).writableRegion(newPosition, size);
      sketch = HllSketch.writableWrap(newMem);
    }
    putSketchIntoCache(newBuf, newPosition, sketch);
  }

  /**
   * Retrieves the sketch at a particular position.
   */
  public HllSketch getSketchAtPosition(final ByteBuffer buf, final int position)
  {
    return sketchCache.get(buf).get(position);
  }

  /**
   * Clean up resources used by this helper.
   */
  public void clear()
  {
    memCache.clear();
    sketchCache.clear();
  }

  public int getLgK()
  {
    return lgK;
  }

  private WritableMemory getMemory(final ByteBuffer buf)
  {
    return memCache.computeIfAbsent(buf,
        b -> WritableMemory.writableWrap(b, ByteOrder.LITTLE_ENDIAN, MEM_REQ_SERVER));
  }

  private void putSketchIntoCache(final ByteBuffer buf, final int position, final HllSketch sketch)
  {
    final Int2ObjectMap<HllSketch> map = sketchCache.computeIfAbsent(buf, b -> new Int2ObjectOpenHashMap<>());
    map.put(position, sketch);
  }
}
