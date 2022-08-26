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

package org.apache.druid.query.aggregation.datasketches.kll;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.datasketches.kll.KllSketch;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;

abstract class KllSketchMergeBufferAggregatorHelper<SketchType extends KllSketch>
{
  private static final MemoryRequestServer MEM_REQ_SERVER = new DefaultMemoryRequestServer();
  private final int k;
  private final int maxIntermediateSize;
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<SketchType>> unions = new IdentityHashMap<>();

  KllSketchMergeBufferAggregatorHelper(
      final int k,
      final int maxIntermediateSize
  )
  {
    this.k = k;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  public void init(final ByteBuffer buffer, final int position)
  {
    final WritableMemory mem = getMemory(buffer);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final SketchType union = newDirectInstance(k, region, MEM_REQ_SERVER);
    putUnion(buffer, position, union);
  }

  public void clear()
  {
    unions.clear();
    memCache.clear();
  }

  // A small number of sketches may run out of the given memory, request more memory on heap and move there.
  // In that case we need to reuse the object from the cache as opposed to wrapping the new buffer.
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    SketchType union = unions.get(oldBuffer).get(oldPosition);
    final WritableMemory oldMem = getMemory(oldBuffer).writableRegion(oldPosition, maxIntermediateSize);
    if (union.isSameResource(oldMem)) { // union was not relocated on heap
      final WritableMemory newMem = getMemory(newBuffer).writableRegion(newPosition, maxIntermediateSize);
      union = writableWrap(newMem, MEM_REQ_SERVER);
    }
    putUnion(newBuffer, newPosition, union);

    Int2ObjectMap<SketchType> map = unions.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      unions.remove(oldBuffer);
      memCache.remove(oldBuffer);
    }
  }

  /**
   * Retrieves the sketch at a particular position. The returned sketch references the provided buffer.
   */
  public SketchType getSketchAtPosition(final ByteBuffer buf, final int position)
  {
    return unions.get(buf).get(position);
  }

  private WritableMemory getMemory(final ByteBuffer buffer)
  {
    return memCache.computeIfAbsent(buffer,
        buf -> WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN, MEM_REQ_SERVER));
  }

  private void putUnion(final ByteBuffer buffer, final int position, final SketchType union)
  {
    Int2ObjectMap<SketchType> map = unions.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, union);
  }

  abstract SketchType newDirectInstance(int k, WritableMemory mem, MemoryRequestServer reqServer);

  abstract SketchType writableWrap(WritableMemory mem, MemoryRequestServer reqServer);

  /**
   * Returns a copy of the union at the provided buffer position. The returned union does not reference the
   * provided buffer.
   */
  abstract SketchType get(ByteBuffer buffer, int position);
}
