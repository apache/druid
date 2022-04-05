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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.datasketches.memory.DefaultMemoryRequestServer;
import org.apache.datasketches.memory.MemoryRequestServer;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.quantiles.CompactDoublesSketch;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.UpdateDoublesSketch;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.IdentityHashMap;

public class DoublesSketchBuildBufferAggregatorHelper
{
  private static final MemoryRequestServer MEM_REQ_SERVER = new DefaultMemoryRequestServer();
  private final int size;
  private final int maxIntermediateSize;
  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<UpdateDoublesSketch>> sketches = new IdentityHashMap<>();

  public DoublesSketchBuildBufferAggregatorHelper(final int size, final int maxIntermediateSize)
  {
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  public void init(final ByteBuffer buffer, final int position)
  {
    final WritableMemory mem = getMemory(buffer);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final UpdateDoublesSketch sketch = DoublesSketch.builder().setK(size).build(region);
    putSketch(buffer, position, sketch);
  }

  public CompactDoublesSketch get(final ByteBuffer buffer, final int position)
  {
    return sketches.get(buffer).get(position).compact();
  }

  // A small number of sketches may run out of the given memory, request more memory on heap and move there.
  // In that case we need to reuse the object from the cache as opposed to wrapping the new buffer.
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    UpdateDoublesSketch sketch = sketches.get(oldBuffer).get(oldPosition);
    final WritableMemory oldRegion = getMemory(oldBuffer).writableRegion(oldPosition, maxIntermediateSize);
    if (sketch.isSameResource(oldRegion)) { // sketch was not relocated on heap
      final WritableMemory newRegion = getMemory(newBuffer).writableRegion(newPosition, maxIntermediateSize);
      sketch = UpdateDoublesSketch.wrap(newRegion);
    }
    putSketch(newBuffer, newPosition, sketch);

    final Int2ObjectMap<UpdateDoublesSketch> map = sketches.get(oldBuffer);
    map.remove(oldPosition);
    if (map.isEmpty()) {
      sketches.remove(oldBuffer);
      memCache.remove(oldBuffer);
    }
  }

  public void clear()
  {
    sketches.clear();
    memCache.clear();
  }

  /**
   * Retrieves the sketch at a particular position.
   */
  public UpdateDoublesSketch getSketchAtPosition(final ByteBuffer buf, final int position)
  {
    return sketches.get(buf).get(position);
  }

  private WritableMemory getMemory(final ByteBuffer buffer)
  {
    return memCache.computeIfAbsent(buffer,
        buf -> WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN, MEM_REQ_SERVER));
  }

  private void putSketch(final ByteBuffer buffer, final int position, final UpdateDoublesSketch sketch)
  {
    Int2ObjectMap<UpdateDoublesSketch> map = sketches.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, sketch);
  }
}
