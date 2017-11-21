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

package io.druid.query.aggregation.datasketches.quantiles;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class DoublesSketchBuildBufferAggregator implements BufferAggregator
{

  private final ColumnValueSelector<Double> selector;
  private final int size;
  private final int maxIntermediateSize;

  private final IdentityHashMap<ByteBuffer, WritableMemory> memCache = new IdentityHashMap<>();
  private final IdentityHashMap<ByteBuffer, Int2ObjectMap<UpdateDoublesSketch>> sketches = new IdentityHashMap<>();

  public DoublesSketchBuildBufferAggregator(final ColumnValueSelector<Double> valueSelector, final int size,
      final int maxIntermediateSize)
  {
    this.selector = valueSelector;
    this.size = size;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public synchronized void init(final ByteBuffer buffer, final int position)
  {
    final WritableMemory mem = getMemory(buffer);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final UpdateDoublesSketch sketch = UpdateDoublesSketch.builder().setK(size).build(region);
    putSketch(buffer, position, sketch);
  }

  @Override
  public synchronized void aggregate(final ByteBuffer buffer, final int position)
  {
    final UpdateDoublesSketch sketch = sketches.get(buffer).get(position);
    sketch.update(selector.getDouble());
  }

  @Override
  public synchronized Object get(final ByteBuffer buffer, final int position)
  {
    return sketches.get(buffer).get(position);
  }

  @Override
  public float getFloat(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buffer, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public synchronized void close()
  {
    sketches.clear();
    memCache.clear();
  }

  // A small number of sketches may run out of the given memory, request more memory on heap and move there.
  // In that case we need to reuse the object from the cache as opposed to wrapping the new buffer.
  @Override
  public synchronized void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
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

  private WritableMemory getMemory(final ByteBuffer buffer)
  {
    return memCache.computeIfAbsent(buffer, buf -> WritableMemory.wrap(buf));
  }

  private void putSketch(final ByteBuffer buffer, final int position, final UpdateDoublesSketch sketch)
  {
    Int2ObjectMap<UpdateDoublesSketch> map = sketches.computeIfAbsent(buffer, buf -> new Int2ObjectOpenHashMap<>());
    map.put(position, sketch);
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }

}
