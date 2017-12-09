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

package io.druid.query.aggregation.datasketches.tuple;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseDoubleColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ArrayOfDoublesSketchBuildBufferAggregator implements BufferAggregator
{

  private final DimensionSelector keySelector;
  private final List<BaseDoubleColumnValueSelector> valueSelectors;
  private final int nominalEntries;
  private final int maxIntermediateSize;

  public ArrayOfDoublesSketchBuildBufferAggregator(final DimensionSelector keySelector,
      final List<BaseDoubleColumnValueSelector> valueSelectors, int nominalEntries, int maxIntermediateSize)
  {
    this.keySelector = keySelector;
    this.valueSelectors = valueSelectors;
    this.nominalEntries = nominalEntries;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(nominalEntries)
        .setNumberOfValues(valueSelectors.size())
        .setNumberOfValues(valueSelectors.size()).build(region);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    try (final IndexedInts keys = keySelector.getRow()) {
      if (keys == null) {
        return;
      }
      final WritableMemory mem = WritableMemory.wrap(buf);
      final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
      final ArrayOfDoublesUpdatableSketch sketch = ArrayOfDoublesSketches.wrapUpdatableSketch(region);
      final double[] values = new double[valueSelectors.size()];
      int valueIndex = 0;
      for (final BaseDoubleColumnValueSelector valueSelector : valueSelectors) {
        values[valueIndex++] = valueSelector.getDouble();
      }
      for (int i = 0; i < keys.size(); i++) {
        final String key = keySelector.lookupName(keys.get(i));
        sketch.update(key, values);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final ArrayOfDoublesUpdatableSketch sketch = (ArrayOfDoublesUpdatableSketch) ArrayOfDoublesSketches
        .wrapSketch(region);
    return sketch.compact();
  }

  @Override
  public float getFloat(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(final ByteBuffer buf, final int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("keyselector", keySelector);
    inspector.visit("valueselectors", valueSelectors.toArray());
  }

}
