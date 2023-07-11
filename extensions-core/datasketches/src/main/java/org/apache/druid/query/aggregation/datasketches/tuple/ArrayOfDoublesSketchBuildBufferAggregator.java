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

package org.apache.druid.query.aggregation.datasketches.tuple;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This aggregator builds sketches from raw data.
 * The input is in the form of a key and array of double values.
 * The output is {@link org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch}.
 */
public class ArrayOfDoublesSketchBuildBufferAggregator implements BufferAggregator
{
  private final DimensionSelector keySelector;
  private final BaseDoubleColumnValueSelector[] valueSelectors;
  private final int nominalEntries;
  private final int maxIntermediateSize;
  @Nullable
  private double[] values; // not part of the state, but to reuse in aggregate() method


  private final boolean canLookupUtf8;
  private final boolean canCacheById;
  private final LinkedHashMap<Integer, Object> stringCache = new LinkedHashMap<Integer, Object>()
  {
    @Override
    protected boolean removeEldestEntry(Map.Entry eldest)
    {
      return size() >= 10;
    }
  };

  public ArrayOfDoublesSketchBuildBufferAggregator(
      final DimensionSelector keySelector,
      final List<BaseDoubleColumnValueSelector> valueSelectors,
      int nominalEntries,
      int maxIntermediateSize
  )
  {
    this.keySelector = keySelector;
    this.valueSelectors = valueSelectors.toArray(new BaseDoubleColumnValueSelector[0]);
    this.nominalEntries = nominalEntries;
    this.maxIntermediateSize = maxIntermediateSize;
    values = new double[valueSelectors.size()];

    this.canCacheById = this.keySelector.nameLookupPossibleInAdvance();
    this.canLookupUtf8 = this.keySelector.supportsLookupNameUtf8();
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(nominalEntries)
                                              .setNumberOfValues(valueSelectors.length)
                                              .setNumberOfValues(valueSelectors.length).build(region);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < valueSelectors.length; i++) {
      if (valueSelectors[i].isNull()) {
        return;
      } else {
        values[i] = valueSelectors[i].getDouble();
      }
    }
    // Wrapping memory and ArrayOfDoublesSketch is inexpensive compared to sketch operations.
    // Maintaining a cache of wrapped objects per buffer position like in Theta sketch aggregator
    // might might be considered, but it would increase complexity including relocate() support.
    final WritableMemory mem = WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final ArrayOfDoublesUpdatableSketch sketch = ArrayOfDoublesSketches.wrapUpdatableSketch(region);
    final IndexedInts keys = keySelector.getRow();
    if (canLookupUtf8) {
      for (int i = 0, keysSize = keys.size(); i < keysSize; i++) {
        final ByteBuffer key;
        if (canCacheById) {
          key = (ByteBuffer) stringCache.computeIfAbsent(keys.get(i), keySelector::lookupNameUtf8);
        } else {
          key = keySelector.lookupNameUtf8(keys.get(i));
        }

        if (key != null) {
          byte[] bytes = new byte[key.remaining()];
          key.mark();
          key.get(bytes);
          key.reset();

          sketch.update(bytes, values);
        }
      }
    } else {
      for (int i = 0, keysSize = keys.size(); i < keysSize; i++) {
        final String key;
        if (canCacheById) {
          key = (String) stringCache.computeIfAbsent(keys.get(i), keySelector::lookupName);
        } else {
          key = keySelector.lookupName(keys.get(i));
        }

        sketch.update(key, values);
      }
    }
  }

  /**
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by consequent
   * aggregate() calls
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN);
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
    values = null;
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("keySelector", keySelector);
    inspector.visit("valueSelectors", valueSelectors);
  }
}
