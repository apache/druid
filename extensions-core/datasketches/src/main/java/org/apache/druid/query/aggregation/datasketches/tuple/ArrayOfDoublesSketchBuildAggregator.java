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

import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * This aggregator builds sketches from raw data.
 * The input is in the form of a key and array of double values.
 * The output is {@link org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch}.
 */
public class ArrayOfDoublesSketchBuildAggregator implements Aggregator
{

  private final DimensionSelector keySelector;
  private final BaseDoubleColumnValueSelector[] valueSelectors;
  @Nullable
  private double[] values; // not part of the state, but to reuse in aggregate() method
  @Nullable
  private ArrayOfDoublesUpdatableSketch sketch;

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

  public ArrayOfDoublesSketchBuildAggregator(
      final DimensionSelector keySelector,
      final List<BaseDoubleColumnValueSelector> valueSelectors,
      final int nominalEntries
  )
  {
    this.keySelector = keySelector;
    this.valueSelectors = valueSelectors.toArray(new BaseDoubleColumnValueSelector[0]);
    values = new double[valueSelectors.size()];
    sketch = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(nominalEntries)
                                                       .setNumberOfValues(valueSelectors.size()).build();

    this.canCacheById = this.keySelector.nameLookupPossibleInAdvance();
    this.canLookupUtf8 = this.keySelector.supportsLookupNameUtf8();
  }

  /**
   * This method uses synchronization because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/druid/pull/3956
   */
  @Override
  public void aggregate()
  {
    final IndexedInts keys = keySelector.getRow();
    for (int i = 0; i < valueSelectors.length; i++) {
      if (valueSelectors[i].isNull()) {
        return;
      } else {
        values[i] = valueSelectors[i].getDouble();
      }
    }
    synchronized (this) {
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
  }

  /**
   * This method uses synchronization because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/druid/pull/3956
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by consequent
   * aggregate() calls
   */
  @Override
  public synchronized Object get()
  {
    return sketch.compact();
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
    sketch = null;
    values = null;
  }

}
