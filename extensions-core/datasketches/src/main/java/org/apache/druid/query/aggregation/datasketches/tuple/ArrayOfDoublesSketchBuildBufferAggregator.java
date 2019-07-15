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

import com.google.common.util.concurrent.Striped;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.tuple.ArrayOfDoublesSketches;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketch;
import com.yahoo.sketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This aggregator builds sketches from raw data.
 * The input is in the form of a key and array of double values.
 * The output is {@link com.yahoo.sketches.tuple.ArrayOfDoublesSketch}.
 */
public class ArrayOfDoublesSketchBuildBufferAggregator implements BufferAggregator
{

  private static final int NUM_STRIPES = 64; // for locking per buffer position (power of 2 to make index computation faster)

  private final DimensionSelector keySelector;
  private final BaseDoubleColumnValueSelector[] valueSelectors;
  private final int nominalEntries;
  private final int maxIntermediateSize;
  @Nullable
  private double[] values; // not part of the state, but to reuse in aggregate() method
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(NUM_STRIPES);

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
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(nominalEntries)
        .setNumberOfValues(valueSelectors.length)
        .setNumberOfValues(valueSelectors.length).build(region);
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/incubator-druid/pull/3956
   */
  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < valueSelectors.length; i++) {
      values[i] = valueSelectors[i].getDouble();
    }
    final IndexedInts keys = keySelector.getRow();
    // Wrapping memory and ArrayOfDoublesSketch is inexpensive compared to sketch operations.
    // Maintaining a cache of wrapped objects per buffer position like in Theta sketch aggregator
    // might might be considered, but it would increase complexity including relocate() support.
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final Lock lock = stripedLock.getAt(lockIndex(position)).writeLock();
    lock.lock();
    try {
      final ArrayOfDoublesUpdatableSketch sketch = ArrayOfDoublesSketches.wrapUpdatableSketch(region);
      for (int i = 0, keysSize = keys.size(); i < keysSize; i++) {
        final String key = keySelector.lookupName(keys.get(i));
        sketch.update(key, values);
      }
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/incubator-druid/pull/3956
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by consequent
   * aggregate() calls
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final Lock lock = stripedLock.getAt(lockIndex(position)).readLock();
    lock.lock();
    try {
      final ArrayOfDoublesUpdatableSketch sketch = (ArrayOfDoublesUpdatableSketch) ArrayOfDoublesSketches
          .wrapSketch(region);
      return sketch.compact();
    }
    finally {
      lock.unlock();
    }
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

  // compute lock index to avoid boxing in Striped.get() call
  static int lockIndex(final int position)
  {
    return smear(position) % NUM_STRIPES;
  }

  // from https://github.com/google/guava/blob/master/guava/src/com/google/common/util/concurrent/Striped.java#L536-L548
  private static int smear(int hashCode)
  {
    hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
    return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
  }

}
