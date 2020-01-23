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
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.tuple.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.ArrayOfDoublesUnion;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This aggregator merges existing sketches.
 * The input column contains ArrayOfDoublesSketch.
 * The output is {@link ArrayOfDoublesSketch} that is a union of the input sketches.
 */
public class ArrayOfDoublesSketchMergeBufferAggregator implements BufferAggregator
{

  private static final int NUM_STRIPES = 64; // for locking per buffer position

  private final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector;
  private final int nominalEntries;
  private final int numberOfValues;
  private final int maxIntermediateSize;
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(NUM_STRIPES);

  public ArrayOfDoublesSketchMergeBufferAggregator(
      final BaseObjectColumnValueSelector<ArrayOfDoublesSketch> selector,
      final int nominalEntries,
      final int numberOfValues,
      final int maxIntermediateSize
  )
  {
    this.selector = selector;
    this.nominalEntries = nominalEntries;
    this.numberOfValues = numberOfValues;
    this.maxIntermediateSize = maxIntermediateSize;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    new ArrayOfDoublesSetOperationBuilder().setNominalEntries(nominalEntries)
        .setNumberOfValues(numberOfValues).buildUnion(region);
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/druid/pull/3956
   */
  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final ArrayOfDoublesSketch update = selector.getObject();
    if (update == null) {
      return;
    }
    // Wrapping memory and ArrayOfDoublesUnion is inexpensive compared to union operations.
    // Maintaining a cache of wrapped objects per buffer position like in Theta sketch aggregator
    // might might be considered, but it would increase complexity including relocate() support.
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final Lock lock = stripedLock.getAt(ArrayOfDoublesSketchBuildBufferAggregator.lockIndex(position)).writeLock();
    lock.lock();
    try {
      final ArrayOfDoublesUnion union = ArrayOfDoublesSketches.wrapUnion(region);
      union.update(update);
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * https://github.com/apache/druid/pull/3956
   * The returned sketch is a separate instance of ArrayOfDoublesCompactSketch
   * representing the current state of the aggregation, and is not affected by consequent
   * aggregate() calls
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN);
    final WritableMemory region = mem.writableRegion(position, maxIntermediateSize);
    final Lock lock = stripedLock.getAt(ArrayOfDoublesSketchBuildBufferAggregator.lockIndex(position)).readLock();
    lock.lock();
    try {
      final ArrayOfDoublesUnion union = ArrayOfDoublesSketches.wrapUnion(region);
      return union.getResult();
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
  }

  @Override
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }

}
