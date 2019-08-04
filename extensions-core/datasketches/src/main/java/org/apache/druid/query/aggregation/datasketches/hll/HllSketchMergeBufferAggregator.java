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

import com.google.common.util.concurrent.Striped;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * This aggregator merges existing sketches.
 * The input column must contain {@link HllSketch}
 */
public class HllSketchMergeBufferAggregator implements BufferAggregator
{

  /**
   * for locking per buffer position (power of 2 to make index computation faster)
   */
  private static final int NUM_STRIPES = 64;

  private final ColumnValueSelector<HllSketch> selector;
  private final int lgK;
  private final TgtHllType tgtHllType;
  private final int size;
  private final Striped<ReadWriteLock> stripedLock = Striped.readWriteLock(NUM_STRIPES);

  /**
   * Used by {@link #init(ByteBuffer, int)}. We initialize by copying a prebuilt empty Union image.
   * {@link HllSketchBuildBufferAggregator} does something similar, but different enough that we don't share code. The
   * "build" flavor uses {@link HllSketch} objects and the "merge" flavor uses {@link Union} objects.
   */
  private final byte[] emptyUnion;

  public HllSketchMergeBufferAggregator(
      final ColumnValueSelector<HllSketch> selector,
      final int lgK,
      final TgtHllType tgtHllType,
      final int size
  )
  {
    this.selector = selector;
    this.lgK = lgK;
    this.tgtHllType = tgtHllType;
    this.size = size;
    this.emptyUnion = new byte[size];

    //noinspection ResultOfObjectAllocationIgnored (Union writes to "emptyUnion" as a side effect of construction)
    new Union(lgK, WritableMemory.wrap(emptyUnion));
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    // Copy prebuilt empty union object.
    // Not necessary to cache a Union wrapper around the initialized memory, because:
    //  - It is cheap to reconstruct by re-wrapping the memory in "aggregate" and "get".
    //  - Unlike the HllSketch objects used by HllSketchBuildBufferAggregator, our Union objects never exceed the
    //    max size and therefore do not need to be potentially moved in-heap.

    final int oldPosition = buf.position();
    try {
      buf.position(position);
      buf.put(emptyUnion);
    }
    finally {
      buf.position(oldPosition);
    }
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final HllSketch sketch = selector.getObject();
    if (sketch == null) {
      return;
    }
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN).writableRegion(position, size);
    final Lock lock = stripedLock.getAt(HllSketchBuildBufferAggregator.lockIndex(position)).writeLock();
    lock.lock();
    try {
      final Union union = Union.writableWrap(mem);
      union.update(sketch);
    }
    finally {
      lock.unlock();
    }
  }

  /**
   * This method uses locks because it can be used during indexing,
   * and Druid can call aggregate() and get() concurrently
   * See https://github.com/druid-io/druid/pull/3956
   */
  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    final WritableMemory mem = WritableMemory.wrap(buf, ByteOrder.LITTLE_ENDIAN).writableRegion(position, size);
    final Lock lock = stripedLock.getAt(HllSketchBuildBufferAggregator.lockIndex(position)).readLock();
    lock.lock();
    try {
      final Union union = Union.writableWrap(mem);
      return union.getResult(tgtHllType);
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public void close()
  {
    // nothing to close
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
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    // lgK should be inspected because different execution paths exist in Union.update() that is called from
    // @CalledFromHotLoop-annotated aggregate() depending on the lgK.
    // See https://github.com/apache/incubator-druid/pull/6893#discussion_r250726028
    inspector.visit("lgK", lgK);
  }
}
