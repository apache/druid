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

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * This aggregator merges existing sketches.
 * The input column must contain {@link HllSketch}
 */
public class HllSketchMergeBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<HllSketch> selector;
  private final HllSketchMergeBufferAggregatorHelper helper;

  public HllSketchMergeBufferAggregator(
      final ColumnValueSelector<HllSketch> selector,
      final int lgK,
      final TgtHllType tgtHllType,
      final int size
  )
  {
    this.selector = selector;
    this.helper = new HllSketchMergeBufferAggregatorHelper(lgK, tgtHllType, size);
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    helper.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final HllSketch sketch = selector.getObject();
    if (sketch == null) {
      return;
    }

    final WritableMemory mem = WritableMemory.writableWrap(buf, ByteOrder.LITTLE_ENDIAN)
                                             .writableRegion(position, helper.getSize());

    final Union union = Union.writableWrap(mem);
    union.update(sketch);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return helper.get(buf, position);
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
    // See https://github.com/apache/druid/pull/6893#discussion_r250726028
    inspector.visit("lgK", helper.getLgK());
  }
}
