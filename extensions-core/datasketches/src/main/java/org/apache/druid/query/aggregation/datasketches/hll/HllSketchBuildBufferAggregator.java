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

import org.apache.datasketches.hll.TgtHllType;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * This aggregator builds sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class HllSketchBuildBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<Object> selector;
  private final HllSketchBuildBufferAggregatorHelper helper;

  public HllSketchBuildBufferAggregator(
      final ColumnValueSelector<Object> selector,
      final int lgK,
      final TgtHllType tgtHllType,
      final int size
  )
  {
    this.selector = selector;
    this.helper = new HllSketchBuildBufferAggregatorHelper(lgK, tgtHllType, size);
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    helper.init(buf, position);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position)
  {
    final Object value = selector.getObject();
    if (value == null) {
      return;
    }

    HllSketchBuildAggregator.updateSketch(helper.getSketchAtPosition(buf, position), value);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return helper.get(buf, position);
  }

  @Override
  public void close()
  {
    helper.clear();
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

  /**
   * In very rare cases sketches can exceed given memory, request on-heap memory and move there.
   * We need to identify such sketches and reuse the same objects as opposed to wrapping new memory regions.
   */
  @Override
  public void relocate(final int oldPosition, final int newPosition, final ByteBuffer oldBuf, final ByteBuffer newBuf)
  {
    helper.relocate(oldPosition, newPosition, oldBuf, newBuf);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    // lgK should be inspected because different execution paths exist in HllSketch.update() that is called from
    // @CalledFromHotLoop-annotated aggregate() depending on the lgK.
    // See https://github.com/apache/druid/pull/6893#discussion_r250726028
    inspector.visit("lgK", helper.getLgK());
  }
}
