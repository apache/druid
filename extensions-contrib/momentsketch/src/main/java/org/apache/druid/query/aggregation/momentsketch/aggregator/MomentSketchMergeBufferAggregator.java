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

package org.apache.druid.query.aggregation.momentsketch.aggregator;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.momentsketch.MomentSketchWrapper;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;

public class MomentSketchMergeBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<MomentSketchWrapper> selector;
  private final int size;
  private final boolean compress;

  public MomentSketchMergeBufferAggregator(
      ColumnValueSelector<MomentSketchWrapper> selector,
      int size,
      boolean compress
  )
  {
    this.selector = selector;
    this.size = size;
    this.compress = compress;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    MomentSketchWrapper h = new MomentSketchWrapper(size);
    h.setCompressed(compress);

    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    h.toBytes(mutationBuffer);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    MomentSketchWrapper msNext = selector.getObject();
    if (msNext == null) {
      return;
    }
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    MomentSketchWrapper ms0 = MomentSketchWrapper.fromBytes(mutationBuffer);
    ms0.merge(msNext);

    mutationBuffer.position(position);
    ms0.toBytes(mutationBuffer);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
    mutationBuffer.position(position);
    return MomentSketchWrapper.fromBytes(mutationBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
