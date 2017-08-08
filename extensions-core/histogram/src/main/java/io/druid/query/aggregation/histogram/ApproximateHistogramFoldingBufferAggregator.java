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

package io.druid.query.aggregation.histogram;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

public class ApproximateHistogramFoldingBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector<ApproximateHistogram> selector;
  private final int resolution;
  private final float upperLimit;
  private final float lowerLimit;

  private float[] tmpBufferP;
  private long[] tmpBufferB;

  public ApproximateHistogramFoldingBufferAggregator(
      ObjectColumnSelector<ApproximateHistogram> selector,
      int resolution,
      float lowerLimit,
      float upperLimit
  )
  {
    this.selector = selector;
    this.resolution = resolution;
    this.lowerLimit = lowerLimit;
    this.upperLimit = upperLimit;

    tmpBufferP = new float[resolution];
    tmpBufferB = new long[resolution];
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ApproximateHistogram h = new ApproximateHistogram(resolution, lowerLimit, upperLimit);

    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    // use dense storage for aggregation
    h.toBytesDense(mutationBuffer);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    ApproximateHistogram h0 = ApproximateHistogram.fromBytesDense(mutationBuffer);
    h0.setLowerLimit(lowerLimit);
    h0.setUpperLimit(upperLimit);
    ApproximateHistogram hNext = selector.get();
    h0.foldFast(hNext, tmpBufferP, tmpBufferB);

    mutationBuffer.position(position);
    h0.toBytesDense(mutationBuffer);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.asReadOnlyBuffer();
    mutationBuffer.position(position);
    return ApproximateHistogram.fromBytesDense(mutationBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("ApproximateHistogramFoldingBufferAggregator does not support getDouble()");
  }
  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
