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

package io.druid.query.aggregation;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HistogramBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;
  private final float[] breaks;
  private final int minOffset;
  private final int maxOffset;

  public HistogramBufferAggregator(FloatColumnSelector selector, float[] breaks)
  {
    this.selector = selector;
    this.breaks   = breaks;
    this.minOffset = Longs.BYTES * (breaks.length + 1);
    this.maxOffset = this.minOffset + Floats.BYTES;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    final long[] bins = new long[breaks.length + 1];
    mutationBuffer.asLongBuffer().put(bins);
    mutationBuffer.putFloat(position + minOffset, Float.POSITIVE_INFINITY);
    mutationBuffer.putFloat(position + maxOffset, Float.NEGATIVE_INFINITY);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final float value = selector.get();
    final int minPos = position + minOffset;
    final int maxPos = position + maxOffset;

    if(value < buf.getFloat(minPos)) {
      buf.putFloat(minPos, value);
    }
    if(value > buf.getFloat(maxPos)) {
      buf.putFloat(maxPos, value);
    }

    int index = Arrays.binarySearch(breaks, value);
    index = (index >= 0) ? index : -(index + 1);

    final int offset = position + (index * Longs.BYTES);
    final long count = buf.getLong(offset);
    buf.putLong(offset, count + 1);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    long[] bins = new long[breaks.length + 1];
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.asLongBuffer().get(bins);

    float min = mutationBuffer.getFloat(position + minOffset);
    float max = mutationBuffer.getFloat(position + maxOffset);
    return new Histogram(breaks, bins, min, max);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HistogramBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HistogramBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("HistogramBufferAggregator does not support getDouble");
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
