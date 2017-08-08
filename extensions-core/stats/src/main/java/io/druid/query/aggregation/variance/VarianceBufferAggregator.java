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

package io.druid.query.aggregation.variance;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class VarianceBufferAggregator implements BufferAggregator
{
  private static final int COUNT_OFFSET = 0;
  private static final int SUM_OFFSET = Longs.BYTES;
  private static final int NVARIANCE_OFFSET = SUM_OFFSET + Doubles.BYTES;

  protected final String name;

  public VarianceBufferAggregator(String name)
  {
    this.name = name;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position + COUNT_OFFSET, 0)
       .putDouble(position + SUM_OFFSET, 0)
       .putDouble(position + NVARIANCE_OFFSET, 0);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    VarianceAggregatorCollector holder = new VarianceAggregatorCollector();
    holder.count = buf.getLong(position);
    holder.sum = buf.getDouble(position + SUM_OFFSET);
    holder.nvariance = buf.getDouble(position + NVARIANCE_OFFSET);
    return holder;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("VarianceBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("VarianceBufferAggregator does not support getFloat()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("VarianceBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
  }

  public static final class FloatVarianceAggregator extends VarianceBufferAggregator
  {
    private final FloatColumnSelector selector;

    public FloatVarianceAggregator(String name, FloatColumnSelector selector)
    {
      super(name);
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      float v = selector.get();
      long count = buf.getLong(position + COUNT_OFFSET) + 1;
      double sum = buf.getDouble(position + SUM_OFFSET) + v;
      buf.putLong(position, count);
      buf.putDouble(position + SUM_OFFSET, sum);
      if (count > 1) {
        double t = count * v - sum;
        double variance = buf.getDouble(position + NVARIANCE_OFFSET) + (t * t) / ((double) count * (count - 1));
        buf.putDouble(position + NVARIANCE_OFFSET, variance);
      }
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }

  public static final class LongVarianceAggregator extends VarianceBufferAggregator
  {
    private final LongColumnSelector selector;

    public LongVarianceAggregator(String name, LongColumnSelector selector)
    {
      super(name);
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      long v = selector.get();
      long count = buf.getLong(position + COUNT_OFFSET) + 1;
      double sum = buf.getDouble(position + SUM_OFFSET) + v;
      buf.putLong(position, count);
      buf.putDouble(position + SUM_OFFSET, sum);
      if (count > 1) {
        double t = count * v - sum;
        double variance = buf.getDouble(position + NVARIANCE_OFFSET) + (t * t) / ((double) count * (count - 1));
        buf.putDouble(position + NVARIANCE_OFFSET, variance);
      }
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }

  public static final class ObjectVarianceAggregator extends VarianceBufferAggregator
  {
    private final ObjectColumnSelector selector;

    public ObjectVarianceAggregator(String name, ObjectColumnSelector selector)
    {
      super(name);
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      VarianceAggregatorCollector holder2 = (VarianceAggregatorCollector) selector.get();

      long count = buf.getLong(position + COUNT_OFFSET);
      if (count == 0) {
        buf.putLong(position, holder2.count);
        buf.putDouble(position + SUM_OFFSET, holder2.sum);
        buf.putDouble(position + NVARIANCE_OFFSET, holder2.nvariance);
        return;
      }

      double sum = buf.getDouble(position + SUM_OFFSET);
      double nvariance = buf.getDouble(position + NVARIANCE_OFFSET);

      final double ratio = count / (double) holder2.count;
      final double t = sum / ratio - holder2.sum;

      nvariance += holder2.nvariance + (ratio / (count + holder2.count) * t * t);
      count += holder2.count;
      sum += holder2.sum;

      buf.putLong(position, count);
      buf.putDouble(position + SUM_OFFSET, sum);
      buf.putDouble(position + NVARIANCE_OFFSET, nvariance);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }
}
