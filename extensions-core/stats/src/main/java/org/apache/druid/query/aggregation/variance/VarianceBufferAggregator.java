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

package org.apache.druid.query.aggregation.variance;

import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class VarianceBufferAggregator implements BufferAggregator
{
  private static final int COUNT_OFFSET = 0;
  private static final int SUM_OFFSET = Long.BYTES;
  private static final int NVARIANCE_OFFSET = SUM_OFFSET + Double.BYTES;

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
    private final boolean noNulls = NullHandling.replaceWithDefault();
    private final BaseFloatColumnValueSelector selector;

    public FloatVarianceAggregator(BaseFloatColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      if (noNulls || !selector.isNull()) {
        float v = selector.getFloat();
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
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }

  public static final class DoubleVarianceAggregator extends VarianceBufferAggregator
  {
    private final boolean noNulls = NullHandling.replaceWithDefault();
    private final BaseDoubleColumnValueSelector selector;

    public DoubleVarianceAggregator(BaseDoubleColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      if (noNulls || !selector.isNull()) {
        double v = selector.getDouble();
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
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }

  public static final class LongVarianceAggregator extends VarianceBufferAggregator
  {
    private final boolean noNulls = NullHandling.replaceWithDefault();
    private final BaseLongColumnValueSelector selector;

    public LongVarianceAggregator(BaseLongColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      if (noNulls || !selector.isNull()) {
        long v = selector.getLong();
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
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }

  public static final class ObjectVarianceAggregator extends VarianceBufferAggregator
  {
    private final BaseObjectColumnValueSelector selector;

    public ObjectVarianceAggregator(BaseObjectColumnValueSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      VarianceAggregatorCollector holder2 = (VarianceAggregatorCollector) selector.getObject();
      Preconditions.checkState(holder2 != null);
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
