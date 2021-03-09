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

import org.apache.druid.com.google.common.base.Preconditions;
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
  private static final int SUM_OFFSET = COUNT_OFFSET + Long.BYTES;
  private static final int NVARIANCE_OFFSET = SUM_OFFSET + Double.BYTES;

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    doInit(buf, position);
  }

  @Override
  public VarianceAggregatorCollector get(final ByteBuffer buf, final int position)
  {
    return getVarianceCollector(buf, position);
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

  public static void doInit(ByteBuffer buf, int position)
  {
    buf.putLong(position + COUNT_OFFSET, 0)
       .putDouble(position + SUM_OFFSET, 0)
       .putDouble(position + NVARIANCE_OFFSET, 0);
  }

  public static long getCount(ByteBuffer buf, int position)
  {
    return buf.getLong(position + COUNT_OFFSET);
  }

  public static double getSum(ByteBuffer buf, int position)
  {
    return buf.getDouble(position + SUM_OFFSET);
  }

  public static double getVariance(ByteBuffer buf, int position)
  {
    return buf.getDouble(position + NVARIANCE_OFFSET);
  }
  public static VarianceAggregatorCollector getVarianceCollector(ByteBuffer buf, int position)
  {
    return new VarianceAggregatorCollector(
        getCount(buf, position),
        getSum(buf, position),
        getVariance(buf, position)
    );
  }

  public static void writeNVariance(ByteBuffer buf, int position, long count, double sum, double nvariance)
  {
    buf.putLong(position + COUNT_OFFSET, count);
    buf.putDouble(position + SUM_OFFSET, sum);
    if (count > 1) {
      buf.putDouble(position + NVARIANCE_OFFSET, nvariance);
    }
  }

  public static void writeCountAndSum(ByteBuffer buf, int position, long count, double sum)
  {
    buf.putLong(position + COUNT_OFFSET, count);
    buf.putDouble(position + SUM_OFFSET, sum);
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
        long count = getCount(buf, position) + 1;
        double sum = getSum(buf, position) + v;
        writeCountAndSum(buf, position, count, sum);
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
        long count = getCount(buf, position) + 1;
        double sum = getSum(buf, position) + v;
        writeCountAndSum(buf, position, count, sum);
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
        long count = getCount(buf, position) + 1;
        double sum = getSum(buf, position) + v;
        writeCountAndSum(buf, position, count, sum);
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
      long count = getCount(buf, position);
      if (count == 0) {
        buf.putLong(position, holder2.count);
        buf.putDouble(position + SUM_OFFSET, holder2.sum);
        buf.putDouble(position + NVARIANCE_OFFSET, holder2.nvariance);
        return;
      }

      double sum = getSum(buf, position);
      double nvariance = buf.getDouble(position + NVARIANCE_OFFSET);

      final double ratio = count / (double) holder2.count;
      final double t = sum / ratio - holder2.sum;

      nvariance += holder2.nvariance + (ratio / (count + holder2.count) * t * t);
      count += holder2.count;
      sum += holder2.sum;

      writeNVariance(buf, position, count, sum, nvariance);
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("selector", selector);
    }
  }
}
