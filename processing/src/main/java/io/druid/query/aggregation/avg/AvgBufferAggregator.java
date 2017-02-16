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

package io.druid.query.aggregation.avg;

import com.google.common.primitives.Longs;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public abstract class AvgBufferAggregator implements BufferAggregator
{
  private static final int COUNT_OFFSET = 0;
  private static final int SUM_OFFSET = Longs.BYTES;

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position + COUNT_OFFSET, 0).putDouble(position + SUM_OFFSET, 0);
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    AvgAggregatorCollector holder = new AvgAggregatorCollector();
    holder.count = buf.getLong(position);
    holder.sum = buf.getDouble(position + SUM_OFFSET);
    return holder;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    long count = buf.getLong(position + COUNT_OFFSET);
    double sum = buf.getDouble(position + SUM_OFFSET);
    return (float) sum / count;
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    long count = buf.getLong(position + COUNT_OFFSET);
    double sum = buf.getDouble(position + SUM_OFFSET);
    return (long) sum / count;
  }

  @Override
  public void close()
  {
  }

  public static final class FloatAvgBufferAggregator extends AvgBufferAggregator
  {
    private final FloatColumnSelector selector;

    public FloatAvgBufferAggregator(FloatColumnSelector selector)
    {
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
    }
  }

  public static final class LongAvgBufferAggregator extends AvgBufferAggregator
  {
    private final LongColumnSelector selector;

    public LongAvgBufferAggregator(LongColumnSelector selector)
    {
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
    }
  }

  public static final class ObjectAvgBufferAggregator extends AvgBufferAggregator
  {
    private final ObjectColumnSelector selector;

    public ObjectAvgBufferAggregator(ObjectColumnSelector selector)
    {
      this.selector = selector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      AvgAggregatorCollector holder2 = (AvgAggregatorCollector) selector.get();

      long count = buf.getLong(position + COUNT_OFFSET);
      if (count == 0) {
        buf.putLong(position, holder2.count);
        buf.putDouble(position + SUM_OFFSET, holder2.sum);
        return;
      }

      double sum = buf.getDouble(position + SUM_OFFSET);

      count += holder2.count;
      sum += holder2.sum;

      buf.putLong(position, count);
      buf.putDouble(position + SUM_OFFSET, sum);
    }
  }
}
