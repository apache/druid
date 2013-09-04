/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation;

import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
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
    mutationBuffer.putFloat(position + minOffset, Float.MAX_VALUE);
    mutationBuffer.putFloat(position + maxOffset, Float.MIN_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final float value = selector.get();
    final int minPos = position + minOffset;
    final int maxPos = position + maxOffset;

    if(value < buf.getFloat(minPos)) buf.putFloat(minPos, value);
    if(value > buf.getFloat(maxPos)) buf.putFloat(maxPos, value);

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
  public void close()
  {
    // no resources to cleanup
  }
}
