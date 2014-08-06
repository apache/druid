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

package io.druid.query.aggregation.histogram;

import io.druid.query.aggregation.BufferAggregator;
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
  public void close()
  {
    // no resources to cleanup
  }
}
