/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.AdaptiveCounting;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.metamx.common.logger.Logger;
import com.metamx.druid.processing.ComplexMetricSelector;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AdaptiveCountingBufferAggregator implements BufferAggregator
{
  private static final Logger log = new Logger(AdaptiveCountingBufferAggregator.class);

  private final ComplexMetricSelector<ICardinality> selector;
  private ICardinality card;

  public AdaptiveCountingBufferAggregator(ComplexMetricSelector<ICardinality> selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    card = AdaptiveCounting.Builder.obyCount(Integer.MAX_VALUE).build();
    try {
      byte[] bytes = card.getBytes();
      ByteBuffer copy = buf.duplicate();
      copy.position(position);
      copy.put(bytes);
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to init: " + e);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    int size = card.sizeof();
    byte[] bytes = new byte[size];
    ByteBuffer copy = buf.duplicate();
    copy.position(position);
    copy.get(bytes, 0, size);
    ICardinality cardinalityCounter = new AdaptiveCounting(bytes);
    ICardinality valueToAgg = selector.get();
    try {
      cardinalityCounter = cardinalityCounter.merge(valueToAgg);
      bytes = cardinalityCounter.getBytes();
      copy.position(position);
      copy.put(bytes);
    }
    catch (CardinalityMergeException e) {
      throw new RuntimeException("Failed to aggregate: " + e);
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to aggregate: " + e);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    int size = card.sizeof();
    byte[] bytes = new byte[size];
    ByteBuffer copy = buf.duplicate();
    copy.position(position);
    copy.get(bytes, 0, size);
    ICardinality cardinalityCounter = new AdaptiveCounting(bytes);
    return cardinalityCounter;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("AdaptiveCountingBufferAggregator does not support getFloat()");
  }

  @Override
  public void close()
  {
    card = null;
  }
}
