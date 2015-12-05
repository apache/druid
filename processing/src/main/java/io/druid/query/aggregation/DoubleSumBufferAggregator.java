/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.aggregation;

import io.druid.segment.FloatColumnSelector;

import java.nio.ByteBuffer;

/**
 */
public class DoubleSumBufferAggregator implements BufferAggregator
{
  private final FloatColumnSelector selector;
  private final int exponent;

  public DoubleSumBufferAggregator(
      FloatColumnSelector selector,
      int exponent
  )
  {
    this.selector = selector;
    this.exponent = exponent;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    buf.putDouble(
        position,
        buf.getDouble(position) + (exponent == 1 ?  + selector.get() : Math.pow(selector.get(), exponent))
    );
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
