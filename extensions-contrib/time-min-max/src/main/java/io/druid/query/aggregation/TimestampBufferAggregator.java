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

import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ObjectColumnSelector;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class TimestampBufferAggregator implements BufferAggregator
{
  private final ObjectColumnSelector selector;
  private final TimestampSpec timestampSpec;
  private final Comparator<Long> comparator;
  private final Long initValue;

  public TimestampBufferAggregator(
      ObjectColumnSelector selector,
      TimestampSpec timestampSpec,
      Comparator<Long> comparator,
      Long initValue)
  {
    this.selector = selector;
    this.timestampSpec = timestampSpec;
    this.comparator = comparator;
    this.initValue = initValue;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, initValue);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Long newTime = TimestampAggregatorFactory.convertLong(timestampSpec, selector.get());
    if (newTime != null) {
      long prev = buf.getLong(position);
      buf.putLong(position, comparator.compare(prev, newTime) > 0 ? prev: newTime);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getLong(position);
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("comparator", comparator);
  }
}
