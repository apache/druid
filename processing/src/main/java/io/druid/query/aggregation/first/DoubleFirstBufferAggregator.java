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

package io.druid.query.aggregation.first;

import com.google.common.primitives.Longs;
import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;

import java.nio.ByteBuffer;

public class DoubleFirstBufferAggregator implements BufferAggregator
{
  private final LongColumnSelector timeSelector;
  private final FloatColumnSelector valueSelector;

  public DoubleFirstBufferAggregator(LongColumnSelector timeSelector, FloatColumnSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MAX_VALUE);
    buf.putDouble(position + Longs.BYTES, 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    long time = timeSelector.get();
    long firstTime = buf.getLong(position);
    if (time < firstTime) {
      buf.putLong(position, time);
      buf.putDouble(position + Longs.BYTES, valueSelector.get());
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new SerializablePair<>(buf.getLong(position), buf.getDouble(position + Longs.BYTES));
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position + Longs.BYTES);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position + Longs.BYTES);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("timeSelector", timeSelector);
    inspector.visit("valueSelector", valueSelector);
  }
}
