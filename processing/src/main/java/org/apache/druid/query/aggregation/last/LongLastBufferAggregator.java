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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;

public class LongLastBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseLongColumnValueSelector valueSelector;

  public LongLastBufferAggregator(BaseLongColumnValueSelector timeSelector, BaseLongColumnValueSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MIN_VALUE);
    buf.putLong(position + Long.BYTES, 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    long time = timeSelector.getLong();
    long lastTime = buf.getLong(position);
    if (time >= lastTime) {
      buf.putLong(position, time);
      buf.putLong(position + Long.BYTES, valueSelector.getLong());
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new SerializablePair<>(buf.getLong(position), buf.getLong(position + Long.BYTES));
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position + Long.BYTES);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getLong(position + Long.BYTES);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position + Long.BYTES);
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
