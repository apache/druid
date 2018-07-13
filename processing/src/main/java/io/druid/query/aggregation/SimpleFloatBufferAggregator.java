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

package io.druid.query.aggregation;

import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseFloatColumnValueSelector;

import java.nio.ByteBuffer;

public abstract class SimpleFloatBufferAggregator implements BufferAggregator
{
  final BaseFloatColumnValueSelector selector;

  SimpleFloatBufferAggregator(BaseFloatColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @CalledFromHotLoop
  public abstract void aggregate(ByteBuffer buf, int position, float value);

  @Override
  public final void aggregate(ByteBuffer buf, int position)
  {
    aggregate(buf, position, selector.getFloat());
  }

  @Override
  public final Object get(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public final float getFloat(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public final long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getFloat(position);
  }

  @Override
  public double getDouble(ByteBuffer buffer, int position)
  {
    return (double) buffer.getFloat(position);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
