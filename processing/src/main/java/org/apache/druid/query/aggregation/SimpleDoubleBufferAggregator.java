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

package org.apache.druid.query.aggregation;

import org.apache.druid.query.monomorphicprocessing.CalledFromHotLoop;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

public abstract class SimpleDoubleBufferAggregator implements BufferAggregator
{
  final BaseDoubleColumnValueSelector selector;

  public SimpleDoubleBufferAggregator(BaseDoubleColumnValueSelector selector)
  {
    this.selector = selector;
  }

  public BaseDoubleColumnValueSelector getSelector()
  {
    return selector;
  }

  /**
   * Faster equivalent to
   * aggregator.init(buf, position);
   * aggregator.aggregate(buf, position, value);
   */
  @CalledFromHotLoop
  public abstract void putFirst(ByteBuffer buf, int position, double value);

  @CalledFromHotLoop
  public abstract void aggregate(ByteBuffer buf, int position, double value);

  @Override
  public final void aggregate(ByteBuffer buf, int position)
  {
    aggregate(buf, position, selector.getDouble());
  }

  @Override
  public final Object get(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  public final float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(position);
  }

  @Override
  public final long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(position);
  }

  @Override
  public double getDouble(ByteBuffer buffer, int position)
  {
    return buffer.getDouble(position);
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
