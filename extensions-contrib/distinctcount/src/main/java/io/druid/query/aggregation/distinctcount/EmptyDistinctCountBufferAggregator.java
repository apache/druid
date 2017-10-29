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

package io.druid.query.aggregation.distinctcount;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.nio.ByteBuffer;

/**
 * The difference from {@link io.druid.query.aggregation.NoopBufferAggregator} is that
 * EmptyDistinctCountBufferAggregator returns 0 instead of null from {@link #get(ByteBuffer, int)}.
 */
public final class EmptyDistinctCountBufferAggregator implements BufferAggregator
{
  private static final EmptyDistinctCountBufferAggregator INSTANCE = new EmptyDistinctCountBufferAggregator();

  static EmptyDistinctCountBufferAggregator instance()
  {
    return INSTANCE;
  }

  private EmptyDistinctCountBufferAggregator()
  {
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return 0L;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) 0;
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) 0;
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return 0;
  }

  @Override
  public void close()
  {
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
