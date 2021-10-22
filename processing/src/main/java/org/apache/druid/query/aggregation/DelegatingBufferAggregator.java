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

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * A BufferAggregator that delegates everything. It is used by BufferAggregator wrappers e.g.
 * {@link StringColumnDoubleBufferAggregatorWrapper} that modify some behavior of a delegate.
 */
public abstract class DelegatingBufferAggregator implements BufferAggregator
{
  protected BufferAggregator delegate;

  @Override
  public void init(ByteBuffer buf, int position)
  {
    delegate.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    delegate.aggregate(buf, position);
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return delegate.get(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return delegate.getFloat(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return delegate.getLong(buf, position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return delegate.getDouble(buf, position);
  }

  @Override
  public void close()
  {
    delegate.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    delegate.inspectRuntimeShape(inspector);
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    delegate.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
  }

  @Override
  public boolean isNull(ByteBuffer buf, int position)
  {
    return delegate.isNull(buf, position);
  }
}
