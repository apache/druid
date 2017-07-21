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

import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.nio.ByteBuffer;

public class FilteredBufferAggregator implements BufferAggregator
{
  private final ValueMatcher matcher;
  private final BufferAggregator delegate;

  public FilteredBufferAggregator(ValueMatcher matcher, BufferAggregator delegate)
  {
    this.matcher = matcher;
    this.delegate = delegate;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    delegate.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (matcher.matches()) {
      delegate.aggregate(buf, position);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return delegate.get(buf, position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return delegate.getLong(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return delegate.getFloat(buf, position);
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
    inspector.visit("matcher", matcher);
    inspector.visit("delegate", delegate);
  }
}
