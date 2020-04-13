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

import javax.annotation.Nullable;

/**
 * An Aggregator that delegates everything. It is used by Aggregator wrappers e.g.
 * {@link StringColumnDoubleAggregatorWrapper} that modify some behavior of a delegate.
 */
public abstract class DelegatingAggregator implements Aggregator
{
  protected Aggregator delegate;

  @Override
  public void aggregate()
  {
    delegate.aggregate();
  }

  @Nullable
  @Override
  public Object get()
  {
    return delegate.get();
  }

  @Override
  public float getFloat()
  {
    return delegate.getFloat();
  }

  @Override
  public long getLong()
  {
    return delegate.getLong();
  }

  @Override
  public double getDouble()
  {
    return delegate.getDouble();
  }

  @Override
  public boolean isNull()
  {
    return delegate.isNull();
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
