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

package io.druid.java.util.common.guava;

/**
 *  @deprecated this class uses expensive volatile counter inside, but it is not thread-safe. It is going to be removed
 *  in the future.
 */

@Deprecated
public class LimitedYieldingAccumulator<OutType, T> extends YieldingAccumulator<OutType, T>
{
  private final int limit;
  private final YieldingAccumulator<OutType, T> delegate;

  private volatile int count = 0;

  public LimitedYieldingAccumulator(
      YieldingAccumulator<OutType, T> delegate, int limit
  )
  {
    this.limit = limit;
    this.delegate = delegate;
  }

  @Override
  public void yield()
  {
    delegate.yield();
  }

  @Override
  public boolean yielded()
  {
    return delegate.yielded();
  }

  @Override
  public void reset()
  {
    delegate.reset();
  }

  @Override
  public OutType accumulate(OutType accumulated, T in)
  {
    if (count < limit) {
      count++;
      return delegate.accumulate(accumulated, in);
    }
    return accumulated;
  }
}
