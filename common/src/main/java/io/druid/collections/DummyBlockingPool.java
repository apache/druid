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

package io.druid.collections;

import java.util.List;

/**
 * BlockingPool with 0 maxSize, all take*() methods immediately throw {@link UnsupportedOperationException}.
 */
public final class DummyBlockingPool<T> implements BlockingPool<T>
{
  private static final DummyBlockingPool INSTANCE = new DummyBlockingPool();

  @SuppressWarnings("unchecked")
  public static <T> BlockingPool<T> instance()
  {
    return INSTANCE;
  }

  private DummyBlockingPool()
  {
  }

  @Override
  public int maxSize()
  {
    return 0;
  }

  @Override
  public ReferenceCountingResourceHolder<T> take(long timeoutMs)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ReferenceCountingResourceHolder<T> take()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ReferenceCountingResourceHolder<T>> takeBatch(int elementNum, long timeoutMs)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<ReferenceCountingResourceHolder<T>> takeBatch(int elementNum)
  {
    throw new UnsupportedOperationException();
  }
}
