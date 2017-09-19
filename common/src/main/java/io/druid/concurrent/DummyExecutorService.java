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

package io.druid.concurrent;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * ExecutorService which is terminated and shutdown from the moment of creation and not able to accept any tasks.
 */
final class DummyExecutorService implements ExecutorService
{
  static final DummyExecutorService INSTANCE = new DummyExecutorService();

  private DummyExecutorService()
  {
  }

  @Override
  public void shutdown()
  {
    // Do nothing, alread shutdown
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return Collections.emptyList();
  }

  @Override
  public boolean isShutdown()
  {
    return true;
  }

  @Override
  public boolean isTerminated()
  {
    return true;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    return true;
  }

  @Override
  public <T> Future<T> submit(Callable<T> task)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Future<?> submit(Runnable task)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit
  ) throws InterruptedException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(Runnable command)
  {
    throw new UnsupportedOperationException();
  }
}
