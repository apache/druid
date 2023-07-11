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

package org.apache.druid.rpc;

import com.google.common.util.concurrent.ForwardingExecutorService;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Used by {@link ServiceClientImplTest} so retries happen immediately.
 */
public class NoDelayScheduledExecutorService extends ForwardingExecutorService implements ScheduledExecutorService
{
  private final ExecutorService delegate;

  public NoDelayScheduledExecutorService(final ExecutorService delegate)
  {
    this.delegate = delegate;
  }

  @Override
  protected ExecutorService delegate()
  {
    return delegate;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
  {
    return new NoDelayScheduledFuture<>(delegate.submit(command));
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
  {
    return new NoDelayScheduledFuture<>(delegate.submit(callable));
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  private static class NoDelayScheduledFuture<T> implements ScheduledFuture<T>
  {
    private final Future<T> delegate;

    public NoDelayScheduledFuture(final Future<T> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public long getDelay(TimeUnit unit)
    {
      return 0;
    }

    @Override
    public int compareTo(Delayed o)
    {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return delegate.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled()
    {
      return delegate.isCancelled();
    }

    @Override
    public boolean isDone()
    {
      return delegate.isDone();
    }

    @Override
    public T get() throws InterruptedException, ExecutionException
    {
      return delegate.get();
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
      return delegate.get(timeout, unit);
    }
  }
}
