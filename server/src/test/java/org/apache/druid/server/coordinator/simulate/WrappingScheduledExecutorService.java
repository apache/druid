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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.java.util.common.logger.Logger;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Wraps an {@link ExecutorService} into a {@link ScheduledExecutorService}.
 */
public class WrappingScheduledExecutorService implements ScheduledExecutorService
{
  private static final Logger log = new Logger(WrappingScheduledExecutorService.class);

  private final String nameFormat;
  private final ExecutorService delegate;
  private final boolean ignoreScheduledTasks;

  public WrappingScheduledExecutorService(
      String nameFormat,
      ExecutorService delegate,
      boolean ignoreScheduledTasks
  )
  {
    this.nameFormat = nameFormat;
    this.delegate = delegate;
    this.ignoreScheduledTasks = ignoreScheduledTasks;
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
  {
    if (ignoreScheduledTasks) {
      log.debug("[%s] Ignoring scheduled task", nameFormat);
      return new WrappingScheduledFuture<>(CompletableFuture.completedFuture(null));
    }

    // Ignore the delay and just queue the task
    return new WrappingScheduledFuture<>(submit(command));
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
  {
    if (ignoreScheduledTasks) {
      log.debug("[%s] Ignoring scheduled task", nameFormat);
      return new WrappingScheduledFuture<>(CompletableFuture.completedFuture(null));
    }

    // Ignore the delay and just queue the task
    return new WrappingScheduledFuture<>(submit(callable));
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command,
      long initialDelay,
      long period,
      TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command,
      long initialDelay,
      long delay,
      TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown()
  {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown()
  {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated()
  {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    return delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task)
  {
    return delegate.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result)
  {
    return delegate.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task)
  {
    return delegate.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    return delegate.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit
  ) throws InterruptedException
  {
    return delegate.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
  {
    return delegate.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    return delegate.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command)
  {
    delegate.execute(command);
  }

  /**
   * Wraps a Future into a ScheduledFuture.
   */
  private static class WrappingScheduledFuture<V> implements ScheduledFuture<V>
  {
    private final Future<V> future;

    private WrappingScheduledFuture(Future<V> future)
    {
      this.future = future;
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
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled()
    {
      return future.isCancelled();
    }

    @Override
    public boolean isDone()
    {
      return future.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException
    {
      return future.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
      return future.get(timeout, unit);
    }
  }
}
