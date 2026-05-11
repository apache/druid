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

package org.apache.druid.java.util.common.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An {@link ExecutorService} where all tasks are automatically decorated before being submitted to a
 * delegate executor service.
 */
public class DecoratedExecutorService implements ExecutorService
{
  protected final ExecutorService exec;
  protected final Decorator decorator;

  public DecoratedExecutorService(
      final ExecutorService exec,
      final Decorator decorator
  )
  {
    this.exec = exec;
    this.decorator = decorator;
  }

  @Override
  public <T> Future<T> submit(final Callable<T> task)
  {
    return exec.submit(decorator.decorateCallable(task));
  }

  @Override
  public Future<?> submit(final Runnable task)
  {
    return exec.submit(decorator.decorateRunnable(task));
  }

  @Override
  public <T> Future<T> submit(final Runnable task, final T result)
  {
    return exec.submit(decorator.decorateRunnable(task), result);
  }

  @Override
  public <T> List<Future<T>> invokeAll(final Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    return exec.invokeAll(decorateAll(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      final Collection<? extends Callable<T>> tasks,
      final long timeout,
      final TimeUnit unit
  ) throws InterruptedException
  {
    return exec.invokeAll(decorateAll(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(final Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException
  {
    return exec.invokeAny(decorateAll(tasks));
  }

  @Override
  public <T> T invokeAny(
      final Collection<? extends Callable<T>> tasks,
      final long timeout,
      final TimeUnit unit
  ) throws InterruptedException, ExecutionException, TimeoutException
  {
    return exec.invokeAny(decorateAll(tasks), timeout, unit);
  }

  @Override
  public void execute(final Runnable command)
  {
    exec.execute(decorator.decorateRunnable(command));
  }

  @Override
  public void shutdown()
  {
    exec.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return exec.shutdownNow();
  }

  @Override
  public boolean isShutdown()
  {
    return exec.isShutdown();
  }

  @Override
  public boolean isTerminated()
  {
    return exec.isTerminated();
  }

  @Override
  public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException
  {
    return exec.awaitTermination(timeout, unit);
  }

  private <T> List<Callable<T>> decorateAll(final Collection<? extends Callable<T>> tasks)
  {
    final List<Callable<T>> decorated = new ArrayList<>(tasks.size());
    for (final Callable<T> task : tasks) {
      decorated.add(decorator.decorateCallable(task));
    }
    return decorated;
  }

  /**
   * Decorates tasks before they are submitted to an executor.
   */
  public interface Decorator
  {
    <T> Callable<T> decorateCallable(Callable<T> callable);

    Runnable decorateRunnable(Runnable runnable);

    /**
     * Decorates a callable that will be executed after an intended delay. Used by
     * {@link DecoratedScheduledExecutorService} for {@link java.util.concurrent.ScheduledExecutorService#schedule}.
     * Defaults to {@link #decorateCallable}.
     */
    default <T> Callable<T> decorateScheduledCallable(Callable<T> callable, long delay, TimeUnit unit)
    {
      return decorateCallable(callable);
    }

    /**
     * Decorates a runnable that will be executed after an intended delay. Used by
     * {@link DecoratedScheduledExecutorService} for {@link java.util.concurrent.ScheduledExecutorService#schedule}.
     * Defaults to {@link #decorateRunnable}.
     */
    default Runnable decorateScheduledRunnable(Runnable runnable, long delay, TimeUnit unit)
    {
      return decorateRunnable(runnable);
    }
  }
}
