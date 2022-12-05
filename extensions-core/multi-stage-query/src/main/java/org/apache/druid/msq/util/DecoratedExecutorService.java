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

package org.apache.druid.msq.util;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@link ListeningExecutorService} where all tasks are automatically decorated before being submitted to a
 * delegate executor service.
 */
public class DecoratedExecutorService implements ListeningExecutorService
{
  private final ListeningExecutorService exec;
  private final Decorator decorator;

  public DecoratedExecutorService(
      final ListeningExecutorService exec,
      final Decorator decorator
  )
  {
    this.exec = exec;
    this.decorator = decorator;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> task)
  {
    return exec.submit(decorator.decorateCallable(task));
  }

  @Override
  public ListenableFuture<?> submit(Runnable task)
  {
    return exec.submit(decorator.decorateRunnable(task));
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable task, T result)
  {
    return exec.submit(decorator.decorateRunnable(task), result);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    final List<Callable<T>> decoratedTasks = new ArrayList<>();

    for (final Callable<T> task : tasks) {
      decoratedTasks.add(decorator.decorateCallable(task));
    }

    return exec.invokeAll(decoratedTasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException
  {
    final List<Callable<T>> decoratedTasks = new ArrayList<>();

    for (final Callable<T> task : tasks) {
      decoratedTasks.add(decorator.decorateCallable(task));
    }

    return exec.invokeAll(decoratedTasks, timeout, unit);
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
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    return exec.awaitTermination(timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
  {
    final List<Callable<T>> decoratedTasks = new ArrayList<>();

    for (final Callable<T> task : tasks) {
      decoratedTasks.add(decorator.decorateCallable(task));
    }

    return exec.invokeAny(decoratedTasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    final List<Callable<T>> decoratedTasks = new ArrayList<>();

    for (final Callable<T> task : tasks) {
      decoratedTasks.add(decorator.decorateCallable(task));
    }

    return exec.invokeAny(decoratedTasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command)
  {
    exec.execute(decorator.decorateRunnable(command));
  }

  public interface Decorator
  {
    <T> Callable<T> decorateCallable(Callable<T> callable);

    Runnable decorateRunnable(Runnable runnable);
  }
}
