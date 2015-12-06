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

package io.druid.curator;

import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class exists to ignore the shutdownNow() call that PathChildrenCache does on close() so that we can share the
 * same executor amongst multiple caches...
 */
public class ShutdownNowIgnoringExecutorService implements ExecutorService
{
  private final ExecutorService exec;

  public ShutdownNowIgnoringExecutorService(
      ExecutorService exec
  )
  {
    this.exec = exec;
  }

  @Override
  public void shutdown()
  {
    // Ignore!
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    // Ignore!
    return ImmutableList.of();
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
  public <T> Future<T> submit(Callable<T> task)
  {
    return exec.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result)
  {
    return exec.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task)
  {
    return exec.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    return exec.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks,
      long timeout,
      TimeUnit unit
  ) throws InterruptedException
  {
    return exec.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
  {
    return exec.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(
      Collection<? extends Callable<T>> tasks,
      long timeout,
      TimeUnit unit
  ) throws InterruptedException, ExecutionException, TimeoutException
  {
    return exec.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command)
  {
    exec.execute(command);
  }
}
