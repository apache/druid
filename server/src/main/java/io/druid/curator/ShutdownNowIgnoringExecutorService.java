/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
