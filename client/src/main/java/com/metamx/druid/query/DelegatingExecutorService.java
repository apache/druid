/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class DelegatingExecutorService implements ExecutorService
{
  private final ExecutorService delegate;

  public DelegatingExecutorService(ExecutorService delegate)
  {
    this.delegate = delegate;
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
  public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException
  {
    return delegate.awaitTermination(l, timeUnit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> tCallable)
  {
    return delegate.submit(tCallable);
  }

  @Override
  public <T> Future<T> submit(Runnable runnable, T t)
  {
    return delegate.submit(runnable, t);
  }

  @Override
  public Future<?> submit(Runnable runnable)
  {
    return delegate.submit(runnable);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> callables) throws InterruptedException
  {
    return delegate.invokeAll(callables);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> callables,
      long l,
      TimeUnit timeUnit
  ) throws InterruptedException
  {
    return delegate.invokeAll(callables, l, timeUnit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> callables) throws InterruptedException, ExecutionException
  {
    return delegate.invokeAny(callables);
  }

  @Override
  public <T> T invokeAny(
      Collection<? extends Callable<T>> callables,
      long l,
      TimeUnit timeUnit
  ) throws InterruptedException, ExecutionException, TimeoutException
  {
    return delegate.invokeAny(callables, l, timeUnit);
  }

  @Override
  public void execute(Runnable runnable)
  {
    delegate.execute(runnable);
  }
}
