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

package org.apache.druid.query;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.error.DruidException;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link QueryProcessingPool} that throws when any query execution task unit is submitted to it. It is
 * semantically shutdown from the moment it is created, and since the shutdown methods are supposed to be idempotent,
 * they do not throw like the execution methods
 */
public class NoopQueryProcessingPool implements QueryProcessingPool
{
  private static final NoopQueryProcessingPool INSTANCE = new NoopQueryProcessingPool();

  public static NoopQueryProcessingPool instance()
  {
    return INSTANCE;
  }

  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task)
  {
    throw unsupportedException();
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> callable)
  {
    throw unsupportedException();
  }

  @Override
  public ListenableFuture<?> submit(Runnable runnable)
  {
    throw unsupportedException();
  }

  @Override
  public <T> ListenableFuture<T> submit(Runnable runnable, T t)
  {
    throw unsupportedException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection)
  {
    throw unsupportedException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
  {
    throw unsupportedException();
  }

  @Override
  public void shutdown()
  {
    // No op, since it is already shutdown
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
  public boolean awaitTermination(long l, TimeUnit timeUnit)
  {
    return true;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> collection)
  {
    throw unsupportedException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
  {
    throw unsupportedException();
  }

  @Override
  public void execute(Runnable runnable)
  {
    throw unsupportedException();
  }

  private DruidException unsupportedException()
  {
    return DruidException.defensive("Unexpected call made to NoopQueryProcessingPool");
  }
}
