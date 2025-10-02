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

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link QueryProcessingPool} that just forwards operations, including query execution tasks,
 * to an underlying {@link ExecutorService}
 */
public class ForwardingQueryProcessingPool extends ForwardingListeningExecutorService implements QueryProcessingPool
{
  private final ListeningExecutorService delegate;
  @Nullable
  private final ScheduledExecutorService timeoutService;

  public ForwardingQueryProcessingPool(ExecutorService executorService)
  {
    this(MoreExecutors.listeningDecorator(executorService), null);
  }

  public ForwardingQueryProcessingPool(
      ExecutorService executorService,
      @Nullable ScheduledExecutorService timeoutService
  )
  {
    this.delegate = MoreExecutors.listeningDecorator(executorService);
    if (timeoutService != null) {
      this.timeoutService = MoreExecutors.listeningDecorator(timeoutService);
    } else {
      this.timeoutService = null;
    }
  }

  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task)
  {
    return delegate().submit(task);
  }

  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(
      PrioritizedQueryRunnerCallable<T, V> task,
      long timeout,
      TimeUnit unit
  )
  {
    if (timeoutService != null) {
      return Futures.withTimeout(
          delegate().submit(task),
          timeout,
          TimeUnit.MILLISECONDS,
          timeoutService
      );
    } else {
      return submitRunnerTask(task);
    }
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }

}
