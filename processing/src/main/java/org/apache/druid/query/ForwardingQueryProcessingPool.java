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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;

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
  private final ScheduledExecutorService timeoutService;

  public ForwardingQueryProcessingPool(ExecutorService executorService, ScheduledExecutorService timeoutService)
  {
    this.delegate = MoreExecutors.listeningDecorator(executorService);
    this.timeoutService = timeoutService;
  }

  @VisibleForTesting
  public ForwardingQueryProcessingPool(ExecutorService executorService)
  {
    this.delegate = MoreExecutors.listeningDecorator(executorService);
    this.timeoutService = ScheduledExecutors.fixed(1, "ForwardingQueryProcessingPool-Timeout-%d");
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
    return Futures.withTimeout(
        delegate().submit(task),
        timeout,
        unit,
        timeoutService
    );
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }
}
