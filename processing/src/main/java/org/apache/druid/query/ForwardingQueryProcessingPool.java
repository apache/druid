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
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Default implementation of {@link QueryProcessingPool} that just forwards operations, including query execution tasks,
 * to an underlying {@link ExecutorService}
 * Exposes a method {@link #submitRunnerTask(PrioritizedQueryRunnerCallable, long, TimeUnit)} which allows execution tasks to be serviced a custom timeout.
 */
public class ForwardingQueryProcessingPool extends ForwardingListeningExecutorService implements QueryProcessingPool
{
  private final ListeningExecutorService delegate;
  @Nullable private final ScheduledExecutorService timeoutService;

  public ForwardingQueryProcessingPool(ExecutorService executorService, @Nullable ScheduledExecutorService timeoutService)
  {
    this.delegate = MoreExecutors.listeningDecorator(executorService);
    if (timeoutService != null) {
      this.timeoutService = MoreExecutors.listeningDecorator(timeoutService);
    } else {
      this.timeoutService = null;
    }
  }

  @VisibleForTesting
  public ForwardingQueryProcessingPool(ExecutorService executorService)
  {
    this(executorService, null);
  }

  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(PrioritizedQueryRunnerCallable<T, V> task)
  {
    return delegate().submit(task);
  }

  /**
   * The timeout only starts counting once a processing thread has actually picked the task off the pool's queue,
   * not when it is submitted. Otherwise a task that sits in the queue behind other segments could exhaust its
   * per-segment timeout without ever having been given a chance to run.
   */
  @Override
  public <T, V> ListenableFuture<T> submitRunnerTask(
      PrioritizedQueryRunnerCallable<T, V> task,
      long timeout,
      TimeUnit unit
  )
  {
    if (timeoutService == null) {
      return submitRunnerTask(task);
    }

    final SettableFuture<Void> started = SettableFuture.create();
    final ListenableFuture<T> execFuture = submitRunnerTask(
        new AbstractPrioritizedQueryRunnerCallable<T, V>(task.getPriority(), task.getRunner())
        {
          @Override
          public T call() throws Exception
          {
            started.set(null);
            return task.call();
          }
        }
    );
    // If the task never gets to run (cancelled or rejected while queued), unblock the transform below so that the
    // returned future completes with the underlying outcome instead of hanging forever.
    execFuture.addListener(() -> started.set(null), MoreExecutors.directExecutor());

    final ListenableFuture<T> timedFuture = Futures.transformAsync(
        started,
        ignored -> Futures.withTimeout(execFuture, timeout, unit, timeoutService),
        MoreExecutors.directExecutor()
    );
    // Cancelling the returned future while the task is still queued must cancel the queued task as well, which the
    // transform cannot do on its own since it is waiting on 'started' rather than on the task itself.
    timedFuture.addListener(
        () -> {
          if (timedFuture.isCancelled()) {
            execFuture.cancel(true);
          }
        },
        MoreExecutors.directExecutor()
    );
    return timedFuture;
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }

  @Override
  public void shutdown()
  {
    super.shutdown(); // shutdown delegate()
    if (timeoutService != null) {
      timeoutService.shutdown();
    }
  }
}
