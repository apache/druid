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

import org.apache.druid.error.NotYetImplemented;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ScheduledExecutorService} where all tasks are automatically decorated before being submitted to a
 * delegate executor service. Extends {@link DecoratedExecutorService} to handle the
 * {@link java.util.concurrent.ExecutorService} methods.
 * <p>
 * The JDK {@link #scheduleAtFixedRate} and {@link #scheduleWithFixedDelay} methods are not supported;
 * use {@link ScheduledExecutors} instead.
 */
public class DecoratedScheduledExecutorService extends DecoratedExecutorService
    implements ScheduledExecutorService
{
  private final ScheduledExecutorService scheduledExec;

  public DecoratedScheduledExecutorService(
      final ScheduledExecutorService delegate,
      final Decorator decorator
  )
  {
    super(delegate, decorator);
    this.scheduledExec = delegate;
  }

  @Override
  public ScheduledFuture<?> schedule(final Runnable command, final long delay, final TimeUnit unit)
  {
    return scheduledExec.schedule(decorator.decorateScheduledRunnable(command, delay, unit), delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(final Callable<V> callable, final long delay, final TimeUnit unit)
  {
    return scheduledExec.schedule(decorator.decorateScheduledCallable(callable, delay, unit), delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      final Runnable command,
      final long initialDelay,
      final long period,
      final TimeUnit unit
  )
  {
    throw NotYetImplemented.ex(
        null,
        "Class[%s] does not implement scheduleAtFixedRate, use ScheduledExecutors.scheduleAtFixedRate",
        getClass().getName()
    );
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      final Runnable command,
      final long initialDelay,
      final long delay,
      final TimeUnit unit
  )
  {
    throw NotYetImplemented.ex(
        null,
        "Class[%s] does not implement scheduleWithFixedDelay, use ScheduledExecutors.scheduleWithFixedDelay",
        getClass().getName()
    );
  }
}
