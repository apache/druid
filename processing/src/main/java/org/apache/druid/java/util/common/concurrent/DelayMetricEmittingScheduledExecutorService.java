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

import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link DecoratedScheduledExecutorService} that emits a scheduling delay metric each time a
 * task runs. The metric value is the difference between the actual time from submission
 * to execution and the intended delay, in milliseconds, floored at zero.
 * <p>
 * Use {@link ScheduledExecutors#emittingDelayMetric} for convenience.
 */
public class DelayMetricEmittingScheduledExecutorService extends DecoratedScheduledExecutorService
{
  public DelayMetricEmittingScheduledExecutorService(
      final ScheduledExecutorService delegate,
      final ServiceEmitter emitter,
      final String metricName,
      final Map<String, Object> metricDimensions
  )
  {
    super(delegate, new SchedulingDelayDecorator(emitter, metricName, metricDimensions));
  }

  private static class SchedulingDelayDecorator implements Decorator
  {
    private final ServiceEmitter emitter;
    private final String metricName;
    private final Map<String, Object> metricDimensions;

    private SchedulingDelayDecorator(
        final ServiceEmitter emitter,
        final String metricName,
        final Map<String, Object> metricDimensions
    )
    {
      this.emitter = emitter;
      this.metricName = metricName;
      this.metricDimensions = metricDimensions;
    }

    @Override
    public <T> Callable<T> decorateCallable(final Callable<T> callable)
    {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      return () -> {
        emitSchedulingDelay(stopwatch.millisElapsed(), 0);
        return callable.call();
      };
    }

    @Override
    public Runnable decorateRunnable(final Runnable runnable)
    {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      return () -> {
        emitSchedulingDelay(stopwatch.millisElapsed(), 0);
        runnable.run();
      };
    }

    @Override
    public <T> Callable<T> decorateScheduledCallable(
        final Callable<T> callable,
        final long delay,
        final TimeUnit unit
    )
    {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      final long intendedDelayMillis = unit.toMillis(delay);
      return () -> {
        emitSchedulingDelay(stopwatch.millisElapsed(), intendedDelayMillis);
        return callable.call();
      };
    }

    @Override
    public Runnable decorateScheduledRunnable(final Runnable runnable, final long delay, final TimeUnit unit)
    {
      final Stopwatch stopwatch = Stopwatch.createStarted();
      final long intendedDelayMillis = unit.toMillis(delay);
      return () -> {
        emitSchedulingDelay(stopwatch.millisElapsed(), intendedDelayMillis);
        runnable.run();
      };
    }

    private void emitSchedulingDelay(final long actualDelayMillis, final long intendedDelayMillis)
    {
      final long delayMillis = Math.max(0, actualDelayMillis - intendedDelayMillis);

      final ServiceMetricEvent.Builder builder = ServiceMetricEvent.builder();
      for (final Map.Entry<String, Object> entry : metricDimensions.entrySet()) {
        builder.setDimensionIfNotNull(entry.getKey(), entry.getValue());
      }

      emitter.emit(builder.setMetric(metricName, delayMillis));
    }
  }
}
