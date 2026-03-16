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

import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutors
{
  private static final Logger log = new Logger(ScheduledExecutors.class);

  /**
   * Run runnable repeatedly with the given delay between calls, after the given
   * initial delay. Exceptions are caught and logged as errors.
   */
  public static void scheduleWithFixedDelay(
      final ScheduledExecutorService exec,
      final Duration initialDelay,
      final Duration delay,
      final Runnable runnable
  )
  {
    scheduleWithFixedDelay(
        exec,
        initialDelay,
        delay,
        () -> {
          runnable.run(); // (Exceptions are handled for us)
          return Signal.REPEAT;
        }
    );
  }

  /**
   * Run callable repeatedly with the given delay between calls, until it
   * returns Signal.STOP or the executor is shut down. Exceptions are caught
   * and logged as errors, and do not prevent subsequent executions.
   */
  public static void scheduleWithFixedDelay(
      final ScheduledExecutorService exec,
      final Duration initialDelay,
      final Duration delay,
      final Callable<Signal> callable
  )
  {
    log.debug("Scheduling repeatedly: %s with delay %s", callable, delay);
    exec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            Signal signal = Signal.REPEAT;

            try {
              log.trace("Running %s (delay %s)", callable, delay);
              signal = callable.call();
            }
            catch (Throwable e) {
              log.warn(e, "Uncaught exception. Rescheduling.");
            }

            if (signal == Signal.REPEAT && !exec.isShutdown()) {
              log.trace("Rescheduling %s (delay %s)", callable, delay);
              exec.schedule(this, delay.getMillis(), TimeUnit.MILLISECONDS);
            } else {
              log.debug("Stopped rescheduling %s (delay %s)", callable, delay);
            }
          }
        },
        initialDelay.getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Schedules a runnable to execute repeatedly at a fixed rate. The first execution occurs after the initial delay,
   * and subsequent executions are scheduled at fixed intervals measured from the start of each execution.
   * <p>
   * This differs from {@link #scheduleWithFixedDelay} in that the period is measured from the start of each
   * execution rather than from the completion. If an execution takes longer than the period, the next execution
   * will begin immediately after the current one completes.
   * <p>
   * This also differs from {@link ScheduledExecutorService#scheduleAtFixedRate} in that it prevents task pileup:
   * only one future execution is scheduled at a time rather than scheduling all future executions upfront.
   * This prevents a backlog of pending tasks from building up if the executor is delayed or tasks run slowly.
   * <p>
   * Exceptions thrown by the task are caught and logged as errors, and do not prevent subsequent executions.
   * Scheduling also stops if the executor is shut down.
   *
   * @param exec         the ScheduledExecutorService to use for scheduling
   * @param initialDelay the duration to wait before the first execution
   * @param period       the target duration between the start of consecutive executions
   * @param runnable     the task to execute repeatedly
   */
  public static void scheduleAtFixedRate(
      final ScheduledExecutorService exec,
      final Duration initialDelay,
      final Duration period,
      final Runnable runnable
  )
  {
    scheduleAtFixedRate(
        exec,
        initialDelay,
        period,
        () -> {
          runnable.run(); // (Exceptions are handled for us)
          return Signal.REPEAT;
        }
    );
  }

  public static void scheduleAtFixedRate(ScheduledExecutorService exec, Duration rate, Callable<Signal> callable)
  {
    scheduleAtFixedRate(exec, rate, rate, callable);
  }

  public static void scheduleAtFixedRate(
      final ScheduledExecutorService exec,
      final Duration initialDelay,
      final Duration rate,
      final Callable<Signal> callable
  )
  {
    log.debug("Scheduling periodically: %s with period %s", callable, rate);
    exec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            final long startNanos = System.nanoTime();
            Signal signal = Signal.REPEAT;

            try {
              log.trace("Running %s (period %s)", callable, rate);
              signal = callable.call();
            }
            catch (Throwable e) {
              log.warn(e, "Uncaught exception. Rescheduling.");
            }

            if (signal == Signal.REPEAT && !exec.isShutdown()) {
              final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
              final long nextDelay = Math.max(0, rate.getMillis() - elapsedMillis);
              exec.schedule(this, nextDelay, TimeUnit.MILLISECONDS);
            }
          }
        },
        initialDelay.getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  public enum Signal
  {
    REPEAT, STOP
  }

  /**
   * Wraps a {@link ScheduledExecutorService} to emit a metric each time a task from
   * {@link ScheduledExecutorService#schedule} runs. The metric value is the scheduling lag:
   * the difference between the actual delay and the intended delay, in milliseconds, floored at zero.
   *
   * @param exec             the executor to wrap
   * @param emitter          the emitter to emit metrics to
   * @param metricName       the name of the metric to emit
   * @param metricDimensions dimensions to include with the metric
   */
  public static ScheduledExecutorService emittingDelayMetric(
      final ScheduledExecutorService exec,
      final ServiceEmitter emitter,
      final String metricName,
      final Map<String, Object> metricDimensions
  )
  {
    return new DelayMetricEmittingScheduledExecutorService(exec, emitter, metricName, metricDimensions);
  }

  public static ScheduledExecutorFactory createFactory(final Lifecycle lifecycle)
  {
    return (corePoolSize, nameFormat) -> ExecutorServices.manageLifecycle(lifecycle, fixed(corePoolSize, nameFormat));
  }

  /**
   * Creates a new {@link ScheduledExecutorService} with a minimum number of threads.
   *
   * @param corePoolSize the minimum number of threads in the pool
   * @param nameFormat   the naming format for threads created by the pool
   * @return a new {@link ScheduledExecutorService} with the specified configuration
   */
  public static ScheduledExecutorService fixed(int corePoolSize, String nameFormat)
  {
    return Executors.newScheduledThreadPool(corePoolSize, Execs.makeThreadFactory(nameFormat));
  }

  /**
   * Creates a new {@link ScheduledExecutorService} with a minimum number of threads along with a
   * keep-alive time for idle non-core threads.
   * <p>
   */
  public static ScheduledThreadPoolExecutor fixedWithKeepAliveTime(
      int corePoolSize,
      String nameFormat,
      long keepAliveTimeInMillis
  )
  {
    ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(
        corePoolSize,
        Execs.makeThreadFactory(nameFormat)
    );
    scheduledExecutor.setKeepAliveTime(keepAliveTimeInMillis, TimeUnit.MILLISECONDS);
    return scheduledExecutor;
  }
}
