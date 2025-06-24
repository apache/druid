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
import org.joda.time.Duration;

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
        new Callable<>()
        {
          @Override
          public Signal call()
          {
            runnable.run(); // (Exceptions are handled for us)
            if (exec.isShutdown()) {
              log.warn("ScheduledExecutorService is ShutDown. Return 'Signal.STOP' and stopped rescheduling %s (delay %s)", this, delay);
              return Signal.STOP;
            } else {
              return Signal.REPEAT;
            }
          }
        }
    );
  }

  /**
   * Run callable repeatedly with the given delay between calls, until it
   * returns Signal.STOP. Exceptions are caught and logged as errors.
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
            try {
              log.trace("Running %s (delay %s)", callable, delay);
              if (callable.call() == Signal.REPEAT) {
                log.trace("Rescheduling %s (delay %s)", callable, delay);
                exec.schedule(this, delay.getMillis(), TimeUnit.MILLISECONDS);
              } else {
                log.debug("Stopped rescheduling %s (delay %s)", callable, delay);
              }
            }
            catch (Throwable e) {
              log.error(e, "Uncaught exception.");
            }
          }
        },
        initialDelay.getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Run runnable once every period, after the given initial delay. Exceptions
   * are caught and logged as errors.
   */
  public static void scheduleAtFixedRate(
      final ScheduledExecutorService exec,
      final Duration initialDelay,
      final Duration period,
      final Runnable runnable
  )
  {
    scheduleAtFixedRate(exec, initialDelay, period, new Callable<Signal>()
    {
      @Override
      public Signal call()
      {
        runnable.run();
        return Signal.REPEAT;
      }
    });
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
          private volatile Signal prevSignal = null;

          @Override
          public void run()
          {
            if (prevSignal == null || prevSignal == Signal.REPEAT) {
              exec.schedule(this, rate.getMillis(), TimeUnit.MILLISECONDS);
            }

            try {
              log.trace("Running %s (period %s)", callable, rate);
              prevSignal = callable.call();
            }
            catch (Throwable e) {
              log.error(e, "Uncaught exception.");
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
