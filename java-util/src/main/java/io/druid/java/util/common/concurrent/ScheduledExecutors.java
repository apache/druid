/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import org.joda.time.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutors
{
  private static final Logger log = new Logger(ScheduledExecutors.class);

  /**
   * Run runnable repeatedly with the given delay between calls. Exceptions are
   * caught and logged as errors.
   */
  public static void scheduleWithFixedDelay(ScheduledExecutorService exec, Duration delay, Runnable runnable)
  {
    scheduleWithFixedDelay(exec, delay, delay, runnable);
  }

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
        new Callable<Signal>()
        {
          @Override
          public Signal call()
          {
            runnable.run(); // (Exceptions are handled for us)
            return Signal.REPEAT;
          }
        }
    );
  }

  /**
   * Run callable repeatedly with the given delay between calls, after the given
   * initial delay, until it returns Signal.STOP. Exceptions are caught and
   * logged as errors.
   */
  public static void scheduleWithFixedDelay(ScheduledExecutorService exec, Duration delay, Callable<Signal> callable)
  {
    scheduleWithFixedDelay(exec, delay, delay, callable);
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
              log.debug("Running %s (delay %s)", callable, delay);
              if (callable.call() == Signal.REPEAT) {
                log.debug("Rescheduling %s (delay %s)", callable, delay);
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
   * Run runnable once every period. Exceptions are caught and logged as errors.
   */
  public static void scheduleAtFixedRate(ScheduledExecutorService exec, Duration rate, Runnable runnable)
  {
    scheduleAtFixedRate(exec, rate, rate, runnable);
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
      public Signal call() throws Exception
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
              log.debug("Running %s (period %s)", callable, rate);
              prevSignal = callable.call();
            }
            catch(Throwable e) {
              log.error(e, "Uncaught exception.");
            }
          }
        },
        initialDelay.getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  public static enum Signal
  {
    REPEAT, STOP
  }

  public static ScheduledExecutorFactory createFactory(final Lifecycle lifecycle)
  {
    return new ScheduledExecutorFactory()
    {
      public ScheduledExecutorService create(int corePoolSize, String nameFormat)
      {
        return ExecutorServices.manageLifecycle(lifecycle, fixed(corePoolSize, nameFormat));
      }
    };
  }

  public static ScheduledExecutorService fixed(int corePoolSize, String nameFormat)
  {
    return Executors.newScheduledThreadPool(
        corePoolSize, new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build()
    );
  }
}
