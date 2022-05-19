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

package org.apache.druid.java.util.metrics;

import io.timeandspace.cronscheduler.CronScheduler;
import io.timeandspace.cronscheduler.CronTask;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link MonitorScheduler} implementation based on {@link CronScheduler}.
 */
public class ClockDriftSafeMonitorScheduler extends MonitorScheduler
{
  private static final Logger LOG = new Logger(ClockDriftSafeMonitorScheduler.class);

  private final CronScheduler monitorScheduler;
  private final ExecutorService monitorRunner;

  public ClockDriftSafeMonitorScheduler(
      MonitorSchedulerConfig config,
      ServiceEmitter emitter,
      List<Monitor> monitors,
      CronScheduler monitorScheduler,
      ExecutorService monitorRunner
  )
  {
    super(config, emitter, monitors);
    this.monitorScheduler = monitorScheduler;
    this.monitorRunner = monitorRunner;
  }

  @Override
  void startMonitor(final Monitor monitor)
  {
    monitor.start();
    long rate = getConfig().getEmitterPeriod().getMillis();
    final AtomicReference<Future<?>> futureReference = new AtomicReference<>();
    Future<?> future = monitorScheduler.scheduleAtFixedRate(
        rate,
        rate,
        TimeUnit.MILLISECONDS,
        new CronTask()
        {
          private Future<?> cancellationFuture = null;
          private Future<Boolean> monitorFuture = null;

          @Override
          public void run(long scheduledRunTimeMillis)
          {
            waitForScheduleFutureToBeSet();
            if (cancellationFuture == null) {
              LOG.error("scheduleFuture is not set. Can't run monitor[%s]", monitor.getClass().getName());
              return;
            }
            try {
              // Do nothing if the monitor is still running.
              if (monitorFuture == null || monitorFuture.isDone()) {
                if (monitorFuture != null) {
                  // monitorFuture must be done at this moment if it's not null
                  if (!(monitorFuture.get() && hasMonitor(monitor))) {
                    stopMonitor(monitor);
                    return;
                  }
                }

                LOG.trace("Running monitor[%s]", monitor.getClass().getName());
                monitorFuture = monitorRunner.submit(() -> {
                  try {
                    return monitor.monitor(getEmitter());
                  }
                  catch (Throwable e) {
                    LOG.error(
                        e,
                        "Exception while executing monitor[%s]. Rescheduling in %s ms",
                        monitor.getClass().getName(),
                        rate
                    );
                    return Boolean.TRUE;
                  }
                });
              }
            }
            catch (Throwable e) {
              LOG.error(e, "Uncaught exception.");
            }
          }

          private void waitForScheduleFutureToBeSet()
          {
            if (cancellationFuture == null) {
              while (!Thread.currentThread().isInterrupted()) {
                if (futureReference.get() != null) {
                  cancellationFuture = futureReference.get();
                  break;
                }
              }
            }
          }

          private void stopMonitor(Monitor monitor)
          {
            removeMonitor(monitor);
            cancellationFuture.cancel(false);
            LOG.debug("Stopped monitor[%s]", monitor.getClass().getName());
          }
        }
    );
    futureReference.set(future);
  }
}
