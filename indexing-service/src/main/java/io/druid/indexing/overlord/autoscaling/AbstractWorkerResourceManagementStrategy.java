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

package io.druid.indexing.overlord.autoscaling;

import com.metamx.emitter.EmittingLogger;
import io.druid.granularity.PeriodGranularity;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.java.util.common.concurrent.ScheduledExecutors;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.concurrent.ScheduledExecutorService;

/**
 */
public abstract class AbstractWorkerResourceManagementStrategy implements ResourceManagementStrategy<WorkerTaskRunner>
{
  private static final EmittingLogger log = new EmittingLogger(AbstractWorkerResourceManagementStrategy.class);

  private final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig;
  private final ScheduledExecutorService exec;
  private final Object lock = new Object();

  private volatile boolean started = false;

  protected AbstractWorkerResourceManagementStrategy(
      ResourceManagementSchedulerConfig resourceManagementSchedulerConfig,
      ScheduledExecutorService exec
  )
  {
    this.resourceManagementSchedulerConfig = resourceManagementSchedulerConfig;
    this.exec = exec;
  }

  @Override
  public void startManagement(final WorkerTaskRunner runner)
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Started Resource Management Scheduler");

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          resourceManagementSchedulerConfig.getProvisionPeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              // Any Errors are caught by ScheduledExecutors
              doProvision(runner);
            }
          }
      );

      // Schedule termination of worker nodes periodically
      Period period = resourceManagementSchedulerConfig.getTerminatePeriod();
      PeriodGranularity granularity = new PeriodGranularity(
          period,
          resourceManagementSchedulerConfig.getOriginTime(),
          null
      );
      final long startTime = granularity.next(granularity.truncate(new DateTime().getMillis()));

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          new Duration(System.currentTimeMillis(), startTime),
          resourceManagementSchedulerConfig.getTerminatePeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              // Any Errors are caught by ScheduledExecutors
              doTerminate(runner);
            }
          }
      );

      started = true;

    }
  }

  abstract boolean doTerminate(WorkerTaskRunner runner);

  abstract boolean doProvision(WorkerTaskRunner runner);

  @Override
  public void stopManagement()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }
      log.info("Stopping Resource Management Scheduler");
      exec.shutdown();
      started = false;
    }
  }

}
