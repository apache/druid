/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord.autoscaling;

import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.granularity.PeriodGranularity;
import io.druid.indexing.overlord.RemoteTaskRunner;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.concurrent.ScheduledExecutorService;

/**
 * The ResourceManagementScheduler schedules a check for when worker nodes should potentially be created or destroyed.
 * The ResourceManagementScheduler does not contain the logic to decide whether provision or termination should actually
 * occur. That decision is made in the {@link ResourceManagementStrategy}.
 */
public class ResourceManagementScheduler
{
  private static final Logger log = new Logger(ResourceManagementScheduler.class);

  private final RemoteTaskRunner taskRunner;
  private final ResourceManagementStrategy resourceManagementStrategy;
  private final ResourceManagementSchedulerConfig config;
  private final ScheduledExecutorService exec;

  private final Object lock = new Object();
  private volatile boolean started = false;

  public ResourceManagementScheduler(
      RemoteTaskRunner taskRunner,
      ResourceManagementStrategy resourceManagementStrategy,
      ResourceManagementSchedulerConfig config,
      ScheduledExecutorService exec
  )
  {
    this.taskRunner = taskRunner;
    this.resourceManagementStrategy = resourceManagementStrategy;
    this.config = config;
    this.exec = exec;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Started Resource Management Scheduler");

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          config.getProvisionPeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              resourceManagementStrategy.doProvision(taskRunner);
            }
          }
      );

      // Schedule termination of worker nodes periodically
      Period period = config.getTerminatePeriod();
      PeriodGranularity granularity = new PeriodGranularity(period, config.getOriginTime(), null);
      final long startTime = granularity.next(granularity.truncate(new DateTime().getMillis()));

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          new Duration(System.currentTimeMillis(), startTime),
          config.getTerminatePeriod().toStandardDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              resourceManagementStrategy.doTerminate(taskRunner);
            }
          }
      );

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
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

  public ScalingStats getStats()
  {
    return resourceManagementStrategy.getStats();
  }
}
