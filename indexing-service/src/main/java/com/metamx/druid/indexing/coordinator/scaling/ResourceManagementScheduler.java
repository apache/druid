/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexing.coordinator.scaling;

import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.PeriodGranularity;
import com.metamx.druid.indexing.coordinator.TaskRunner;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.concurrent.ScheduledExecutorService;

/**
 * The ResourceManagementScheduler schedules a check for when worker nodes should potentially be created or destroyed.
 * It uses a {@link TaskRunner} to return all pending tasks in the system and the status of the worker nodes in
 * the system.
 * The ResourceManagementScheduler does not contain the logic to decide whether provision or termination should actually
 * occur. That decision is made in the {@link ResourceManagementStrategy}.
 */
public class ResourceManagementScheduler
{
  private static final Logger log = new Logger(ResourceManagementScheduler.class);

  private final TaskRunner taskRunner;
  private final ResourceManagementStrategy resourceManagementStrategy;
  private final ResourceManagementSchedulerConfig config;
  private final ScheduledExecutorService exec;

  private final Object lock = new Object();
  private volatile boolean started = false;

  public ResourceManagementScheduler(
      TaskRunner taskRunner,
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
          config.getProvisionResourcesDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              resourceManagementStrategy.doProvision(
                  taskRunner.getPendingTasks(),
                  taskRunner.getWorkers()
              );
            }
          }
      );

      // Schedule termination of worker nodes periodically
      Period period = new Period(config.getTerminateResourcesDuration());
      PeriodGranularity granularity = new PeriodGranularity(period, config.getTerminateResourcesOriginDateTime(), null);
      final long startTime = granularity.next(granularity.truncate(new DateTime().getMillis()));

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          new Duration(
              System.currentTimeMillis(),
              startTime
          ),
          config.getTerminateResourcesDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              resourceManagementStrategy.doTerminate(
                  taskRunner.getPendingTasks(),
                  taskRunner.getWorkers()
              );
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
