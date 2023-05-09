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

package org.apache.druid.indexing.overlord.duty;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.Duration;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Manages the execution of all overlord duties on a single-threaded executor.
 */
public class OverlordDutyExecutor
{
  private static final Logger log = new Logger(OverlordDutyExecutor.class);

  private final ScheduledExecutorFactory execFactory;
  private final Set<OverlordDuty> duties;

  private volatile ScheduledExecutorService exec;
  private final Object startStopLock = new Object();
  private volatile boolean started = false;

  @Inject
  public OverlordDutyExecutor(
      ScheduledExecutorFactory scheduledExecutorFactory,
      Set<OverlordDuty> duties
  )
  {
    this.execFactory = scheduledExecutorFactory;
    this.duties = duties;
  }

  @LifecycleStart
  public void start()
  {
    synchronized (startStopLock) {
      if (!started) {
        log.info("Starting OverlordDutyExecutor.");
        for (OverlordDuty duty : duties) {
          if (duty.isEnabled()) {
            schedule(duty);
          }
        }

        started = true;
        log.info("OverlordDutyExecutor is now running.");
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopLock) {
      if (started) {
        log.info("Stopping OverlordDutyExecutor.");
        if (exec != null) {
          exec.shutdownNow();
          exec = null;
        }
        started = false;

        log.info("OverlordDutyExecutor has been stopped.");
      }
    }
  }

  /**
   * Schedules execution of the given overlord duty.
   */
  private void schedule(OverlordDuty duty)
  {
    initExecutor();

    final DutySchedule schedule = duty.getSchedule();
    final String dutyName = duty.getClass().getName();

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        Duration.millis(schedule.getInitialDelayMillis()),
        Duration.millis(schedule.getPeriodMillis()),
        () -> {
          try {
            duty.run();
          }
          catch (Exception e) {
            log.error(e, "Error while running duty [%s]", dutyName);
          }
        }
    );

    log.info(
        "Scheduled overlord duty [%s] with initial delay [%d], period [%d].",
        dutyName, schedule.getInitialDelayMillis(), schedule.getPeriodMillis()
    );
  }

  private void initExecutor()
  {
    if (exec == null) {
      final int numThreads = 1;
      exec = execFactory.create(numThreads, "Overlord-Duty-Exec--%d");
      log.info("Initialized duty executor with [%d] threads", numThreads);
    }
  }
}
