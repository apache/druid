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

import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors.Signal;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@link MonitorScheduler} implementation based on {@link ScheduledExecutorService}.
 */
public class BasicMonitorScheduler extends MonitorScheduler
{
  private final ScheduledExecutorService exec;

  public BasicMonitorScheduler(
      MonitorSchedulerConfig config,
      ServiceEmitter emitter,
      List<Monitor> monitors,
      ScheduledExecutorService exec
  )
  {
    super(config, emitter, monitors);
    this.exec = exec;
  }

  @Override
  void startMonitor(Monitor monitor)
  {
    monitor.start();
    ScheduledExecutors.scheduleAtFixedRate(
        exec,
        getConfig().getEmitterPeriod(),
        () -> {
          // Run one more time even if the monitor was removed, in case there's some extra data to flush
          if (monitor.monitor(getEmitter()) && hasMonitor(monitor)) {
            return Signal.REPEAT;
          } else {
            removeMonitor(monitor);
            return Signal.STOP;
          }
        }
    );
  }
}
