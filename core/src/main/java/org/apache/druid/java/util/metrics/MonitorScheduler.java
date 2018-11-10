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

import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class MonitorScheduler
{
  private final MonitorSchedulerConfig config;
  private final ScheduledExecutorService exec;
  private final ServiceEmitter emitter;
  private final Set<Monitor> monitors;
  private final Object lock = new Object();

  private volatile boolean started = false;

  public MonitorScheduler(
      MonitorSchedulerConfig config,
      ScheduledExecutorService exec,
      ServiceEmitter emitter,
      List<Monitor> monitors
  )
  {
    this.config = config;
    this.exec = exec;
    this.emitter = emitter;
    this.monitors = Sets.newHashSet(monitors);
  }

  @LifecycleStart
  public void start()
  {
    synchronized (lock) {
      if (started) {
        return;
      }
      started = true;

      for (final Monitor monitor : monitors) {
        startMonitor(monitor);
      }
    }
  }

  public void addMonitor(final Monitor monitor)
  {
    synchronized (lock) {
      if (!started) {
        throw new ISE("addMonitor must be called after start");
      }
      if (hasMonitor(monitor)) {
        throw new ISE("Monitor already monitoring: %s", monitor);
      }
      monitors.add(monitor);
      startMonitor(monitor);
    }
  }

  public void removeMonitor(final Monitor monitor)
  {
    synchronized (lock) {
      monitors.remove(monitor);
      monitor.stop();
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }

      started = false;
      for (Monitor monitor : monitors) {
        monitor.stop();
      }
    }
  }

  private void startMonitor(final Monitor monitor)
  {
    synchronized (lock) {
      monitor.start();
      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          config.getEmitterPeriod(),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              // Run one more time even if the monitor was removed, in case there's some extra data to flush
              if (monitor.monitor(emitter) && hasMonitor(monitor)) {
                return ScheduledExecutors.Signal.REPEAT;
              } else {
                removeMonitor(monitor);
                return ScheduledExecutors.Signal.STOP;
              }
            }
          }
      );
    }
  }

  private boolean hasMonitor(final Monitor monitor)
  {
    synchronized (lock) {
      return monitors.contains(monitor);
    }
  }
}
