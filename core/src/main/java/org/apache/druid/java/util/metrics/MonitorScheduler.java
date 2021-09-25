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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;

import java.util.List;
import java.util.Optional;
import java.util.Set;


/**
 */
public abstract class MonitorScheduler
{
  private final MonitorSchedulerConfig config;
  private final ServiceEmitter emitter;
  private final Set<Monitor> monitors;
  private final Object lock = new Object();

  private volatile boolean started = false;
  
  MonitorScheduler(
      MonitorSchedulerConfig config,
      ServiceEmitter emitter,
      List<Monitor> monitors
  )
  {
    this.config = config;
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

  /**
   * Returns a {@link Monitor} instance of the given class if any. Note that this method searches for the monitor
   * from the current snapshot of {@link #monitors}.
   */
  public <T extends Monitor> Optional<T> findMonitor(Class<T> monitorClass)
  {
    synchronized (lock) {
      return (Optional<T>) monitors.stream().filter(m -> m.getClass() == monitorClass).findFirst();
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

  boolean hasMonitor(final Monitor monitor)
  {
    synchronized (lock) {
      return monitors.contains(monitor);
    }
  }

  MonitorSchedulerConfig getConfig()
  {
    return config;
  }

  ServiceEmitter getEmitter()
  {
    return emitter;
  }

  @GuardedBy("lock")
  abstract void startMonitor(Monitor monitor);
}
