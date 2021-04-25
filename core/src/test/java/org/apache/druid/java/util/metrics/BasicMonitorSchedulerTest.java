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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.concurrent.ScheduledExecutorService;

public class BasicMonitorSchedulerTest
{
  private final MonitorSchedulerConfig config = new MonitorSchedulerConfig()
  {
    @Override
    public Duration getEmitterPeriod()
    {
      return Duration.millis(5);
    }
  };
  private ServiceEmitter emitter;
  private ScheduledExecutorService exec;

  @Before
  public void setup()
  {
    emitter = Mockito.mock(ServiceEmitter.class);
    exec = Execs.scheduledSingleThreaded("BasicMonitorSchedulerTest");
  }

  @Test
  public void testStart_RepeatScheduling() throws InterruptedException
  {
    final Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.monitor(ArgumentMatchers.any())).thenReturn(true);

    final BasicMonitorScheduler scheduler = new BasicMonitorScheduler(
        config,
        emitter,
        ImmutableList.of(monitor),
        exec
    );
    scheduler.start();
    Thread.sleep(100);
    Mockito.verify(monitor, Mockito.atLeast(2)).monitor(ArgumentMatchers.any());
    scheduler.stop();
  }

  @Test
  public void testStart_RepeatAndStopScheduling() throws InterruptedException
  {
    final Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.monitor(ArgumentMatchers.any())).thenReturn(true, true, true, false);

    final BasicMonitorScheduler scheduler = new BasicMonitorScheduler(
        config,
        emitter,
        ImmutableList.of(monitor),
        exec
    );
    scheduler.start();
    Thread.sleep(100);
    // monitor.monitor() is called 5 times since a new task is scheduled first and then the current one is executed.
    // See ScheduledExecutors.scheduleAtFixedRate() for details.
    Mockito.verify(monitor, Mockito.times(5)).monitor(ArgumentMatchers.any());
    scheduler.stop();
  }

  @Test
  public void testStart_UnexpectedExceptionWhileMonitoring_ContinueMonitor() throws InterruptedException
  {
    final Monitor monitor = Mockito.mock(Monitor.class);
    Mockito.when(monitor.monitor(ArgumentMatchers.any()))
           .thenThrow(new RuntimeException("Test throwing exception while monitoring"));

    final BasicMonitorScheduler scheduler = new BasicMonitorScheduler(
        config,
        emitter,
        ImmutableList.of(monitor),
        exec
    );
    scheduler.start();
    Thread.sleep(100);
    // monitor.monitor() is called 5 times since a new task is scheduled first and then the current one is executed.
    // See ScheduledExecutors.scheduleAtFixedRate() for details.
    Mockito.verify(monitor, Mockito.atLeast(2)).monitor(ArgumentMatchers.any());
    scheduler.stop();
  }
}
