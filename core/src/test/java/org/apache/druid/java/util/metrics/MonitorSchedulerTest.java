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
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;

public class MonitorSchedulerTest
{
  @Test
  public void testFindMonitor()
  {
    class Monitor1 extends NoopMonitor
    {
    }
    class Monitor2 extends NoopMonitor
    {
    }
    class Monitor3 extends NoopMonitor
    {
    }

    final Monitor1 monitor1 = new Monitor1();
    final Monitor2 monitor2 = new Monitor2();

    final MonitorScheduler scheduler = new MonitorScheduler(
        Mockito.mock(MonitorSchedulerConfig.class),
        Execs.scheduledSingleThreaded("monitor-scheduler-test"),
        Mockito.mock(ServiceEmitter.class),
        ImmutableList.of(monitor1, monitor2)
    );

    final Optional<Monitor1> maybeFound1 = scheduler.findMonitor(Monitor1.class);
    final Optional<Monitor2> maybeFound2 = scheduler.findMonitor(Monitor2.class);
    Assert.assertTrue(maybeFound1.isPresent());
    Assert.assertTrue(maybeFound2.isPresent());
    Assert.assertSame(monitor1, maybeFound1.get());
    Assert.assertSame(monitor2, maybeFound2.get());

    Assert.assertFalse(scheduler.findMonitor(Monitor3.class).isPresent());
  }

  private static class NoopMonitor implements Monitor
  {
    @Override
    public void start()
    {

    }

    @Override
    public void stop()
    {

    }

    @Override
    public boolean monitor(ServiceEmitter emitter)
    {
      return true;
    }
  }
}
