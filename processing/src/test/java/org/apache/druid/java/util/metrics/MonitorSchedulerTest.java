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
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.IOException;

public class MonitorSchedulerTest
{

  @Test
  public void testMonitorAndStopOnRemove() throws IOException
  {
    DruidMonitorSchedulerConfig infiniteFlushDelayConfig = new DruidMonitorSchedulerConfig()
    {
      @Override
      public Duration getEmissionDuration()
      {
        return Duration.millis(Long.MAX_VALUE);
      }
    };

    ServiceEmitter emitter = EasyMock.mock(ServiceEmitter.class);

    Monitor monitor = new AbstractMonitor()
    {
      @Override
      public boolean doMonitor(ServiceEmitter emitter)
      {
        try {
          emitter.flush();
          return true;
        }
        catch (Throwable t) {
          return false;
        }
      }
    };

    MonitorScheduler scheduler = new BasicMonitorScheduler(
        infiniteFlushDelayConfig,
        emitter,
        ImmutableList.of(monitor),
        Execs.scheduledSingleThreaded("MonitorScheduler-%s")
    );
    scheduler.start();

    // Expect an emitter flush, despite infinite scheduler duration, when monitor is removed
    emitter.flush();
    EasyMock.expectLastCall().times(1);

    EasyMock.replay(emitter);

    scheduler.removeMonitor(monitor);

    EasyMock.verify(emitter);
  }
}
