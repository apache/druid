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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.VirtualMemory;
import oshi.software.os.OperatingSystem;
import oshi.util.Util;

import java.util.List;

public class OshiSysMonitorTest
{

  private SystemInfo si;
  private HardwareAbstractionLayer hal;
  private OperatingSystem os;

  private enum STATS
  {
    MEM, SWAP, FS, DISK, NET, CPU, SYS, TCP
  }

  @Before
  public void setUp()
  {
    si = Mockito.mock(SystemInfo.class);
    hal = Mockito.mock(HardwareAbstractionLayer.class);
    os = Mockito.mock(OperatingSystem.class);
    Mockito.when(si.getHardware()).thenReturn(hal);
    Mockito.when(si.getOperatingSystem()).thenReturn(os);
  }

  @Test
  public void testDoMonitor()
  {

    ServiceEmitter serviceEmitter = Mockito.mock(ServiceEmitter.class);
    OshiSysMonitor sysMonitorOshi = new OshiSysMonitor();
    serviceEmitter.start();
    sysMonitorOshi.monitor(serviceEmitter);

    Assert.assertTrue(sysMonitorOshi.doMonitor(serviceEmitter));

  }

  @Test
  public void testDefaultFeedSysMonitorOshi()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    OshiSysMonitor m = new OshiSysMonitor();
    m.start();
    m.monitor(emitter);
    // Sleep for 2 sec to get all metrics which are difference of prev and now metrics
    Util.sleep(2000);
    m.monitor(emitter);
    m.stop();
    checkEvents(emitter.getEvents(), "metrics");
  }

  @Test
  public void testMemStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    GlobalMemory mem = Mockito.mock(GlobalMemory.class);
    Mockito.when(mem.getTotal()).thenReturn(64L);
    Mockito.when(mem.getAvailable()).thenReturn(16L);
    Mockito.when(hal.getMemory()).thenReturn(mem);

    OshiSysMonitor m = new OshiSysMonitor(si);
    m.start();
    m.monitorStats(STATS.MEM.ordinal(), emitter);
    m.stop();
    Assert.assertEquals(3, emitter.getEvents().size());
    emitter.verifyEmitted("sys/mem/max", 1);
    emitter.verifyEmitted("sys/mem/used", 1);
    emitter.verifyEmitted("sys/mem/free", 1);
    emitter.verifyValue("sys/mem/max", 64L);
    emitter.verifyValue("sys/mem/used", 48L);
    emitter.verifyValue("sys/mem/free", 16L);
  }

  @Test
  public void testSwapStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    GlobalMemory mem = Mockito.mock(GlobalMemory.class);
    VirtualMemory swap = Mockito.mock(VirtualMemory.class);
    Mockito.when(swap.getSwapPagesIn()).thenReturn(300L);
    Mockito.when(swap.getSwapPagesOut()).thenReturn(200L);
    Mockito.when(swap.getSwapTotal()).thenReturn(1000L);
    Mockito.when(swap.getSwapUsed()).thenReturn(700L);
    Mockito.when(mem.getVirtualMemory()).thenReturn(swap);
    Mockito.when(hal.getMemory()).thenReturn(mem);

    OshiSysMonitor m = new OshiSysMonitor(si);
    m.start();
    m.monitorStats(STATS.SWAP.ordinal(), emitter);
    Assert.assertEquals(4, emitter.getEvents().size());
    emitter.verifyEmitted("sys/swap/pageIn", 1);
    emitter.verifyEmitted("sys/swap/pageOut", 1);
    emitter.verifyEmitted("sys/swap/max", 1);
    emitter.verifyEmitted("sys/swap/free", 1);
    emitter.verifyValue("sys/swap/pageIn", 300L);
    emitter.verifyValue("sys/swap/pageOut", 200L);
    emitter.verifyValue("sys/swap/max", 1000L);
    emitter.verifyValue("sys/swap/free", 300L);
    // Emit again to assert diff in pageIn stats
    Mockito.when(swap.getSwapPagesIn()).thenReturn(400L);
    Mockito.when(swap.getSwapPagesOut()).thenReturn(250L);
    Mockito.when(swap.getSwapUsed()).thenReturn(500L);
    emitter.flush();
    m.monitorStats(STATS.SWAP.ordinal(), emitter);
    emitter.verifyValue("sys/swap/pageIn", 100L);
    emitter.verifyValue("sys/swap/pageOut", 50L);
    emitter.verifyValue("sys/swap/max", 1000L);
    emitter.verifyValue("sys/swap/free", 500L);
    m.stop();
  }

  private void checkEvents(List<Event> events, String expectedFeed)
  {
    Assert.assertFalse("no events emitted", events.isEmpty());
    for (Event e : events) {
      if (!expectedFeed.equals(e.getFeed())) {
        String message = StringUtils.format("\"feed\" in event: %s", e.toMap().toString());
        Assert.assertEquals(message, expectedFeed, e.getFeed());
      }
    }
  }


}
