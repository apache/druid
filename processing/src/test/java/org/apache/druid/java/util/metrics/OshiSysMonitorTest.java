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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;
import oshi.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OshiSysMonitorTest
{

  private SystemInfo si;
  private HardwareAbstractionLayer hal;
  private OperatingSystem os;

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
    OshiSysMonitor sysMonitorOshi = createMonitor(new SystemInfo());
    serviceEmitter.start();
    sysMonitorOshi.monitor(serviceEmitter);

    Assert.assertTrue(sysMonitorOshi.doMonitor(serviceEmitter));

  }

  @Test
  public void testDefaultFeedSysMonitorOshi()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    OshiSysMonitor m = createMonitor(new SystemInfo());
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

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorMemStats(emitter);
    m.stop();
    Assert.assertEquals(3, emitter.getNumEmittedEvents());
    emitter.verifyEmitted("sys/mem/max", 1);
    emitter.verifyEmitted("sys/mem/used", 1);
    emitter.verifyEmitted("sys/mem/free", 1);
    emitter.verifyValue("sys/mem/max", 64L);
    emitter.verifyValue("sys/mem/used", 48L);
    emitter.verifyValue("sys/mem/free", 16L);
  }

  @Test
  public void testMemStatsSkipOthers()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    GlobalMemory mem = Mockito.mock(GlobalMemory.class);
    Mockito.when(mem.getTotal()).thenReturn(64L);
    Mockito.when(mem.getAvailable()).thenReturn(16L);
    Mockito.when(hal.getMemory()).thenReturn(mem);

    OshiSysMonitor m = createMonitor(si, ImmutableList.of("mem"));
    m.start();
    m.doMonitor(emitter);
    m.stop();
    Assert.assertEquals(3, emitter.getNumEmittedEvents());
    emitter.verifyEmitted("sys/mem/max", 1);
    emitter.verifyEmitted("sys/mem/used", 1);
    emitter.verifyEmitted("sys/mem/free", 1);
    emitter.verifyEmitted("sys/swap/pageIn", 0);
    emitter.verifyEmitted("sys/fs/max", 0);
  }

  @Test
  public void testJnaBindingsWithRealSystemInfo()
  {
    // Use a real SystemInfo instance rather than a mock to ensure all Oshi/JNA bindings are correctly wired
    OshiSysMonitor m = createMonitor(new SystemInfo(), List.of("mem"));
    StubServiceEmitter emitter = new StubServiceEmitter();

    m.start();
    m.doMonitor(emitter);
    m.stop();

    emitter.verifyEmitted("sys/mem/max", 1);
    emitter.verifyEmitted("sys/mem/used", 1);
    emitter.verifyEmitted("sys/mem/free", 1);
    emitter.verifyEmitted("sys/fs/max", 0);
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

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorSwapStats(emitter);
    Assert.assertEquals(4, emitter.getNumEmittedEvents());
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
    m.monitorSwapStats(emitter);
    emitter.verifyValue("sys/swap/pageIn", 100L);
    emitter.verifyValue("sys/swap/pageOut", 50L);
    emitter.verifyValue("sys/swap/max", 1000L);
    emitter.verifyValue("sys/swap/free", 500L);
    m.stop();
  }

  @Test
  public void testFsStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    FileSystem fileSystem = Mockito.mock(FileSystem.class);
    OSFileStore fs1 = Mockito.mock(OSFileStore.class);
    OSFileStore fs2 = Mockito.mock(OSFileStore.class);
    Mockito.when(fs1.getTotalSpace()).thenReturn(300L);
    Mockito.when(fs1.getUsableSpace()).thenReturn(200L);
    Mockito.when(fs1.getTotalInodes()).thenReturn(1000L);
    Mockito.when(fs1.getFreeInodes()).thenReturn(700L);
    Mockito.when(fs1.getVolume()).thenReturn("/dev/disk1");
    Mockito.when(fs1.getMount()).thenReturn("/System/Volumes/boot1");
    Mockito.when(fs2.getTotalSpace()).thenReturn(400L);
    Mockito.when(fs2.getUsableSpace()).thenReturn(320L);
    Mockito.when(fs2.getTotalInodes()).thenReturn(800L);
    Mockito.when(fs2.getFreeInodes()).thenReturn(600L);
    Mockito.when(fs2.getVolume()).thenReturn("/dev/disk2");
    Mockito.when(fs2.getMount()).thenReturn("/System/Volumes/boot2");
    List<OSFileStore> osFileStores = ImmutableList.of(fs1, fs2);
    Mockito.when(fileSystem.getFileStores(true)).thenReturn(osFileStores);
    Mockito.when(os.getFileSystem()).thenReturn(fileSystem);

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorFsStats(emitter);
    Assert.assertEquals(8, emitter.getNumEmittedEvents());
    emitter.verifyEmitted("sys/fs/max", 2);
    emitter.verifyEmitted("sys/fs/used", 2);
    emitter.verifyEmitted("sys/fs/files/count", 2);
    emitter.verifyEmitted("sys/fs/files/free", 2);
    Map<String, Object> userDims1 = ImmutableMap.of(
        "fsDevName",
        "/dev/disk1",
        "fsDirName",
        "/System/Volumes/boot1"
    );
    List<Number> metricValues1 = emitter.getMetricValues("sys/fs/max", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(300L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/fs/used", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/fs/files/count", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(1000L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/fs/files/free", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(700L, metricValues1.get(0));

    Map<String, Object> userDims2 = ImmutableMap.of(
        "fsDevName",
        "/dev/disk2",
        "fsDirName",
        "/System/Volumes/boot2"
    );
    List<Number> metricValues2 = emitter.getMetricValues("sys/fs/max", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(400L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/fs/used", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(80L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/fs/files/count", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(800L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/fs/files/free", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(600L, metricValues2.get(0));
    m.stop();
  }

  @Test
  public void testDiskStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    HWDiskStore disk1 = Mockito.mock(HWDiskStore.class);
    HWDiskStore disk2 = Mockito.mock(HWDiskStore.class);
    Mockito.when(disk1.getReadBytes()).thenReturn(300L);
    Mockito.when(disk1.getReads()).thenReturn(200L);
    Mockito.when(disk1.getWriteBytes()).thenReturn(400L);
    Mockito.when(disk1.getWrites()).thenReturn(500L);
    Mockito.when(disk1.getCurrentQueueLength()).thenReturn(100L);
    Mockito.when(disk1.getTransferTime()).thenReturn(150L);
    Mockito.when(disk1.getName()).thenReturn("disk1");
    Mockito.when(disk2.getReadBytes()).thenReturn(2000L);
    Mockito.when(disk2.getReads()).thenReturn(3000L);
    Mockito.when(disk2.getWriteBytes()).thenReturn(1000L);
    Mockito.when(disk2.getWrites()).thenReturn(4000L);
    Mockito.when(disk2.getCurrentQueueLength()).thenReturn(750L);
    Mockito.when(disk2.getTransferTime()).thenReturn(800L);
    Mockito.when(disk2.getName()).thenReturn("disk2");
    List<HWDiskStore> hwDiskStores = ImmutableList.of(disk1, disk2);
    Mockito.when(hal.getDiskStores()).thenReturn(hwDiskStores);

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorDiskStats(emitter);
    Assert.assertEquals(0, emitter.getNumEmittedEvents());

    Mockito.when(disk1.getReadBytes()).thenReturn(400L);
    Mockito.when(disk1.getReads()).thenReturn(220L);
    Mockito.when(disk1.getWriteBytes()).thenReturn(600L);
    Mockito.when(disk1.getWrites()).thenReturn(580L);
    Mockito.when(disk1.getCurrentQueueLength()).thenReturn(300L);
    Mockito.when(disk1.getTransferTime()).thenReturn(250L);
    Mockito.when(disk2.getReadBytes()).thenReturn(4500L);
    Mockito.when(disk2.getReads()).thenReturn(3500L);
    Mockito.when(disk2.getWriteBytes()).thenReturn(2300L);
    Mockito.when(disk2.getWrites()).thenReturn(5000L);
    Mockito.when(disk2.getCurrentQueueLength()).thenReturn(900L);
    Mockito.when(disk2.getTransferTime()).thenReturn(1100L);

    m.monitorDiskStats(emitter);
    Assert.assertEquals(12, emitter.getNumEmittedEvents());

    Map<String, Object> userDims1 = ImmutableMap.of(
        "diskName",
        "disk1"
    );
    List<Number> metricValues1 = emitter.getMetricValues("sys/disk/read/size", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/disk/read/count", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(20L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/disk/write/size", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(200L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/disk/write/count", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(80L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/disk/queue", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(200L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/disk/transferTime", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));

    Map<String, Object> userDims2 = ImmutableMap.of(
        "diskName",
        "disk2"
    );
    List<Number> metricValues2 = emitter.getMetricValues("sys/disk/read/size", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(2500L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/disk/read/count", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(500L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/disk/write/size", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(1300L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/disk/write/count", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(1000L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/disk/queue", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(150L, metricValues2.get(0));
    metricValues2 = emitter.getMetricValues("sys/disk/transferTime", userDims2);
    Assert.assertEquals(1, metricValues2.size());
    Assert.assertEquals(300L, metricValues2.get(0));

    m.stop();
  }

  @Test
  public void testNetStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    NetworkIF net1 = Mockito.mock(NetworkIF.class);
    Mockito.when(net1.getBytesRecv()).thenReturn(300L);
    Mockito.when(net1.getPacketsRecv()).thenReturn(200L);
    Mockito.when(net1.getInErrors()).thenReturn(400L);
    Mockito.when(net1.getInDrops()).thenReturn(500L);
    Mockito.when(net1.getBytesSent()).thenReturn(100L);
    Mockito.when(net1.getPacketsSent()).thenReturn(150L);
    Mockito.when(net1.getOutErrors()).thenReturn(200L);
    Mockito.when(net1.getCollisions()).thenReturn(20L);
    Mockito.when(net1.getName()).thenReturn("Wifi");
    Mockito.when(net1.getIPv4addr()).thenReturn(new String[]{"123.456.7.8", "0.0.0.0", "192.1.2.3"});
    Mockito.when(net1.getMacaddr()).thenReturn("ha:rd:wa:re:add");

    List<NetworkIF> networkIFS = ImmutableList.of(net1);
    Mockito.when(hal.getNetworkIFs()).thenReturn(networkIFS);

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorNetStats(emitter);
    Assert.assertEquals(0, emitter.getNumEmittedEvents());

    Mockito.when(net1.getBytesRecv()).thenReturn(400L);
    Mockito.when(net1.getPacketsRecv()).thenReturn(220L);
    Mockito.when(net1.getInErrors()).thenReturn(600L);
    Mockito.when(net1.getInDrops()).thenReturn(580L);
    Mockito.when(net1.getBytesSent()).thenReturn(300L);
    Mockito.when(net1.getPacketsSent()).thenReturn(250L);
    Mockito.when(net1.getOutErrors()).thenReturn(330L);
    Mockito.when(net1.getCollisions()).thenReturn(240L);


    m.monitorNetStats(emitter);
    Assert.assertEquals(16, emitter.getNumEmittedEvents()); // 8 * 2 whitelisted ips

    Map<String, Object> userDims1 = ImmutableMap.of(
        "netName",
        "Wifi",
        "netAddress",
        "123.456.7.8",
        "netHwaddr",
        "ha:rd:wa:re:add"
    );
    Map<String, Object> userDims2 = ImmutableMap.of(
        "netName",
        "Wifi",
        "netAddress",
        "192.1.2.3",
        "netHwaddr",
        "ha:rd:wa:re:add"
    );
    List<Number> metricValues1 = emitter.getMetricValues("sys/net/read/size", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/read/packets", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(20L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/read/errors", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(200L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/read/dropped", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(80L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/size", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(200L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/packets", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/errors", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(130L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/collisions", userDims1);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(220L, metricValues1.get(0));

    metricValues1 = emitter.getMetricValues("sys/net/read/size", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/read/packets", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(20L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/read/errors", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(200L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/read/dropped", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(80L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/size", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(200L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/packets", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(100L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/errors", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(130L, metricValues1.get(0));
    metricValues1 = emitter.getMetricValues("sys/net/write/collisions", userDims2);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(220L, metricValues1.get(0));
    m.stop();
  }

  @Test
  public void testCpuStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    CentralProcessor processor = Mockito.mock(CentralProcessor.class);
    long[][] procTicks = new long[][]{
        {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L},
        {2L, 4L, 6L, 8L, 10L, 12L, 14L, 16L},
        };
    Mockito.when(processor.getProcessorCpuLoadTicks()).thenReturn(procTicks);
    Mockito.when(hal.getProcessor()).thenReturn(processor);

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorCpuStats(emitter);
    Assert.assertEquals(0, emitter.getNumEmittedEvents());

    long[][] procTicks2 = new long[][]{
        {4L, 5L, 6L, 8L, 9L, 7L, 10L, 12L},     // Δtick1 {3,3,3,4,4,1,3,4} _total = 25, emitted percentage
        {5L, 8L, 8L, 10L, 15L, 14L, 18L, 22L},  // Δtick2 {3,4,2,2,5,2,4,6} _total = 28
    };
    Mockito.when(processor.getProcessorCpuLoadTicks()).thenReturn(procTicks2);

    m.monitorCpuStats(emitter);
    m.stop();
    Assert.assertEquals(16, emitter.getNumEmittedEvents()); // 8 ticktype * 2 processors

    Map<String, Object> userDims = new HashMap<>();
    userDims.put("cpuName", "0");
    userDims.put("cpuTime", "user");
    List<Number> metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(12L, metricValues1.get(0));
    userDims.replace("cpuTime", "nice");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(12L, metricValues1.get(0));
    userDims.replace("cpuTime", "sys");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(12L, metricValues1.get(0));
    userDims.replace("cpuTime", "idle");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(16L, metricValues1.get(0));
    userDims.replace("cpuTime", "wait");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(16L, metricValues1.get(0));
    userDims.replace("cpuTime", "irq");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(4L, metricValues1.get(0));
    userDims.replace("cpuTime", "softIrq");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(12L, metricValues1.get(0));
    userDims.replace("cpuTime", "stolen");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(16L, metricValues1.get(0));

    userDims.replace("cpuName", "1");
    userDims.replace("cpuTime", "user");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(10L, metricValues1.get(0));
    userDims.replace("cpuTime", "nice");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(14L, metricValues1.get(0));
    userDims.replace("cpuTime", "sys");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(7L, metricValues1.get(0));
    userDims.replace("cpuTime", "idle");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(7L, metricValues1.get(0));
    userDims.replace("cpuTime", "wait");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(17L, metricValues1.get(0));
    userDims.replace("cpuTime", "irq");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(7L, metricValues1.get(0));
    userDims.replace("cpuTime", "softIrq");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(14L, metricValues1.get(0));
    userDims.replace("cpuTime", "stolen");
    metricValues1 = emitter.getMetricValues("sys/cpu", userDims);
    Assert.assertEquals(1, metricValues1.size());
    Assert.assertEquals(21L, metricValues1.get(0));

  }

  @Test
  public void testSysStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");

    Mockito.when(os.getSystemUptime()).thenReturn(4000L);
    CentralProcessor processor = Mockito.mock(CentralProcessor.class);
    double[] la = new double[]{2.31, 4.31, 5.31};
    Mockito.when(processor.getSystemLoadAverage(3)).thenReturn(la);
    Mockito.when(hal.getProcessor()).thenReturn(processor);

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorSysStats(emitter);
    Assert.assertEquals(4, emitter.getNumEmittedEvents());
    m.stop();
    emitter.verifyEmitted("sys/uptime", 1);
    emitter.verifyEmitted("sys/la/1", 1);
    emitter.verifyEmitted("sys/la/5", 1);
    emitter.verifyEmitted("sys/la/15", 1);
    emitter.verifyValue("sys/uptime", 4000L);
    emitter.verifyValue("sys/la/1", 2.31);
    emitter.verifyValue("sys/la/5", 4.31);
    emitter.verifyValue("sys/la/15", 5.31);

  }

  @Test
  public void testTcpStats()
  {
    StubServiceEmitter emitter = new StubServiceEmitter("dev/monitor-test", "localhost:0000");
    InternetProtocolStats.TcpStats tcpv4 = Mockito.mock(InternetProtocolStats.TcpStats.class);
    InternetProtocolStats ipstats = Mockito.mock(InternetProtocolStats.class);
    Mockito.when(tcpv4.getConnectionsActive()).thenReturn(10L);
    Mockito.when(tcpv4.getConnectionsPassive()).thenReturn(20L);
    Mockito.when(tcpv4.getConnectionFailures()).thenReturn(5L);
    Mockito.when(tcpv4.getConnectionsReset()).thenReturn(7L);
    Mockito.when(tcpv4.getSegmentsReceived()).thenReturn(200L);
    Mockito.when(tcpv4.getInErrors()).thenReturn(3L);
    Mockito.when(tcpv4.getSegmentsSent()).thenReturn(300L);
    Mockito.when(tcpv4.getOutResets()).thenReturn(4L);
    Mockito.when(tcpv4.getSegmentsRetransmitted()).thenReturn(8L);
    Mockito.when(ipstats.getTCPv4Stats()).thenReturn(tcpv4);
    Mockito.when(os.getInternetProtocolStats()).thenReturn(ipstats);

    OshiSysMonitor m = createMonitor(si);
    m.start();
    m.monitorTcpStats(emitter);

    Assert.assertEquals(0, emitter.getNumEmittedEvents());
    Mockito.when(tcpv4.getConnectionsActive()).thenReturn(20L);
    Mockito.when(tcpv4.getConnectionsPassive()).thenReturn(25L);
    Mockito.when(tcpv4.getConnectionFailures()).thenReturn(8L);
    Mockito.when(tcpv4.getConnectionsReset()).thenReturn(14L);
    Mockito.when(tcpv4.getSegmentsReceived()).thenReturn(350L);
    Mockito.when(tcpv4.getInErrors()).thenReturn(4L);
    Mockito.when(tcpv4.getSegmentsSent()).thenReturn(500L);
    Mockito.when(tcpv4.getOutResets()).thenReturn(7L);
    Mockito.when(tcpv4.getSegmentsRetransmitted()).thenReturn(8L);
    m.monitorTcpStats(emitter);
    m.stop();
    Assert.assertEquals(9, emitter.getNumEmittedEvents());
    emitter.verifyValue("sys/tcpv4/activeOpens", 10L);
    emitter.verifyValue("sys/tcpv4/passiveOpens", 5L);
    emitter.verifyValue("sys/tcpv4/attemptFails", 3L);
    emitter.verifyValue("sys/tcpv4/estabResets", 7L);
    emitter.verifyValue("sys/tcpv4/in/segs", 150L);
    emitter.verifyValue("sys/tcpv4/in/errs", 1L);
    emitter.verifyValue("sys/tcpv4/out/segs", 200L);
    emitter.verifyValue("sys/tcpv4/out/rsts", 3L);
    emitter.verifyValue("sys/tcpv4/retrans/segs", 0L);

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

  private OshiSysMonitor createMonitor(SystemInfo si)
  {
    return createMonitor(si, ImmutableList.of());
  }

  private OshiSysMonitor createMonitor(SystemInfo si, List<String> categories)
  {
    return new OshiSysMonitor(new OshiSysMonitorConfig(categories), si);
  }
}
