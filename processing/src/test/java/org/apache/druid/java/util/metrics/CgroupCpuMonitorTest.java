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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.CgroupVersion;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CgroupCpuMonitorTest
{
  @TempDir
  private Path tempDir;
  private File procDir;
  private File cgroupDir;
  private File statFile;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws IOException
  {
    cgroupDir = FileUtils.createTempDirInLocation(tempDir, "cgroupDir");
    procDir = FileUtils.createTempDirInLocation(tempDir, "procDir");
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File cpuDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );

    FileUtils.mkdirp(cpuDir);
    statFile = new File(cpuDir, "cpuacct.stat");
    TestUtils.copyOrReplaceResource("/cpu.shares", new File(cpuDir, "cpu.shares"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_quota_us", new File(cpuDir, "cpu.cfs_quota_us"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_period_us", new File(cpuDir, "cpu.cfs_period_us"));
    TestUtils.copyOrReplaceResource("/cpuacct.stat", statFile);
  }

  @Test
  public void testMonitor() throws IOException, InterruptedException
  {
    final CgroupCpuMonitor monitor = new CgroupCpuMonitor(discoverer, "some_feed");
    final StubServiceEmitter emitter = StubServiceEmitter.createStarted();
    Assertions.assertTrue(monitor.doMonitor(emitter));
    final List<Event> actualEvents = emitter.getEvents();
    Assertions.assertEquals(2, actualEvents.size());
    final Map<String, Object> sharesEvent = actualEvents.get(0).toMap();
    final Map<String, Object> coresEvent = actualEvents.get(1).toMap();
    Assertions.assertEquals("cgroup/cpu/shares", sharesEvent.get("metric"));
    Assertions.assertEquals(1024L, sharesEvent.get("value"));
    Assertions.assertEquals("cgroup/cpu/cores_quota", coresEvent.get("metric"));
    Assertions.assertEquals(3.0D, coresEvent.get("value"));
    emitter.flush();

    TestUtils.copyOrReplaceResource("/cpuacct.stat-2", statFile);
    // We need to pass atleast a second for the calculation to trigger
    // to avoid divide by zero.
    Thread.sleep(1000);

    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertTrue(
        emitter
            .getEvents()
            .stream()
            .map(e -> e.toMap().get("metric"))
            .collect(Collectors.toList())
            .containsAll(
                ImmutableSet.of(
                    CgroupUtil.CPU_TOTAL_USAGE_METRIC,
                    CgroupUtil.CPU_USER_USAGE_METRIC,
                    CgroupUtil.CPU_SYS_USAGE_METRIC
                )));
  }

  @Test
  public void testQuotaCompute()
  {
    Assertions.assertEquals(-1, CgroupUtil.computeProcessorQuota(-1, 100000), 0);
    Assertions.assertEquals(0, CgroupUtil.computeProcessorQuota(0, 100000), 0);
    Assertions.assertEquals(-1, CgroupUtil.computeProcessorQuota(100000, 0), 0);
    Assertions.assertEquals(2.0D, CgroupUtil.computeProcessorQuota(200000, 100000), 0);
    Assertions.assertEquals(0.5D, CgroupUtil.computeProcessorQuota(50000, 100000), 0);
  }

  @Test
  public void testCgroupsV2Detection() throws IOException, URISyntaxException
  {
    // Set up cgroups v2 structure
    File cgroupV2Dir = FileUtils.createTempDirInLocation(tempDir, "cgroupV2Dir");
    File procV2Dir = FileUtils.createTempDirInLocation(tempDir, "procV2Dir");
    TestUtils.setUpCgroupsV2(procV2Dir, cgroupV2Dir);


    CgroupDiscoverer v2Discoverer = ProcSelfCgroupDiscoverer.autoCgroupDiscoverer(procV2Dir.toPath());
    
    // Constructor should detect v2 and log warning
    CgroupCpuMonitor monitor = new CgroupCpuMonitor(v2Discoverer, "test-feed");
    
    final StubServiceEmitter emitter = StubServiceEmitter.createStarted();

    // doMonitor should return true
    Assertions.assertTrue(monitor.doMonitor(emitter));

    Assertions.assertEquals(2, emitter.getEvents().size());
    Assertions.assertEquals(CgroupVersion.V2.name(), emitter.getEvents().get(0).toMap().get("cgroupversion"));
  }

  @Test  
  public void testCgroupsV1MonitoringContinuesNormally() throws IOException, InterruptedException
  {
    // This test verifies that the existing v1 monitoring continues to work
    // after the v2 detection changes
    final CgroupCpuMonitor monitor = new CgroupCpuMonitor(discoverer, "some_feed");
    final StubServiceEmitter emitter = StubServiceEmitter.createStarted();
    
    Assertions.assertTrue(monitor.doMonitor(emitter));
    final List<Event> actualEvents = emitter.getEvents();

    // Should emit metrics normally for v1
    Assertions.assertEquals(2, actualEvents.size());
    final Map<String, Object> sharesEvent = actualEvents.get(0).toMap();
    final Map<String, Object> coresEvent = actualEvents.get(1).toMap();
    Assertions.assertEquals("cgroup/cpu/shares", sharesEvent.get("metric"));
    Assertions.assertEquals(1024L, sharesEvent.get("value"));
    Assertions.assertEquals("cgroup/cpu/cores_quota", coresEvent.get("metric"));
    Assertions.assertEquals(3.0D, coresEvent.get("value"));
    Assertions.assertEquals(CgroupVersion.V1.name(), coresEvent.get("cgroupversion"));
  }
}
