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

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.CgroupVersion;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CgroupCpuSetMonitorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File cpusetDir = new File(
        cgroupDir,
        "cpuset/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );

    FileUtils.mkdirp(cpusetDir);
    TestUtils.copyOrReplaceResource("/cpuset.cpus", new File(cpusetDir, "cpuset.cpus"));
    TestUtils.copyOrReplaceResource("/cpuset.effective_cpus.complex", new File(cpusetDir, "cpuset.effective_cpus"));
    TestUtils.copyOrReplaceResource("/cpuset.mems", new File(cpusetDir, "cpuset.mems"));
    TestUtils.copyOrReplaceResource("/cpuset.effective_mems", new File(cpusetDir, "cpuset.effective_mems"));
  }

  @Test
  public void testMonitor()
  {
    final CgroupCpuSetMonitor monitor = new CgroupCpuSetMonitor(discoverer, "some_feed");
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertEquals(4, emitter.getNumEmittedEvents());

    emitter.verifyValue("cgroup/cpuset/cpu_count", 8);
    emitter.verifyValue("cgroup/cpuset/effective_cpu_count", 7);
    emitter.verifyValue("cgroup/cpuset/mems_count", 4);
    emitter.verifyValue("cgroup/cpuset/effective_mems_count", 1);
    Assert.assertEquals(CgroupVersion.V1.name(), emitter.getEvents().get(0).toMap().get("cgroupversion"));
  }

  @Test
  public void testCgroupsV2DetectionInConstructor() throws IOException
  {
    // Set up cgroups v2 structure
    File cgroupV2Dir = temporaryFolder.newFolder();
    File procV2Dir = temporaryFolder.newFolder();
    TestUtils.setUpCgroupsV2(procV2Dir, cgroupV2Dir);
    
    // Create v2 cpuset files in unified hierarchy
    File cgroupRoot = new File(procV2Dir, "unified");
    FileUtils.mkdirp(cgroupRoot);
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), "0-3\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), "0\n".getBytes(StandardCharsets.UTF_8));

    CgroupDiscoverer v2Discoverer = ProcSelfCgroupDiscoverer.autoCgroupDiscoverer(procV2Dir.toPath());
    Assert.assertEquals(CgroupVersion.V2, v2Discoverer.getCgroupVersion());

    // Constructor should detect v2 and log warning
    CgroupCpuSetMonitor monitor = new CgroupCpuSetMonitor(v2Discoverer, "test-feed");

    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    
    // doMonitor should return true but skip actual monitoring
    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertEquals(4, emitter.getNumEmittedEvents());
    Assert.assertEquals(CgroupVersion.V2.name(), emitter.getEvents().get(0).toMap().get("cgroupversion"));
  }

}
