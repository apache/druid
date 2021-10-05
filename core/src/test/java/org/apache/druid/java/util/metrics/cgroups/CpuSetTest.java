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

package org.apache.druid.java.util.metrics.cgroups;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;


public class CpuSetTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private CgroupDiscoverer discoverer;
  private File cpusetDir;

  @Before
  public void setUp() throws IOException
  {
    File cgroupDir = temporaryFolder.newFolder();
    File procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    cpusetDir = new File(
        cgroupDir,
        "cpuset/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );
    Assert.assertTrue((cpusetDir.isDirectory() && cpusetDir.exists()) || cpusetDir.mkdirs());

    TestUtils.copyOrReplaceResource("/cpuset.cpus", new File(cpusetDir, "cpuset.cpus"));
    TestUtils.copyOrReplaceResource("/cpuset.mems", new File(cpusetDir, "cpuset.mems"));
    TestUtils.copyOrReplaceResource("/cpuset.effective_mems", new File(cpusetDir, "cpuset.effective_mems"));
  }

  @Test
  public void testWontCrash()
  {
    final CpuSet cpuSet = new CpuSet(cgroup -> {
      throw new RuntimeException("Should still continue");
    });
    final CpuSet.CpuSetMetric metric = cpuSet.snapshot();
    Assert.assertEquals(0, metric.getCpuSetCpus().length);
    Assert.assertEquals(0, metric.getEffectiveCpuSetCpus().length);
    Assert.assertEquals(0, metric.getCpuSetMems().length);
    Assert.assertEquals(0, metric.getEffectiveCpuSetMems().length);
  }

  @Test
  public void testSimpleLoad() throws IOException
  {
    TestUtils.copyOrReplaceResource("/cpuset.effective_cpus.simple", new File(cpusetDir, "cpuset.effective_cpus"));
    final CpuSet cpuSet = new CpuSet(discoverer);
    final CpuSet.CpuSetMetric snapshot = cpuSet.snapshot();
    Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7}, snapshot.getCpuSetCpus());
    Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7}, snapshot.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals(new int[]{0, 1, 2, 3}, snapshot.getCpuSetMems());
    Assert.assertArrayEquals(new int[]{0}, snapshot.getEffectiveCpuSetMems());
  }

  @Test
  public void testComplexLoad() throws IOException
  {
    TestUtils.copyOrReplaceResource(
        "/cpuset.effective_cpus.complex",
        new File(cpusetDir, "cpuset.effective_cpus")
    );
    final CpuSet cpuSet = new CpuSet(discoverer);
    final CpuSet.CpuSetMetric snapshot = cpuSet.snapshot();
    Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4, 5, 6, 7}, snapshot.getCpuSetCpus());
    Assert.assertArrayEquals(new int[]{0, 1, 2, 7, 12, 13, 14}, snapshot.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals(new int[]{0, 1, 2, 3}, snapshot.getCpuSetMems());
    Assert.assertArrayEquals(new int[]{0}, snapshot.getEffectiveCpuSetMems());
  }
}
