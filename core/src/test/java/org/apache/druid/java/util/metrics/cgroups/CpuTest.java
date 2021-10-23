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


public class CpuTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    File cgroupDir = temporaryFolder.newFolder();
    File procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File cpuDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );
    Assert.assertTrue((cpuDir.isDirectory() && cpuDir.exists()) || cpuDir.mkdirs());
    TestUtils.copyOrReplaceResource("/cpu.shares", new File(cpuDir, "cpu.shares"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_quota_us", new File(cpuDir, "cpu.cfs_quota_us"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_period_us", new File(cpuDir, "cpu.cfs_period_us"));
  }

  @Test
  public void testWontCrash()
  {
    final Cpu cpu = new Cpu(cgroup -> {
      throw new RuntimeException("Should still continue");
    });
    final Cpu.CpuAllocationMetric metric = cpu.snapshot();
    Assert.assertEquals(-1L, metric.getShares());
    Assert.assertEquals(0, metric.getQuotaUs());
    Assert.assertEquals(0, metric.getPeriodUs());
  }

  @Test
  public void testSimpleLoad()
  {
    final Cpu cpu = new Cpu(discoverer);
    final Cpu.CpuAllocationMetric snapshot = cpu.snapshot();
    Assert.assertEquals(1024, snapshot.getShares());
    Assert.assertEquals(300000, snapshot.getQuotaUs());
    Assert.assertEquals(100000, snapshot.getPeriodUs());
  }
}
