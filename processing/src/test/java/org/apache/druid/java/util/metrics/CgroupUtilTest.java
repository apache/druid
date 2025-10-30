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
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class CgroupUtilTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File cgroupDir;
  private File procDir;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
  }

  @Test
  public void testDetectCgroupsV1() throws IOException
  {
    // Set up cgroups v1 structure
    TestUtils.setUpCgroups(procDir, cgroupDir);
    setupCgroupsV1Files();

    CgroupDiscoverer discoverer = new MockCgroupDiscoverer();
    boolean isV2 = CgroupUtil.isRunningOnCgroupsV2(discoverer);

    Assert.assertFalse("Should detect cgroups v1", isV2);
  }

  @Test
  public void testDetectCgroupsV2() throws IOException
  {
    // Set up cgroups v2 structure  
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);
    setupCgroupsV2Files();

    CgroupDiscoverer discoverer = new MockCgroupDiscoverer();
    boolean isV2 = CgroupUtil.isRunningOnCgroupsV2(discoverer);

    Assert.assertTrue("Should detect cgroups v2", isV2);
  }

  @Test
  public void testDetectCgroupsV2WithoutV1Files() throws IOException
  {
    // Set up cgroups v2 structure with only v2 files (no v1 files present)
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);

    // Create only v2 CPU files
    File cpuDir = new File(cgroupDir, "cpu_v2_test");
    FileUtils.mkdirp(cpuDir);
    Files.write(Paths.get(cpuDir.getAbsolutePath(), "cpu.max"), "100000 100000\n".getBytes(StandardCharsets.UTF_8));
    // Intentionally don't create cpu.cfs_quota_us

    CgroupDiscoverer discoverer = new MockCgroupDiscoverer();
    boolean isV2 = CgroupUtil.isRunningOnCgroupsV2(discoverer);

    Assert.assertTrue("Should detect cgroups v2 when only v2 files exist", isV2);
  }

  @Test
  public void testDetectCgroupsUnknownWhenNoFiles() throws IOException
  {
    // Set up empty directory structure
    CgroupDiscoverer discoverer = new MockCgroupDiscoverer();
    boolean isV2 = CgroupUtil.isRunningOnCgroupsV2(discoverer);

    Assert.assertFalse("Should default to v1 when no files found", isV2);
  }

  @Test
  public void testDetectCgroupsV1WithBothFilesPresent() throws IOException
  {
    // Create a scenario where both v1 and v2 files exist (should prefer v1 logic)
    TestUtils.setUpCgroups(procDir, cgroupDir);

    File cpuDir = new File(cgroupDir, "cpu_mixed_test");
    FileUtils.mkdirp(cpuDir);

    // Create both v1 and v2 files
    Files.write(Paths.get(cpuDir.getAbsolutePath(), "cpu.cfs_quota_us"), "-1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cpuDir.getAbsolutePath(), "cpu.max"), "max\n".getBytes(StandardCharsets.UTF_8));

    CgroupDiscoverer discoverer = new MockCgroupDiscoverer();
    boolean isV2 = CgroupUtil.isRunningOnCgroupsV2(discoverer);

    Assert.assertFalse("Should detect v1 when both files are present", isV2);
  }

  @Test
  public void testDetectCgroupsV2UsingCpusetFallback() throws IOException
  {
    // Test fallback to cpuset detection when CPU cgroup is not available
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);

    File cpusetDir = new File(cgroupDir, "cpuset_v2_test");
    FileUtils.mkdirp(cpusetDir);

    // Create v2 cpuset files
    Files.write(
        Paths.get(cpusetDir.getAbsolutePath(), "cpuset.cpus.effective"),
        "0-3\n".getBytes(StandardCharsets.UTF_8)
    );
    // Don't create cpuset.effective_cpus (v1 file)

    CgroupDiscoverer discoverer = new CpusetOnlyMockDiscoverer();
    boolean isV2 = CgroupUtil.isRunningOnCgroupsV2(discoverer);

    Assert.assertTrue("Should detect v2 using cpuset fallback", isV2);
  }

  private void setupCgroupsV1Files() throws IOException
  {
    // Create v1 CPU files
    File cpuDir = new File(cgroupDir, "cpu,cpuacct/system.slice/test.service");
    FileUtils.mkdirp(cpuDir);

    Files.write(Paths.get(cpuDir.getAbsolutePath(), "cpu.cfs_quota_us"), "-1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cpuDir.getAbsolutePath(), "cpu.cfs_period_us"), "100000\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cpuDir.getAbsolutePath(), "cpu.shares"), "1024\n".getBytes(StandardCharsets.UTF_8));

    // Create v1 cpuset files
    File cpusetDir = new File(cgroupDir, "cpuset/system.slice/test.service");
    FileUtils.mkdirp(cpusetDir);
    Files.write(
        Paths.get(cpusetDir.getAbsolutePath(), "cpuset.effective_cpus"),
        "0-3\n".getBytes(StandardCharsets.UTF_8)
    );
  }

  private void setupCgroupsV2Files() throws IOException
  {
    // Create v2 CPU files in unified hierarchy
    File cgroupRoot = new File(cgroupDir, "unified");
    FileUtils.mkdirp(cgroupRoot);

    TestUtils.writeCpuCgroupV2Files(cgroupRoot);
  }


  // Mock discoverer that returns paths to our test directories
  private class MockCgroupDiscoverer implements CgroupDiscoverer
  {
    @Override
    public Path discover(String cgroup)
    {
      switch (cgroup) {
        case "cpu":
          if (new File(cgroupDir, "cpu,cpuacct/system.slice/test.service").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "cpu,cpuacct/system.slice/test.service");
          }
          if (new File(cgroupDir, "unified").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "unified");
          }
          if (new File(cgroupDir, "cpu_v2_test").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "cpu_v2_test");
          }
          if (new File(cgroupDir, "cpu_mixed_test").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "cpu_mixed_test");
          }
          break;
        case "cpuset":
          if (new File(cgroupDir, "cpuset/system.slice/test.service").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "cpuset/system.slice/test.service");
          }
          if (new File(cgroupDir, "unified").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "unified");
          }
          if (new File(cgroupDir, "cpuset_v2_test").exists()) {
            return Paths.get(cgroupDir.getAbsolutePath(), "cpuset_v2_test");
          }
          break;
      }
      return null;
    }

    @Override
    public org.apache.druid.java.util.metrics.cgroups.Cpu.CpuMetrics getCpuMetrics()
    {
      return null; // Not used in these tests
    }

    @Override
    public org.apache.druid.java.util.metrics.cgroups.CpuSet.CpuSetMetric getCpuSetMetrics()
    {
      return null; // Not used in these tests
    }

    @Override
    public CgroupVersion getCgroupVersion()
    {
      return null;
    }
  }

  // Mock discoverer that only returns cpuset paths (for fallback testing)
  private class CpusetOnlyMockDiscoverer implements CgroupDiscoverer
  {
    @Override
    public Path discover(String cgroup)
    {
      if ("cpuset".equals(cgroup)) {
        return Paths.get(cgroupDir.getAbsolutePath(), "cpuset_v2_test");
      }
      return null; // CPU cgroup not available
    }

    @Override
    public org.apache.druid.java.util.metrics.cgroups.Cpu.CpuMetrics getCpuMetrics()
    {
      return null;
    }

    @Override
    public org.apache.druid.java.util.metrics.cgroups.CpuSet.CpuSetMetric getCpuSetMetrics()
    {
      return null;
    }

    @Override
    public CgroupVersion getCgroupVersion()
    {
      return null;
    }
  }
}
