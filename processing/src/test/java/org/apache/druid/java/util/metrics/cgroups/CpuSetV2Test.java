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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CpuSetV2Test
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private File cgroupDir;
  private File procDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);
    discoverer = new ProcCgroupV2Discoverer(procDir.toPath());
  }

  @Test
  public void testCpuSetV2Snapshot() throws IOException
  {
    // Set up v2 cpuset files directly in cgroupDir (unified hierarchy root)
    File cgroupRoot = cgroupDir;
    
    // Create v2 cpuset files with different names than v1
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "0-7\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "0-3\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "0-1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "0\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    // Verify CPU set parsing
    int[] expectedCpus = {0, 1, 2, 3, 4, 5, 6, 7};
    int[] expectedEffectiveCpus = {0, 1, 2, 3};
    int[] expectedMems = {0, 1};
    int[] expectedEffectiveMems = {0};

    Assert.assertArrayEquals("CPU set should be parsed correctly", expectedCpus, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Effective CPU set should be parsed correctly", expectedEffectiveCpus, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Memory set should be parsed correctly", expectedMems, metrics.getCpuSetMems());
    Assert.assertArrayEquals("Effective memory set should be parsed correctly", expectedEffectiveMems, metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testComplexCpuRangesParsing() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Test complex CPU ranges with mixed single CPUs and ranges
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "0-2,7,12-14\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "0,2,7\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "0,2-3\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "0\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    // 0-2,7,12-14 should expand to [0,1,2,7,12,13,14]
    int[] expectedCpus = {0, 1, 2, 7, 12, 13, 14};
    // 0,2,7 should expand to [0,2,7]
    int[] expectedEffectiveCpus = {0, 2, 7};
    // 0,2-3 should expand to [0,2,3]
    int[] expectedMems = {0, 2, 3};
    // 0 should be [0]
    int[] expectedEffectiveMems = {0};

    Assert.assertArrayEquals("Complex CPU ranges should be parsed correctly", expectedCpus, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Complex effective CPU ranges should be parsed correctly", expectedEffectiveCpus, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Complex memory ranges should be parsed correctly", expectedMems, metrics.getCpuSetMems());
    Assert.assertArrayEquals("Single memory node should be parsed correctly", expectedEffectiveMems, metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testEmptyCpuSetFiles() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Create empty cpuset files
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    // Empty files should result in empty arrays
    Assert.assertArrayEquals("Empty CPU file should result in empty array", new int[0], metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Empty effective CPU file should result in empty array", new int[0], metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Empty memory file should result in empty array", new int[0], metrics.getCpuSetMems());
    Assert.assertArrayEquals("Empty effective memory file should result in empty array", new int[0], metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testMissingCpuSetFiles() throws IOException
  {
    // Set up directory but don't create the files
    File cgroupRoot = cgroupDir;

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    // Missing files should result in empty arrays
    Assert.assertArrayEquals("Missing CPU file should result in empty array", new int[0], metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Missing effective CPU file should result in empty array", new int[0], metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Missing memory file should result in empty array", new int[0], metrics.getCpuSetMems());
    Assert.assertArrayEquals("Missing effective memory file should result in empty array", new int[0], metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testInvalidCpuSetData() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Create files with invalid data
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "invalid-range\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "not-a-number\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "1-abc\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "5-3\n".getBytes(StandardCharsets.UTF_8)); // Invalid range (high < low)

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    // Invalid data should be handled gracefully and result in empty arrays
    Assert.assertArrayEquals("Invalid CPU data should result in empty array", new int[0], metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Invalid effective CPU data should result in empty array", new int[0], metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Invalid memory data should result in empty array", new int[0], metrics.getCpuSetMems());
    Assert.assertArrayEquals("Invalid effective memory data should result in empty array", new int[0], metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testMixedValidAndInvalidData() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Mix valid and invalid data in the same line
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "0-2,invalid,7,bad-range,10\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "0,not-valid,2\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "0\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "0\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    // Should parse only valid parts: 0-2,7,10 -> [0,1,2,7,10]
    int[] expectedCpus = {0, 1, 2, 7, 10};
    // Should parse only valid parts: 0,2 -> [0,2]  
    int[] expectedEffectiveCpus = {0, 2};
    int[] expectedMems = {0};
    int[] expectedEffectiveMems = {0};

    Assert.assertArrayEquals("Should parse only valid CPU parts", expectedCpus, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Should parse only valid effective CPU parts", expectedEffectiveCpus, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Valid memory data should parse correctly", expectedMems, metrics.getCpuSetMems());
    Assert.assertArrayEquals("Valid effective memory data should parse correctly", expectedEffectiveMems, metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testSingleCpuValues() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Test single CPU values without ranges
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "5\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "3\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "1\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "0\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    Assert.assertArrayEquals("Single CPU should be parsed", new int[]{5}, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Single effective CPU should be parsed", new int[]{3}, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Single memory node should be parsed", new int[]{1}, metrics.getCpuSetMems());
    Assert.assertArrayEquals("Single effective memory node should be parsed", new int[]{0}, metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testLargeRanges() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Test larger ranges
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "0-15\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "8-11\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "0-3\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "1-2\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    int[] expectedCpus = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    int[] expectedEffectiveCpus = {8, 9, 10, 11};
    int[] expectedMems = {0, 1, 2, 3};
    int[] expectedEffectiveMems = {1, 2};

    Assert.assertArrayEquals("Large CPU range should be parsed correctly", expectedCpus, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Large effective CPU range should be parsed correctly", expectedEffectiveCpus, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Large memory range should be parsed correctly", expectedMems, metrics.getCpuSetMems());
    Assert.assertArrayEquals("Large effective memory range should be parsed correctly", expectedEffectiveMems, metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testWhitespaceInFiles() throws IOException
  {
    File cgroupRoot = cgroupDir;
    
    // Test files with extra whitespace
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "  0-2,7  \n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "\t0,2\t\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        " 0 \n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "0\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    int[] expectedCpus = {0, 1, 2, 7};
    int[] expectedEffectiveCpus = {0, 2};
    int[] expectedMems = {0};
    int[] expectedEffectiveMems = {0};

    Assert.assertArrayEquals("CPU data with whitespace should be parsed correctly", expectedCpus, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("Effective CPU data with whitespace should be parsed correctly", expectedEffectiveCpus, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("Memory data with whitespace should be parsed correctly", expectedMems, metrics.getCpuSetMems());
    Assert.assertArrayEquals("Effective memory data should be parsed correctly", expectedEffectiveMems, metrics.getEffectiveCpuSetMems());
  }

  @Test
  public void testBackwardsCompatibilityWithV1Parsing() throws IOException
  {
    // This test verifies that the v2 parser can handle the same formats as v1
    File cgroupRoot = cgroupDir;
    
    // Use same format as the existing v1 test resources
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus"), 
        "0-7\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.cpus.effective"), 
        "0,2,4-6\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems"), 
        "0-3\n".getBytes(StandardCharsets.UTF_8));
    Files.write(Paths.get(cgroupRoot.getAbsolutePath(), "cpuset.mems.effective"), 
        "0\n".getBytes(StandardCharsets.UTF_8));

    CpuSetV2 cpuSetV2 = new CpuSetV2(discoverer);
    CpuSet.CpuSetMetric metrics = cpuSetV2.snapshot();

    int[] expectedCpus = {0, 1, 2, 3, 4, 5, 6, 7};
    int[] expectedEffectiveCpus = {0, 2, 4, 5, 6};
    int[] expectedMems = {0, 1, 2, 3};
    int[] expectedEffectiveMems = {0};

    Assert.assertArrayEquals("V1-compatible CPU format should work", expectedCpus, metrics.getCpuSetCpus());
    Assert.assertArrayEquals("V1-compatible effective CPU format should work", expectedEffectiveCpus, metrics.getEffectiveCpuSetCpus());
    Assert.assertArrayEquals("V1-compatible memory format should work", expectedMems, metrics.getCpuSetMems());
    Assert.assertArrayEquals("V1-compatible effective memory format should work", expectedEffectiveMems, metrics.getEffectiveCpuSetMems());
  }
}
