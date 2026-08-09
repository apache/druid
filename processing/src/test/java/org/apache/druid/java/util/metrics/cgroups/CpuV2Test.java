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

import org.apache.druid.testing.JupiterAssertions;
import org.apache.druid.testing.TempDirExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class CpuV2Test
{
  @RegisterExtension
  public TempDirExtension temporaryFolder = new TempDirExtension();

  private File cgroupDir;
  private File procDir;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);
    discoverer = new ProcCgroupV2Discoverer(procDir.toPath());
  }

  @Test
  public void testCpuV2Snapshot() throws IOException
  {
    // Set up v2 files directly in cgroupDir (unified hierarchy root)
    // Create cpu.stat with microsecond values
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 123456789\nsystem_usec 987654321\ncore_sched.force_idle_usec 0\n".getBytes(StandardCharsets.UTF_8)
    );

    // Create cpu.weight (v2 weight)
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "200\n".getBytes(StandardCharsets.UTF_8)
    );

    // Create cpu.max (quota period)
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "150000 100000\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Verify the conversion from v2 to v1 format
    JupiterAssertions.assertEquals("Weight should be converted to shares", 2048L, metrics.getShares());
    JupiterAssertions.assertEquals("Quota should be preserved", 150000L, metrics.getQuotaUs());
    JupiterAssertions.assertEquals("Period should be preserved", 100000L, metrics.getPeriodUs());

    // V2 should not provide jiffies, only microseconds
    JupiterAssertions.assertEquals("V2 should not provide user jiffies", -1L, metrics.getUserJiffies());
    JupiterAssertions.assertEquals("V2 should not provide system jiffies", -1L, metrics.getSystemJiffies());
    JupiterAssertions.assertEquals("V2 should not provide total jiffies", -1L, metrics.getTotalJiffies());
  }

  @Test
  public void testCpuV2SnapshotWithMaxQuota() throws IOException
  {
    // Set up v2 files with unlimited quota
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 50000000\nsystem_usec 25000000\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "100\n".getBytes(StandardCharsets.UTF_8)
    );

    // cpu.max with "max" means no limit
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();
    JupiterAssertions.assertEquals("Default weight should convert to default shares", 1024L, metrics.getShares());
    JupiterAssertions.assertEquals("Max quota should be -1", -1L, metrics.getQuotaUs());
    JupiterAssertions.assertEquals("Max period should be -1", -1L, metrics.getPeriodUs());
    JupiterAssertions.assertEquals("V2 should not provide user jiffies", -1L, metrics.getUserJiffies());
    JupiterAssertions.assertEquals("V2 should not provide system jiffies", -1L, metrics.getSystemJiffies());
  }

  @Test
  public void testCpuV2SnapshotWithMissingFiles() throws IOException
  {
    // Set up directory but don't create the files
    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Should return default/error values when files are missing
    JupiterAssertions.assertEquals("Missing weight should return default shares", -1L, metrics.getShares());
    JupiterAssertions.assertEquals("Missing quota should be -1", -1L, metrics.getQuotaUs());
    JupiterAssertions.assertEquals("Missing period should be -1", -1L, metrics.getPeriodUs());
    JupiterAssertions.assertEquals("Missing user time should be -1", -1L, metrics.getUserJiffies());
    JupiterAssertions.assertEquals("Missing system time should be -1", -1L, metrics.getSystemJiffies());
    JupiterAssertions.assertEquals("Missing total should be -1", -1L, metrics.getTotalJiffies());
  }

  @Test
  public void testCpuV2SnapshotWithInvalidData() throws IOException
  {
    // Set up v2 files with invalid/malformed data
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec invalid_number\nsystem_usec not_a_number\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "invalid\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "invalid format\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Should handle invalid data gracefully
    JupiterAssertions.assertEquals("Invalid weight should return default", -1L, metrics.getShares());
    JupiterAssertions.assertEquals("Invalid quota should be -1", -1L, metrics.getQuotaUs());
    JupiterAssertions.assertEquals("Invalid period should be -1", -1L, metrics.getPeriodUs());
    JupiterAssertions.assertEquals("Invalid user time should be -1", -1L, metrics.getUserJiffies());
    JupiterAssertions.assertEquals("Invalid system time should be -1", -1L, metrics.getSystemJiffies());
  }

  @Test
  public void testWeightToSharesConversion()
  {
    // Test the weight to shares conversion logic via snapshot
    try {

      // Test various weight values
      testWeightConversion(cgroupDir, 1, 10);     // Minimum weight -> minimum shares (clamped to 2)
      testWeightConversion(cgroupDir, 100, 1024); // Default weight -> default shares
      testWeightConversion(cgroupDir, 200, 2048); // Double weight -> double shares
      testWeightConversion(cgroupDir, 1000, 10240); // 10x weight -> 10x shares
      testWeightConversion(cgroupDir, 10000, 102400); // Maximum weight -> maximum shares
    }
    catch (IOException e) {
      JupiterAssertions.fail("IOException during weight conversion test: " + e.getMessage());
    }
  }

  private void testWeightConversion(File cgroupDir, int weight, long expectedShares) throws IOException
  {
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        (weight + "\n").getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 0\nsystem_usec 0\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    JupiterAssertions.assertEquals(
        "Weight " + weight + " should convert to shares " + expectedShares,
        expectedShares, metrics.getShares()
    );
  }

  @Test
  public void testV2DoesNotConvertMicroSecondToJiffies() throws IOException
  {

    // Test microsecond to jiffies conversion (divide by 10000)
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 100000000\nsystem_usec 50000000\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "100\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // V2 should not provide jiffies, only microseconds
    JupiterAssertions.assertEquals("V2 should not provide user jiffies", -1L, metrics.getUserJiffies());
    JupiterAssertions.assertEquals("V2 should not provide system jiffies", -1L, metrics.getSystemJiffies());
    JupiterAssertions.assertEquals("V2 should not provide total jiffies", -1L, metrics.getTotalJiffies());
  }

  @Test
  public void testCpuStatFileWithExtraFields() throws IOException
  {
    // Test parsing cpu.stat with additional fields that should be ignored


    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        ("usage_usec 75000000\n" +
         "user_usec 30000000\n" +
         "system_usec 45000000\n" +
         "core_sched.force_idle_usec 12345\n" +
         "nr_periods 5000\n" +
         "nr_throttled 100\n" +
         "throttled_usec 1000000\n").getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "150\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Should parse weight correctly and ignore extra cpu.stat fields
    JupiterAssertions.assertEquals("Weight should be converted", 1536L, metrics.getShares()); // 150 * 1024 / 100
  }

  @Test
  public void testCpuMaxFileWithOnlyQuota() throws IOException
  {
    // Test cpu.max with only quota value (no period)


    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 10000000\nsystem_usec 5000000\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "100\n".getBytes(StandardCharsets.UTF_8)
    );

    // Invalid format - single value instead of "quota period"
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "75000\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Should handle malformed cpu.max gracefully
    JupiterAssertions.assertEquals("Invalid cpu.max should result in -1 quota", -1L, metrics.getQuotaUs());
    JupiterAssertions.assertEquals("Invalid cpu.max should result in -1 period", -1L, metrics.getPeriodUs());
  }

  @Test
  public void testZeroMicrosecondValues() throws IOException
  {
    // Test handling of zero values in cpu.stat
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 0\nsystem_usec 0\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "100\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // V2 should not provide jiffies, only microseconds  
    JupiterAssertions.assertEquals("V2 should not provide user jiffies", -1L, metrics.getUserJiffies());
    JupiterAssertions.assertEquals("V2 should not provide system jiffies", -1L, metrics.getSystemJiffies());
    JupiterAssertions.assertEquals("V2 should not provide total jiffies", -1L, metrics.getTotalJiffies());
    JupiterAssertions.assertEquals(0, metrics.getUserUs());
    JupiterAssertions.assertEquals(0, metrics.getSystemUs());
    JupiterAssertions.assertEquals(-1, metrics.getTotalUs());
  }

  @Test
  public void testCpuMaxWithExtraWhitespace() throws IOException
  {
    // Test cpu.max parsing with various whitespace scenarios
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 20000000\nsystem_usec 10000000\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "100\n".getBytes(StandardCharsets.UTF_8)
    );

    // cpu.max with extra whitespace
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "  200000   100000  \n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Should handle whitespace correctly
    JupiterAssertions.assertEquals("Should parse quota with whitespace", 200000L, metrics.getQuotaUs());
    JupiterAssertions.assertEquals("Should parse period with whitespace", 100000L, metrics.getPeriodUs());
  }

  @Test
  public void testNegativeWeightValue() throws IOException
  {
    // Test handling of negative weight (should not happen in practice but test robustness)
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 1000000\nsystem_usec 2000000\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "-5\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Negative weight should result in -1 shares (no limit)
    JupiterAssertions.assertEquals("Negative weight should result in -1 shares", -1L, metrics.getShares());
  }

  @Test
  public void testZeroWeightValue() throws IOException
  {
    // Test handling of zero weight (should not happen in practice but test robustness)


    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        "user_usec 1000000\nsystem_usec 2000000\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "0\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Zero weight should result in -1 shares (no limit)
    JupiterAssertions.assertEquals("Zero weight should result in -1 shares", -1L, metrics.getShares());
  }

  @Test
  public void testMalformedCpuStatLines() throws IOException
  {
    // Test cpu.stat with various malformed lines
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.stat"),
        ("incomplete_line\n" +
         "user_usec 15000000\n" +
         "too many parts here extra\n" +
         "system_usec 25000000\n" +
         "empty_value \n").getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.weight"),
        "100\n".getBytes(StandardCharsets.UTF_8)
    );
    Files.write(
        Paths.get(cgroupDir.getAbsolutePath(), "cpu.max"),
        "max\n".getBytes(StandardCharsets.UTF_8)
    );

    CpuV2 cpuV2 = new CpuV2(discoverer);
    Cpu.CpuMetrics metrics = cpuV2.snapshot();

    // Should parse valid weight and handle malformed cpu.stat lines gracefully
    JupiterAssertions.assertEquals("Should parse valid weight", 1024L, metrics.getShares());
  }
}
