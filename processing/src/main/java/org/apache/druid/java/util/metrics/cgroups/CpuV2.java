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

import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.metrics.CgroupUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Collect CPU weight, quota and usage information from cgroups v2 files.
 * This class provides a bridge to return Cpu.CpuMetrics compatible data from cgroups v2.
 */
public class CpuV2
{
  public static final String CGROUP = ""; // cgroups v2 uses unified hierarchy
  private static final Logger LOG = new Logger(CpuV2.class);
  private static final String CPU_STAT_FILE = "cpu.stat";
  private static final String CPU_WEIGHT_FILE = "cpu.weight";
  private static final String
      CPU_MAX_FILE = "cpu.max";

  private final CgroupDiscoverer cgroupDiscoverer;

  public CpuV2(CgroupDiscoverer cgroupDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  /**
   * Take a snapshot of cgroups v2 CPU data with native microsecond precision.
   * This provides maximum accuracy for monitoring calculations.
   *
   * @return A Cpu.CpuMetrics snapshot with native v2 microsecond data.
   */
  public Cpu.CpuMetrics snapshot()
  {
    long usageUsec = -1L;
    long userUsec = -1L;
    long systemUsec = -1L;

    // Read cpu.stat (equivalent to cpuacct.stat but with different format)
    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), CPU_STAT_FILE)
    )) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        final String[] parts = line.split(Pattern.quote(" "));
        if (parts.length != 2) {
          continue;
        }
        switch (parts[0]) {
          case "usage_usec":
            usageUsec = Longs.tryParse(parts[1]);  // Capture this!
            break;
          case "user_usec":
            userUsec = Longs.tryParse(parts[1]);
            break;
          case "system_usec":
            systemUsec = Longs.tryParse(parts[1]);
            break;
        }
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.noStackTrace().warn(ex, "Unable to fetch CPU v2 snapshot. Cgroup metrics will not be emitted.");
    }

    // Read cpu.weight (equivalent to cpu.shares but different scale)
    long weight = CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_WEIGHT_FILE, -1);

    // Read cpu.max (format: "quota period" or "max")
    long quotaUs = -1;
    long periodUs = -1;
    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), CPU_MAX_FILE)
    )) {
      String line = reader.readLine();
      if (line != null && !line.trim().startsWith("max")) {
        String[] parts = line.trim().split("\\s+");
        if (parts.length == 2) {
          quotaUs = Longs.tryParse(parts[0]);
          periodUs = Longs.tryParse(parts[1]);
        }
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.noStackTrace().warn(ex, "Unable to read cpu.max file.");
    }

    // Convert weight to shares for compatibility with existing logic
    long shares = convertWeightToShares(weight);

    return new Cpu.CpuMetrics(shares, quotaUs, periodUs, -1, -1, userUsec, systemUsec, usageUsec);
  }

  /**
   * Convert cgroups v2 cpu.weight to cgroups v1 cpu.shares equivalent.
   * v2 weight range: 1-10000 (default: 100)
   * v1 shares range: 2-262144 (default: 1024)
   *
   * @param weight The cgroups v2 weight value
   * @return Equivalent shares value for compatibility
   */
  private static long convertWeightToShares(long weight)
  {
    if (weight <= 0) {
      return -1; // No limit
    }

    // Convert weight to shares: weight=100 -> shares=1024 (defaults match)
    // Linear scaling: shares = (weight * 1024) / 100
    return Math.max(2, (weight * 1024) / 100);
  }
}