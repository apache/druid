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
 * Collect CPU share and quota information from cpu cgroup files.
 */
public class Cpu
{
  public static final String CGROUP = "cpu";
  private static final Logger LOG = new Logger(Cpu.class);
  private static final String CPUACCT_STAT_FILE = "cpuacct.stat";
  private static final String CPU_SHARES_FILE = "cpu.shares";
  private static final String CPU_QUOTA_FILE = "cpu.cfs_quota_us";
  private static final String CPU_PERIOD_FILE = "cpu.cfs_period_us";

  private final CgroupDiscoverer cgroupDiscoverer;

  public Cpu(CgroupDiscoverer cgroupDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  /**
   * Take a snapshot of cpu cgroup data
   *
   * @return A snapshot with the data populated.
   */
  public CpuMetrics snapshot()
  {
    long userJiffies = -1L;
    long systemJiffies = -1L;
    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), CPUACCT_STAT_FILE)
    )) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        final String[] parts = line.split(Pattern.quote(" "));
        if (parts.length != 2) {
          // ignore
          continue;
        }
        switch (parts[0]) {
          case "user":
            userJiffies = Longs.tryParse(parts[1]);
            break;
          case "system":
            systemJiffies = Longs.tryParse(parts[1]);
            break;
        }
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.error(ex, "Unable to fetch cpu snapshot");
    }


    return new CpuMetrics(
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_SHARES_FILE, -1),
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_QUOTA_FILE, 0),
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_PERIOD_FILE, 0),
        systemJiffies, userJiffies
    );
  }

  public static class CpuMetrics
  {
    // Maps to cpu.shares - the share of CPU given to the process
    private final long shares;

    // Maps to cpu.cfs_quota_us - the maximum time in microseconds during each cfs_period_us
    // in for the current group will be allowed to run
    private final long quotaUs;

    // Maps to cpu.cfs_period_us - the duration in microseconds of each scheduler period, for
    // bandwidth decisions
    private final long periodUs;

    // Maps to user value at cpuacct.stat
    private final long userJiffies;

    // Maps to system value at cpuacct.stat
    private final long systemJiffies;

    CpuMetrics(long shares, long quotaUs, long periodUs, long systemJiffis, long userJiffies)
    {
      this.shares = shares;
      this.quotaUs = quotaUs;
      this.periodUs = periodUs;
      this.userJiffies = userJiffies;
      this.systemJiffies = systemJiffis;
    }

    public final long getShares()
    {
      return shares;
    }

    public final long getQuotaUs()
    {
      return quotaUs;
    }

    public final long getPeriodUs()
    {
      return periodUs;
    }

    public long getUserJiffies()
    {
      return userJiffies;
    }

    public long getSystemJiffies()
    {
      return systemJiffies;
    }

    public long getTotalJiffies()
    {
      return userJiffies + systemJiffies;
    }
  }
}
