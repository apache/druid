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

/**
 * Collect CPU share and quota information from cpu cgroup files.
 */
public class Cpu
{
  private static final String CGROUP = "cpu";
  private static final String CPU_USAGE_FILE = "cpuacct.usage";
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
    return new CpuMetrics(
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_SHARES_FILE, -1),
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_QUOTA_FILE, 0),
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_PERIOD_FILE, 0),
        CgroupUtil.readLongValue(cgroupDiscoverer, CGROUP, CPU_USAGE_FILE, -1)
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

    // Maps to cpuacct.uage - the total CPU time (in nanoseconds) consumed by all tasks in this cgroup
    private final long usageNs;

    CpuMetrics(long shares, long quotaUs, long periodUs, long usageNs)
    {
      this.shares = shares;
      this.quotaUs = quotaUs;
      this.periodUs = periodUs;
      this.usageNs = usageNs;
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

    public final long getUsageNs()
    {
      return usageNs;
    }
  }
}
