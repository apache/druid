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

import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class CgroupUtil
{
  private static final Logger LOG = new Logger(CgroupUtil.class);
  public static final String SPACE_MATCH = Pattern.quote(" ");
  public static final String COMMA_MATCH = Pattern.quote(",");
  public static final String TOTAL = "total";
  public static final String USER = "user";
  public static final String SYSTEM = "system";
  public static final String CPU_TOTAL_USAGE_METRIC = "cgroup/cpu/usage/total/percentage";
  public static final String CPU_USER_USAGE_METRIC = "cgroup/cpu/usage/user/percentage";
  public static final String CPU_SYS_USAGE_METRIC = "cgroup/cpu/usage/sys/percentage";
  public static final String DISK_READ_BYTES_METRIC = "cgroup/disk/read/bytes";
  public static final String DISK_READ_COUNT_METRIC = "cgroup/disk/read/count";
  public static final String DISK_WRITE_BYTES_METRIC = "cgroup/disk/write/bytes";
  public static final String DISK_WRITE_COUNT_METRIC = "cgroup/disk/write/count";


  public static long readLongValue(CgroupDiscoverer discoverer, String cgroup, String fileName, long defaultValue)
  {
    try {
      List<String> lines = Files.readAllLines(Paths.get(discoverer.discover(cgroup).toString(), fileName));
      return lines.stream().map(Longs::tryParse).filter(Objects::nonNull).findFirst().orElse(defaultValue);
    }
    catch (RuntimeException | IOException ex) {
      LOG.noStackTrace().warn(ex, "Unable to fetch %s", fileName);
      return defaultValue;
    }
  }

  /**
   * Detects if we're running on cgroups v2 by checking if the discoverer path contains cgroup2 files.
   * This is a simple heuristic to avoid running v1-specific monitors on v2 systems.
   * 
   * @param cgroupDiscoverer the discoverer to check
   * @return true if running on cgroups v2, false for v1 or unknown
   */
  public static boolean isRunningOnCgroupsV2(CgroupDiscoverer cgroupDiscoverer)
  {
    try {
      // Try CPU cgroup first - check if cpu.max file exists (cgroups v2) instead of cpu.cfs_quota_us (cgroups v1)
      Path cpuCgroupPath = cgroupDiscoverer.discover("cpu");
      if (cpuCgroupPath != null) {
        Path cpuMaxFile = cpuCgroupPath.resolve("cpu.max");
        Path cpuQuotaFile = cpuCgroupPath.resolve("cpu.cfs_quota_us");
        
        if (Files.exists(cpuMaxFile) && !Files.exists(cpuQuotaFile)) {
          return true;
        }
      }
      
      // Fallback: Try cpuset cgroup - check if cpuset.cpus.effective exists (v2) vs cpuset.effective_cpus (v1)
      Path cpusetCgroupPath = cgroupDiscoverer.discover("cpuset");
      if (cpusetCgroupPath != null) {
        Path v2EffectiveCpusFile = cpusetCgroupPath.resolve("cpuset.cpus.effective");
        Path v1EffectiveCpusFile = cpusetCgroupPath.resolve("cpuset.effective_cpus");
        
        return Files.exists(v2EffectiveCpusFile) && !Files.exists(v1EffectiveCpusFile);
      }
    } catch (Exception e) {
      LOG.debug(e, "Could not determine cgroups version, assuming v1");
    }
    return false;
  }


  /**
   * Calculates the total cores allocated through quotas. A negative value indicates that no quota has been specified.
   * We use -1 because that's the default value used in the cgroup.
   *
   * @param quotaUs  the cgroup quota value.
   * @param periodUs the cgroup period value.
   * @return the calculated processor quota, -1 if no quota or period set.
   */
  public static double computeProcessorQuota(long quotaUs, long periodUs)
  {
    return quotaUs < 0 || periodUs == 0
           ? -1
           : (double) quotaUs / periodUs;
  }

}
