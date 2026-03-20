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

import java.nio.file.Path;

public interface CgroupDiscoverer
{
  /**
   * Returns a path for a specific cgroup. This path should contain the interesting cgroup files without further traversing needed.
   * @param cgroup The cgroup
   * @return The path that contains that cgroup's interesting bits.
   */
  Path discover(String cgroup);

  /**
   * Gets CPU metrics (shares, quota, period, usage) appropriate for this cgroups version.
   * Encapsulates the difference between v1 (cpu.shares, cpu.cfs_*) and v2 (cpu.weight, cpu.max).
   * @return CPU metrics compatible with both cgroups v1 and v2
   */
  Cpu.CpuMetrics getCpuMetrics();

  /**
   * Gets CPU set metrics (effective CPU list) appropriate for this cgroups version.
   * Encapsulates the difference between v1 (cpuset.effective_cpus) and v2 (cpuset.cpus.effective).
   * @return CPU set metrics compatible with both cgroups v1 and v2
   */
  CpuSet.CpuSetMetric getCpuSetMetrics();

  CgroupVersion getCgroupVersion();

}
