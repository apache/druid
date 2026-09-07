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

import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * Collect CPU and memory data from cgroups v2 cpuset files.
 * This class provides a bridge to return CpuSet.CpuSetMetric compatible data from cgroups v2.
 */
public class CpuSetV2
{
  private static final Logger LOG = new Logger(CpuSetV2.class);
  public static final String CGROUP = ""; // cgroups v2 uses unified hierarchy
  // cgroups v2 file names (different from v1)
  private static final String CPUS_FILE = "cpuset.cpus";
  private static final String EFFECTIVE_CPUS_FILE = "cpuset.cpus.effective";
  private static final String MEMS_FILE = "cpuset.mems";
  private static final String EFFECTIVE_MEMS_FILE = "cpuset.mems.effective";

  private final CgroupDiscoverer cgroupDiscoverer;

  public CpuSetV2(CgroupDiscoverer cgroupDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  /**
   * Take a snapshot of cgroups v2 cpuset data and convert it to CpuSet.CpuSetMetric format.
   *
   * @return A CpuSet.CpuSetMetric snapshot with v2 data in v1-compatible format.
   */
  public CpuSet.CpuSetMetric snapshot()
  {
    return new CpuSet.CpuSetMetric(
        readCpuSetFile(CPUS_FILE),
        readCpuSetFile(EFFECTIVE_CPUS_FILE),
        readCpuSetFile(MEMS_FILE),
        readCpuSetFile(EFFECTIVE_MEMS_FILE)
    );
  }

  private int[] readCpuSetFile(String file)
  {
    int[] output = {};
    try {
      List<String> lines = Files.readAllLines(
          Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), file));
      output = lines.stream().map(CpuSet::parseStringRangeToIntArray).findFirst().orElse(output);
    }
    catch (RuntimeException | IOException ex) {
      LOG.noStackTrace().warn(ex, "Unable to read %s, these metrics will be skipped", file);
    }
    return output;
  }
}
