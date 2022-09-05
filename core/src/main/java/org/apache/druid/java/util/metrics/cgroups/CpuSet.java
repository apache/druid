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

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Collect CPU and memory data from cpuset cgroup files.
 */
public class CpuSet
{
  private static final Logger LOG = new Logger(CpuSet.class);
  private static final String CGROUP = "cpuset";
  private static final String CPUS_FILE = "cpuset.cpus";
  private static final String EFFECTIVE_CPUS_FILE = "cpuset.effective_cpus";
  private static final String MEMS_FILE = "cpuset.mems";
  private static final String EFFECTIVE_MEMS_FILE = "cpuset.effective_mems";

  private final CgroupDiscoverer cgroupDiscoverer;

  public CpuSet(CgroupDiscoverer cgroupDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  /**
   * Take a snapshot of cpuset cgroup data
   *
   * @return A snapshot with the data populated.
   */
  public CpuSetMetric snapshot()
  {
    return new CpuSetMetric(
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
      output = lines.stream().map(this::parseStringRangeToIntArray).findFirst().orElse(output);
    }
    catch (RuntimeException | IOException ex) {
      LOG.error(ex, "Unable to read %s", file);
    }
    return output;
  }

  /**
   * Parses the cpuset list format and outputs it as a list of CPUs. Examples:
   * 0-4,9        # bits 0, 1, 2, 3, 4, and 9 set
   *              # outputs [0, 1, 2, 3, 4, 9]
   * 0-2,7,12-14  # bits 0, 1, 2, 7, 12, 13, and 14 set
   *              # outputs [0, 1, 2, 7, 12, 13, 14]
   *
   * This method also works fine for memory nodes.
   *
   * @param line The list format cpu value
   * @return the list of CPU IDs
   */
  private int[] parseStringRangeToIntArray(String line)
  {
    String[] cpuParts = line.split(",");
    return Arrays.stream(cpuParts)
       .flatMapToInt(cpuPart -> {
         String[] bits = cpuPart.split("-");
         if (bits.length == 2) {
           Integer low = Ints.tryParse(bits[0]);
           Integer high = Ints.tryParse(bits[1]);
           if (low != null && high != null) {
             return IntStream.rangeClosed(low, high);
           }
         } else if (bits.length == 1) {
           Integer bit = Ints.tryParse(bits[0]);
           if (bit != null) {
             return IntStream.of(bit);
           }
         }

         return IntStream.empty();
       }).toArray();
  }

  public static class CpuSetMetric
  {
    // The list of processor IDs associated with the process
    private final int[] cpuSetCpus;

    // The list of effective/active processor IDs associated with the process
    private final int[] effectiveCpuSetCpus;

    // The list memory nodes associated with the process
    private final int[] cpuSetMems;

    // The list of effective/active memory nodes associated with the process
    private final int[] effectiveCpuSetMems;

    CpuSetMetric(
        int[] cpuSetCpus,
        int[] effectiveCpuSetCpus,
        int[] cpuSetMems,
        int[] effectiveCpuSetMems)
    {
      this.cpuSetCpus = cpuSetCpus;
      this.effectiveCpuSetCpus = effectiveCpuSetCpus;
      this.cpuSetMems = cpuSetMems;
      this.effectiveCpuSetMems = effectiveCpuSetMems;
    }

    public int[] getCpuSetCpus()
    {
      return cpuSetCpus;
    }

    public int[] getEffectiveCpuSetCpus()
    {
      return effectiveCpuSetCpus;
    }

    public int[] getCpuSetMems()
    {
      return cpuSetMems;
    }

    public int[] getEffectiveCpuSetMems()
    {
      return effectiveCpuSetMems;
    }
  }
}
