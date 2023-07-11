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

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Memory
{
  private static final Logger LOG = new Logger(Memory.class);
  private static final String CGROUP = "memory";
  private static final String CGROUP_MEMORY_FILE = "memory.stat";
  private static final String CGROUP_MEMORY_NUMA_FILE = "memory.numa_stat";
  private final CgroupDiscoverer cgroupDiscoverer;

  public Memory(CgroupDiscoverer cgroupDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  public MemoryStat snapshot()
  {
    final MemoryStat memoryStat = new MemoryStat();

    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), CGROUP_MEMORY_FILE)
    )) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        final String[] parts = line.split(Pattern.quote(" "));
        if (parts.length != 2) {
          // ignore
          break;
        }
        final Long val = Longs.tryParse(parts[1]);
        if (val == null) {
          // Ignore
          break;
        }
        memoryStat.memoryStats.put(parts[0], val);
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.error(ex, "Unable to fetch memory snapshot");
    }

    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), CGROUP_MEMORY_NUMA_FILE)
    )) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        // No safety checks here. Just fail as RuntimeException and catch later
        final String[] parts = line.split(Pattern.quote(" "));
        final String[] macro = parts[0].split(Pattern.quote("="));
        final String label = macro[0];
        // Ignored
        //final long total = Long.parseLong(macro[1]);
        for (int i = 1; i < macro.length; ++i) {
          final String[] numaParts = parts[i].split(Pattern.quote("="));
          final long nodeNum = Long.parseLong(numaParts[0].substring(1));
          final long val = Long.parseLong(numaParts[1]);
          final Map<String, Long> nodeMetrics = memoryStat.numaMemoryStats.computeIfAbsent(
              nodeNum,
              l -> new HashMap<>()
          );
          nodeMetrics.put(label, val);
        }
      }
    }
    catch (RuntimeException | IOException e) {
      LOG.error(e, "Unable to fetch memory_numa snapshot");
    }
    return memoryStat;
  }


  public static class MemoryStat
  {
    private final Map<String, Long> memoryStats = new HashMap<>();
    private final Map<Long, Map<String, Long>> numaMemoryStats = new HashMap<>();

    public Map<String, Long> getMemoryStats()
    {
      return ImmutableMap.copyOf(memoryStats);
    }

    public Map<Long, Map<String, Long>> getNumaMemoryStats()
    {
      // They can modify the inner map... but why?
      return ImmutableMap.copyOf(numaMemoryStats);
    }
  }
}
