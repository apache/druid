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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Cpu;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupV2Discoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Monitor that reports cpu usage stats by reading `cpu.stat` reported by cgroupv2
 */
public class CgroupV2CpuMonitor extends FeedDefiningMonitor
{
  private static final Logger LOG = new Logger(CgroupV2CpuMonitor.class);
  private static final String CPU_STAT_FILE = "cpu.stat";
  private static final String SNAPSHOT = "snapshot";
  final CgroupDiscoverer cgroupDiscoverer;
  final Map<String, String[]> dimensions;
  private final KeyedDiff diff = new KeyedDiff();

  public CgroupV2CpuMonitor(CgroupDiscoverer cgroupDiscoverer, final Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
    this.dimensions = dimensions;
  }

  @VisibleForTesting
  CgroupV2CpuMonitor(CgroupDiscoverer cgroupDiscoverer)
  {
    this(cgroupDiscoverer, ImmutableMap.of(), DEFAULT_METRICS_FEED);
  }

  CgroupV2CpuMonitor()
  {
    this(new ProcSelfCgroupDiscoverer(ProcCgroupV2Discoverer.class));
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    Snapshot snapshot = snapshot();
    final Map<String, Long> elapsed = diff.to(
        "usage",
        ImmutableMap.<String, Long>builder()
                    .put(CgroupUtil.USER, snapshot.getUserUsec())
                    .put(CgroupUtil.SYSTEM, snapshot.getSystemUsec())
                    .put(CgroupUtil.TOTAL, snapshot.getUsageUsec())
                    .put(SNAPSHOT, ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now()))
                    .build()
    );

    if (elapsed != null) {
      long elapsedUsecs = elapsed.get(SNAPSHOT);
      double totalUsagePct = 100.0 * elapsed.get(CgroupUtil.TOTAL) / elapsedUsecs;
      double sysUsagePct = 100.0 * elapsed.get(CgroupUtil.SYSTEM) / elapsedUsecs;
      double userUsagePct = 100.0 * elapsed.get(CgroupUtil.USER) / elapsedUsecs;
      emitter.emit(builder.setMetric(CgroupUtil.CPU_TOTAL_USAGE_METRIC, totalUsagePct));
      emitter.emit(builder.setMetric(CgroupUtil.CPU_SYS_USAGE_METRIC, sysUsagePct));
      emitter.emit(builder.setMetric(CgroupUtil.CPU_USER_USAGE_METRIC, userUsagePct));
    }
    return true;
  }

  /*
  file: cpu.stat

  sample content:
  usage_usec 2379951538
  user_usec 1802023024
  system_usec 577928513
  nr_periods 1581231
  nr_throttled 59
  throttled_usec 3095133
  */
  public Snapshot snapshot()
  {
    Map<String, Long> entries = new HashMap<>();
    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(Cpu.CGROUP).toString(), CPU_STAT_FILE)
    )) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        final String[] parts = line.split(Pattern.quote(" "));
        if (parts.length != 2) {
          // ignore
          continue;
        }
        entries.put(parts[0], Longs.tryParse(parts[1]));
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.error(ex, "Unable to fetch cpu snapshot");
    }

    return new Snapshot(entries.get("usage_usec"), entries.get("user_usec"), entries.get("system_usec"));
  }


  public static class Snapshot
  {
    private final long usageUsec;
    private final long userUsec;
    private final long systemUsec;

    public Snapshot(long usageUsec, long userUsec, long systemUsec)
    {
      this.usageUsec = usageUsec;
      this.userUsec = userUsec;
      this.systemUsec = systemUsec;
    }

    public long getUsageUsec()
    {
      return usageUsec;
    }

    public long getUserUsec()
    {
      return userUsec;
    }

    public long getSystemUsec()
    {
      return systemUsec;
    }
  }
}
