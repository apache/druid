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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Cpu;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

public class CgroupCpuMonitor extends FeedDefiningMonitor
{
  private static final Logger LOG = new Logger(CgroupCpuMonitor.class);
  private static final Long DEFAULT_USER_HZ = 100L;
  final CgroupDiscoverer cgroupDiscoverer;
  final Map<String, String[]> dimensions;
  private Long userHz;
  private KeyedDiff jiffies = new KeyedDiff();
  private long prevJiffiesSnapshotAt = 0;

  public CgroupCpuMonitor(CgroupDiscoverer cgroupDiscoverer, final Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
    this.dimensions = dimensions;
    try {
      Process p = new ProcessBuilder("getconf", "CLK_TCK").start();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
        String line = in.readLine();
        if (line != null) {
          userHz = Long.valueOf(line.trim());
        }
      }
    }
    catch (IOException | NumberFormatException e) {
      LOG.warn(e, "Error getting the USER_HZ value");
    }
    finally {
      if (userHz == null) {
        LOG.warn("Using default value for USER_HZ");
        userHz = DEFAULT_USER_HZ;
      }
    }
  }

  public CgroupCpuMonitor(final Map<String, String[]> dimensions, String feed)
  {
    this(new ProcSelfCgroupDiscoverer(), dimensions, feed);
  }

  public CgroupCpuMonitor(final Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public CgroupCpuMonitor()
  {
    this(ImmutableMap.of());
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final Cpu cpu = new Cpu(cgroupDiscoverer);
    final Cpu.CpuMetrics cpuSnapshot = cpu.snapshot();
    long now = Instant.now().getEpochSecond();

    final ServiceMetricEvent.Builder builder = builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    emitter.emit(builder.setMetric("cgroup/cpu/shares", cpuSnapshot.getShares()));
    emitter.emit(builder.setMetric(
        "cgroup/cpu/cores_quota",
        computeProcessorQuota(cpuSnapshot.getQuotaUs(), cpuSnapshot.getPeriodUs())
    ));

    long elapsedJiffiesSnapshotSecs = now - prevJiffiesSnapshotAt;
    if (elapsedJiffiesSnapshotSecs > 0) {
      prevJiffiesSnapshotAt = now;
      final Map<String, Long> elapsedJiffies = jiffies.to(
          "usage",
          ImmutableMap.<String, Long>builder()
                      .put(CgroupUtil.USER, cpuSnapshot.getUserJiffies())
                      .put(CgroupUtil.SYSTEM, cpuSnapshot.getSystemJiffies())
                      .put(CgroupUtil.TOTAL, cpuSnapshot.getTotalJiffies())
                      .build()
      );
      if (elapsedJiffies != null) {
        double totalUsagePct = 100.0 * elapsedJiffies.get(CgroupUtil.TOTAL) / userHz / elapsedJiffiesSnapshotSecs;
        double sysUsagePct = 100.0 * elapsedJiffies.get(CgroupUtil.SYSTEM) / userHz / elapsedJiffiesSnapshotSecs;
        double userUsagePct = 100.0 * elapsedJiffies.get(CgroupUtil.USER) / userHz / elapsedJiffiesSnapshotSecs;
        emitter.emit(builder.setMetric(CgroupUtil.CPU_TOTAL_USAGE_METRIC, totalUsagePct));
        emitter.emit(builder.setMetric(CgroupUtil.CPU_SYS_USAGE_METRIC, sysUsagePct));
        emitter.emit(builder.setMetric(CgroupUtil.CPU_USER_USAGE_METRIC, userUsagePct));
      }
    }
    return true;
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
