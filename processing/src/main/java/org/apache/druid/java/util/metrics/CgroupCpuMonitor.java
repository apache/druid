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
import org.apache.druid.java.util.metrics.cgroups.CgroupVersion;
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
  private Long userHz;
  private final KeyedDiff jiffies = new KeyedDiff();
  private long prevJiffiesSnapshotAt = 0;
  private final boolean isRunningOnCgroupsV2;
  private final CgroupV2CpuMonitor cgroupV2CpuMonitor;

  public CgroupCpuMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;

    // Check if we're running on cgroups v2
    this.isRunningOnCgroupsV2 = cgroupDiscoverer.getCgroupVersion().equals(CgroupVersion.V2);
    if (isRunningOnCgroupsV2) {
      this.cgroupV2CpuMonitor = new CgroupV2CpuMonitor(cgroupDiscoverer, feed);
      LOG.info("Detected cgroups v2, using CgroupV2CpuMonitor behavior for accurate metrics");
    } else {
      this.cgroupV2CpuMonitor = null;
      initUzerHz();
    }

  }

  public CgroupCpuMonitor(String feed)
  {
    this(ProcSelfCgroupDiscoverer.autoCgroupDiscoverer(), feed);
  }

  public CgroupCpuMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (isRunningOnCgroupsV2) {
      return cgroupV2CpuMonitor.doMonitor(emitter);
    } else {
      return doMonitorV1(emitter);
    }
  }

  private boolean doMonitorV1(ServiceEmitter emitter)
  {
    final Cpu.CpuMetrics cpuSnapshot = cgroupDiscoverer.getCpuMetrics();
    long now = Instant.now().getEpochSecond();

    final ServiceMetricEvent.Builder builder = builder();
    builder.setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion().name());

    emitter.emit(builder.setMetric("cgroup/cpu/shares", cpuSnapshot.getShares()));
    emitter.emit(builder.setMetric(
        "cgroup/cpu/cores_quota",
        CgroupUtil.computeProcessorQuota(cpuSnapshot.getQuotaUs(), cpuSnapshot.getPeriodUs())
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

  private void initUzerHz()
  {
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
}
