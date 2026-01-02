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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Cpu;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupV2Discoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Monitor that reports cpu usage stats by reading `cpu.stat` reported by cgroupv2
 */
public class CgroupV2CpuMonitor extends FeedDefiningMonitor
{
  private static final String SNAPSHOT = "snapshot";
  final CgroupDiscoverer cgroupDiscoverer;
  private final KeyedDiff diff = new KeyedDiff();

  public CgroupV2CpuMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  @VisibleForTesting
  CgroupV2CpuMonitor(CgroupDiscoverer cgroupDiscoverer)
  {
    this(cgroupDiscoverer, DEFAULT_METRICS_FEED);
  }

  CgroupV2CpuMonitor()
  {
    this(new ProcSelfCgroupDiscoverer(ProcCgroupV2Discoverer.class));
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final ServiceMetricEvent.Builder builder = builder();
    builder.setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion().name());
    final Cpu.CpuMetrics cpuSnapshot = cgroupDiscoverer.getCpuMetrics();
    emitter.emit(builder.setMetric("cgroup/cpu/shares", cpuSnapshot.getShares()));
    emitter.emit(builder.setMetric(
        "cgroup/cpu/cores_quota",
        CgroupUtil.computeProcessorQuota(cpuSnapshot.getQuotaUs(), cpuSnapshot.getPeriodUs())
    ));


    final Map<String, Long> elapsed = diff.to(
        "usage",
        ImmutableMap.<String, Long>builder()
                    .put(CgroupUtil.USER, cpuSnapshot.getUserUs())
                    .put(CgroupUtil.SYSTEM, cpuSnapshot.getSystemUs())
                    .put(CgroupUtil.TOTAL, cpuSnapshot.getTotalUs())
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
}
