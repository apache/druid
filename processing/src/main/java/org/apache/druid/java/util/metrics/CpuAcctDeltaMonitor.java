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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.CgroupVersion;
import org.apache.druid.java.util.metrics.cgroups.CpuAcct;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;
import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicReference;

public class CpuAcctDeltaMonitor extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(CpuAcctDeltaMonitor.class);
  private static final String USE_CGROUPS_V2_MESSAGE = StringUtils.format(
      "%s doest not function correctly on cgroups v2. Please use %s instead.",
      CpuAcctDeltaMonitor.class.getSimpleName(),
      CgroupV2CpuMonitor.class.getSimpleName()
  );
  private final AtomicReference<SnapshotHolder> priorSnapshot = new AtomicReference<>(null);

  private final CgroupDiscoverer cgroupDiscoverer;
  private final boolean isRunningOnCgroupsV2;

  public CpuAcctDeltaMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  public CpuAcctDeltaMonitor(final String feed)
  {
    this(feed, ProcSelfCgroupDiscoverer.autoCgroupDiscoverer());
  }

  public CpuAcctDeltaMonitor(String feed, CgroupDiscoverer cgroupDiscoverer)
  {
    super(feed);
    this.cgroupDiscoverer = Preconditions.checkNotNull(cgroupDiscoverer, "cgroupDiscoverer required");

    isRunningOnCgroupsV2 = cgroupDiscoverer.getCgroupVersion().equals(CgroupVersion.V2);
    if (isRunningOnCgroupsV2) {
      log.warn(USE_CGROUPS_V2_MESSAGE);
    }
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (isRunningOnCgroupsV2) {
      log.warn(USE_CGROUPS_V2_MESSAGE);
      return false;
    }
    final CpuAcct cpuAcct = new CpuAcct(cgroupDiscoverer);
    final CpuAcct.CpuAcctMetric snapshot = cpuAcct.snapshot();
    final long nanoTime = System.nanoTime(); // Approx time... may be influenced by an unlucky GC
    final DateTime dateTime = DateTimes.nowUtc();
    final SnapshotHolder priorSnapshotHolder = this.priorSnapshot.get();
    if (!priorSnapshot.compareAndSet(priorSnapshotHolder, new SnapshotHolder(snapshot, nanoTime))) {
      log.debug("Pre-empted by another monitor run");
      return false;
    }
    if (priorSnapshotHolder == null) {
      log.info("Detected first run, storing result for next run");
      return false;
    }
    final long elapsedNs = nanoTime - priorSnapshotHolder.timestamp;
    if (snapshot.cpuCount() != priorSnapshotHolder.metric.cpuCount()) {
      log.warn(
          "Prior CPU count [%d] does not match current cpu count [%d]. Skipping metrics emission",
          priorSnapshotHolder.metric.cpuCount(),
          snapshot.cpuCount()
      );
      return false;
    }
    for (int i = 0; i < snapshot.cpuCount(); ++i) {
      final ServiceMetricEvent.Builder builderUsr = builder()
          .setDimension("cpuName", Integer.toString(i))
          .setDimension("cpuTime", "usr")
          .setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion().name());
      final ServiceMetricEvent.Builder builderSys = builder()
          .setDimension("cpuName", Integer.toString(i))
          .setDimension("cpuTime", "sys")
          .setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion().name());
      emitter.emit(builderUsr.setCreatedTime(dateTime).setMetric(
          "cgroup/cpu_time_delta_ns",
          snapshot.usrTime(i) - priorSnapshotHolder.metric.usrTime(i)
      ));
      emitter.emit(builderSys.setCreatedTime(dateTime).setMetric(
          "cgroup/cpu_time_delta_ns",
          snapshot.sysTime(i) - priorSnapshotHolder.metric.sysTime(i)
      ));
    }
    if (snapshot.cpuCount() > 0) {
      // Don't bother emitting metrics if there aren't actually any cpus (usually from error)
      emitter.emit(builder().setCreatedTime(dateTime).setMetric("cgroup/cpu_time_delta_ns_elapsed", elapsedNs));
    }
    return true;
  }

  static class SnapshotHolder
  {
    private final CpuAcct.CpuAcctMetric metric;
    private final long timestamp;

    SnapshotHolder(CpuAcct.CpuAcctMetric metric, long timestamp)
    {
      this.metric = metric;
      this.timestamp = timestamp;
    }
  }
}
