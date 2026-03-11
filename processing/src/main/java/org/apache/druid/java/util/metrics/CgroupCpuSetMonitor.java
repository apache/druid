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

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.CpuSet;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

/**
 * Monitor that reports CPU set metrics from cgroups both v1 and v2.
 */

public class CgroupCpuSetMonitor extends FeedDefiningMonitor
{
  final CgroupDiscoverer cgroupDiscoverer;

  public CgroupCpuSetMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  public CgroupCpuSetMonitor(String feed)
  {
    this(ProcSelfCgroupDiscoverer.autoCgroupDiscoverer(), feed);
  }

  public CgroupCpuSetMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final CpuSet.CpuSetMetric cpusetSnapshot = cgroupDiscoverer.getCpuSetMetrics();
    final ServiceMetricEvent.Builder builder = builder();
    builder.setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion().name());
    emitter.emit(builder.setMetric(
        "cgroup/cpuset/cpu_count",
        cpusetSnapshot.getCpuSetCpus().length
    ));
    emitter.emit(builder.setMetric(
        "cgroup/cpuset/effective_cpu_count",
        cpusetSnapshot.getEffectiveCpuSetCpus().length
    ));
    emitter.emit(builder.setMetric(
        "cgroup/cpuset/mems_count",
        cpusetSnapshot.getCpuSetMems().length
    ));
    emitter.emit(builder.setMetric(
        "cgroup/cpuset/effective_mems_count",
        cpusetSnapshot.getEffectiveCpuSetMems().length
    ));
    return true;
  }
}
