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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Cpu;

import java.util.Map;

public class CgroupCpuMonitor extends FeedDefiningMonitor
{
  final CgroupDiscoverer cgroupDiscoverer;
  final Map<String, String[]> dimensions;

  public CgroupCpuMonitor(CgroupDiscoverer cgroupDiscoverer, final Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
    this.dimensions = dimensions;
  }

  public CgroupCpuMonitor(final Map<String, String[]> dimensions, String feed)
  {
    this(null, dimensions, feed);
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
    final Cpu.CpuAllocationMetric cpuSnapshot = cpu.snapshot();

    final ServiceMetricEvent.Builder builder = builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);
    emitter.emit(builder.build("cgroup/cpu/shares", cpuSnapshot.getShares()));
    emitter.emit(builder.build(
        "cgroup/cpu/cores_quota",
        computeProcessorQuota(cpuSnapshot.getQuotaUs(), cpuSnapshot.getPeriodUs())
    ));

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
