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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.CgroupVersion;
import org.apache.druid.java.util.metrics.cgroups.Memory;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

public class CgroupMemoryMonitor extends FeedDefiningMonitor
{
  private static final Logger LOG = new Logger(CgroupMemoryMonitor.class);
  private static final String MEMORY_USAGE_FILE = "memory.usage_in_bytes";
  private static final String MEMORY_LIMIT_FILE = "memory.limit_in_bytes";

  final CgroupDiscoverer cgroupDiscoverer;
  private final boolean isRunningOnCgroupsV2;
  private final CgroupV2MemoryMonitor cgroupV2MemoryMonitor;


  public CgroupMemoryMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;

    // Check if we're running on cgroups v2
    this.isRunningOnCgroupsV2 = cgroupDiscoverer.getCgroupVersion().equals(CgroupVersion.V2);
    if (isRunningOnCgroupsV2) {
      this.cgroupV2MemoryMonitor = new CgroupV2MemoryMonitor(cgroupDiscoverer, feed);
      LOG.info("Detected cgroups v2, using CgroupV2MemoryMonitor behavior for accurate metrics");
    } else {
      this.cgroupV2MemoryMonitor = null;
    }
  }

  public CgroupMemoryMonitor(String feed)
  {
    this(ProcSelfCgroupDiscoverer.autoCgroupDiscoverer(), feed);
  }

  public CgroupMemoryMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (isRunningOnCgroupsV2) {
      return cgroupV2MemoryMonitor.doMonitor(emitter);
    } else {
      return parseAndEmit(emitter, cgroupDiscoverer, MEMORY_USAGE_FILE, MEMORY_LIMIT_FILE, this);
    }
  }

  /**
   * Common metric parser and emitter for both v1 and v2 cgroupMemory monitors.
   */
  public static boolean parseAndEmit(
      ServiceEmitter emitter,
      CgroupDiscoverer cgroupDiscoverer,
      String memoryUsageFile,
      String memoryLimitFile,
      FeedDefiningMonitor feedDefiningMonitor
  )
  {
    final Memory memory = new Memory(cgroupDiscoverer);
    final Memory.MemoryStat stat = memory.snapshot(memoryUsageFile, memoryLimitFile);
    final ServiceMetricEvent.Builder builder = feedDefiningMonitor.builder();
    builder.setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion().name());
    emitter.emit(builder.setMetric("cgroup/memory/usage/bytes", stat.getUsage()));
    emitter.emit(builder.setMetric("cgroup/memory/limit/bytes", stat.getLimit()));

    stat.getMemoryStats().forEach((key, value) -> {
      // See https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
      // There are inconsistent units for these. Most are bytes.
      emitter.emit(builder.setMetric(StringUtils.format("cgroup/memory/%s", key), value));
    });
    stat.getNumaMemoryStats().forEach((key, value) -> {
      feedDefiningMonitor.builder().setDimension("numaZone", Long.toString(key));
      value.forEach((k, v) -> emitter.emit(builder.setMetric(StringUtils.format("cgroup/memory_numa/%s/pages", k), v)));
    });
    return true;
  }

}
