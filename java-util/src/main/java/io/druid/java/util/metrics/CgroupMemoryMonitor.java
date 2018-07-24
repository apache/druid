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

package io.druid.java.util.metrics;

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import io.druid.java.util.metrics.cgroups.Memory;

import java.util.Map;

public class CgroupMemoryMonitor extends FeedDefiningMonitor
{
  final CgroupDiscoverer cgroupDiscoverer;
  final Map<String, String[]> dimensions;

  public CgroupMemoryMonitor(CgroupDiscoverer cgroupDiscoverer, final Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
    this.dimensions = dimensions;
  }

  public CgroupMemoryMonitor(final Map<String, String[]> dimensions, String feed)
  {
    this(null, dimensions, feed);
  }

  public CgroupMemoryMonitor(final Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public CgroupMemoryMonitor()
  {
    this(ImmutableMap.of());
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final Memory memory = new Memory(cgroupDiscoverer);
    final Memory.MemoryStat stat = memory.snapshot();
    stat.getMemoryStats().forEach((key, value) -> {
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      // See https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
      // There are inconsistent units for these. Most are bytes.
      emitter.emit(builder.build(StringUtils.format("cgroup/memory/%s", key), value));
    });
    stat.getNumaMemoryStats().forEach((key, value) -> {
      final ServiceMetricEvent.Builder builder = builder().setDimension("numaZone", Long.toString(key));
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      value.forEach((k, v) -> emitter.emit(builder.build(StringUtils.format("cgroup/memory_numa/%s/pages", k), v)));
    });
    return true;
  }
}
