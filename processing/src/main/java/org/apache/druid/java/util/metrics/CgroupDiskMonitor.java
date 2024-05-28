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
import org.apache.druid.java.util.metrics.cgroups.Disk;

import java.util.Map;

public class CgroupDiskMonitor extends FeedDefiningMonitor
{
  final CgroupDiscoverer cgroupDiscoverer;
  final Map<String, String[]> dimensions;
  private final KeyedDiff diff = new KeyedDiff();

  public CgroupDiskMonitor(CgroupDiscoverer cgroupDiscoverer, final Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
    this.dimensions = dimensions;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    Map<String, Disk.Metrics> snapshot = new Disk(cgroupDiscoverer).snapshot();
    for (String disk : snapshot.keySet()) {
      Disk.Metrics metrics = snapshot.get(disk);
      final Map<String, Long> stats = diff.to(
          metrics.getDiskName(),
          ImmutableMap.<String, Long>builder()
                      .put("cgroup/disk/read/bytes", metrics.getReadBytes())
                      .put("cgroup/disk/read/count", metrics.getReadCount())
                      .put("cgroup/disk/write/bytes", metrics.getWriteBytes())
                      .put("cgroup/disk/write/count", metrics.getWriteCount())
                      .build()
      );

      if (stats != null) {
        final ServiceMetricEvent.Builder builder = builder()
            .setDimension("diskName", metrics.getDiskName());
        MonitorUtils.addDimensionsToBuilder(builder, dimensions);
        for (Map.Entry<String, Long> entry : stats.entrySet()) {
          emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
        }
      }
    }
    return true;
  }
}
