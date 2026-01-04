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
import org.apache.druid.java.util.metrics.cgroups.Disk;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

import java.util.Map;

public class CgroupDiskMonitor extends FeedDefiningMonitor
{
  private static final Logger LOG = new Logger(CgroupDiskMonitor.class);
  private final CgroupDiscoverer cgroupDiscoverer;
  private final KeyedDiff diff = new KeyedDiff();
  private final boolean isRunningOnCgroupsV2;
  private final CgroupV2DiskMonitor cgroupV2DiskMonitor;

  public CgroupDiskMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;

    // Check if we're running on cgroups v2
    this.isRunningOnCgroupsV2 = cgroupDiscoverer.getCgroupVersion().equals(CgroupVersion.V2);
    if (isRunningOnCgroupsV2) {
      this.cgroupV2DiskMonitor = new CgroupV2DiskMonitor(cgroupDiscoverer, feed);
      LOG.info("Detected cgroups v2, using CgroupV2DiskMonitor behavior for accurate metrics");
    } else {
      this.cgroupV2DiskMonitor = null;
    }
  }

  public CgroupDiskMonitor(String feed)
  {
    this(ProcSelfCgroupDiscoverer.autoCgroupDiscoverer(), feed);
  }

  public CgroupDiskMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    if (isRunningOnCgroupsV2) {
      return cgroupV2DiskMonitor.doMonitor(emitter);
    } else {
      return doMonitorV1(emitter);
    }
  }

  private boolean doMonitorV1(ServiceEmitter emitter)
  {
    Map<String, Disk.Metrics> snapshot = new Disk(cgroupDiscoverer).snapshot();
    for (Map.Entry<String, Disk.Metrics> entry : snapshot.entrySet()) {
      final Map<String, Long> stats = diff.to(
          entry.getKey(),
          ImmutableMap.<String, Long>builder()
                      .put(CgroupUtil.DISK_READ_BYTES_METRIC, entry.getValue().getReadBytes())
                      .put(CgroupUtil.DISK_READ_COUNT_METRIC, entry.getValue().getReadCount())
                      .put(CgroupUtil.DISK_WRITE_BYTES_METRIC, entry.getValue().getWriteBytes())
                      .put(CgroupUtil.DISK_WRITE_COUNT_METRIC, entry.getValue().getWriteCount())
                      .build()
      );

      if (stats != null) {
        final ServiceMetricEvent.Builder builder = builder()
            .setDimension("diskName", entry.getValue().getDiskName());
        builder.setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion());
        for (Map.Entry<String, Long> stat : stats.entrySet()) {
          emitter.emit(builder.setMetric(stat.getKey(), stat.getValue()));
        }
      }
    }
    return true;
  }
}
