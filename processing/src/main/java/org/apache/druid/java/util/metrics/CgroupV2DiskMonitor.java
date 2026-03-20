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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Disk;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupV2Discoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Monitor that reports disk usage stats by reading `io.stat` reported by cgroupv2
 */
public class CgroupV2DiskMonitor extends FeedDefiningMonitor
{
  private static final Logger LOG = new Logger(CgroupV2DiskMonitor.class);
  private static final String IO_STAT = "io.stat";
  final CgroupDiscoverer cgroupDiscoverer;
  private final KeyedDiff diff = new KeyedDiff();

  public CgroupV2DiskMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  @VisibleForTesting
  CgroupV2DiskMonitor(CgroupDiscoverer cgroupDiscoverer)
  {
    this(cgroupDiscoverer, DEFAULT_METRICS_FEED);
  }

  CgroupV2DiskMonitor()
  {
    this(new ProcSelfCgroupDiscoverer(ProcCgroupV2Discoverer.class));
  }


  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    for (Disk.Metrics entry : snapshot()) {
      final Map<String, Long> stats = diff.to(
          entry.getDiskName(),
          ImmutableMap.<String, Long>builder()
                      .put(CgroupUtil.DISK_READ_BYTES_METRIC, entry.getReadBytes())
                      .put(CgroupUtil.DISK_READ_COUNT_METRIC, entry.getReadCount())
                      .put(CgroupUtil.DISK_WRITE_BYTES_METRIC, entry.getWriteBytes())
                      .put(CgroupUtil.DISK_WRITE_COUNT_METRIC, entry.getWriteCount())
                      .build()
      );

      if (stats != null) {
        final ServiceMetricEvent.Builder builder = builder()
            .setDimension("diskName", entry.getDiskName());
        builder.setDimension("cgroupversion", cgroupDiscoverer.getCgroupVersion());
        for (Map.Entry<String, Long> stat : stats.entrySet()) {
          emitter.emit(builder.setMetric(stat.getKey(), stat.getValue()));
        }
      }
    }
    return true;
  }

  /*
  file: io.stat

  sample content:
  8:0 rbytes=933898 wbytes=110870538 rios=238 wios=7132 dbytes=0 dios=0
  15:0 rbytes=34566 wbytes=3466756 rios=23 wios=71 dbytes=0 dios=0
  */
  public List<Disk.Metrics> snapshot()
  {
    List<Disk.Metrics> diskStats = new ArrayList<>();
    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover("disk").toString(), IO_STAT))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        Disk.Metrics disk = getDiskMetrics(line);
        diskStats.add(disk);
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.error(ex, "Unable to fetch memory snapshot");
    }
    return diskStats;
  }

  private static Disk.Metrics getDiskMetrics(String line)
  {
    final String[] parts = line.split(Pattern.quote(" "));

    Disk.Metrics disk = new Disk.Metrics(parts[0]);
    Map<String, Long> stats = new HashMap<>();
    for (int i = 1; i < parts.length; i++) {
      String[] keyValue = parts[i].split("=");
      if (keyValue.length == 2) {
        stats.put(keyValue[0], Long.parseLong(keyValue[1]));
      }
    }

    disk.setReadBytes(stats.get("rbytes"));
    disk.setReadCount(stats.get("rios"));
    disk.setWriteBytes(stats.get("wbytes"));
    disk.setWriteCount(stats.get("wios"));
    return disk;
  }
}
