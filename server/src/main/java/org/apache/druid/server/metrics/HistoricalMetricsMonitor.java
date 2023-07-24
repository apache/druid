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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

public class HistoricalMetricsMonitor extends AbstractMonitor
{
  private final DruidServerConfig serverConfig;
  private final SegmentManager segmentManager;
  private final SegmentLoadDropHandler segmentLoadDropMgr;

  @Inject
  public HistoricalMetricsMonitor(
      DruidServerConfig serverConfig,
      SegmentManager segmentManager,
      SegmentLoadDropHandler segmentLoadDropMgr
  )
  {
    this.serverConfig = serverConfig;
    this.segmentManager = segmentManager;
    this.segmentLoadDropMgr = segmentLoadDropMgr;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emitter.emit(new ServiceMetricEvent.Builder().build("segment/max", serverConfig.getMaxSize()));

    final Object2LongOpenHashMap<String> pendingDeleteSizes = new Object2LongOpenHashMap<>();

    for (DataSegment segment : segmentLoadDropMgr.getPendingDeleteSnapshot()) {
      pendingDeleteSizes.addTo(segment.getDataSource(), segment.getSize());
    }

    for (final Object2LongMap.Entry<String> entry : pendingDeleteSizes.object2LongEntrySet()) {

      final String dataSource = entry.getKey();
      final long pendingDeleteSize = entry.getLongValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.DATASOURCE, dataSource)
              .setDimension("tier", serverConfig.getTier())
              .setDimension("priority", String.valueOf(serverConfig.getPriority()))
              .build("segment/pendingDelete", pendingDeleteSize)
      );
    }

    for (Map.Entry<String, Long> entry : segmentManager.getDataSourceSizes().entrySet()) {
      String dataSource = entry.getKey();
      long used = entry.getValue();

      final ServiceMetricEvent.Builder builder =
          new ServiceMetricEvent.Builder().setDimension(DruidMetrics.DATASOURCE, dataSource)
                                          .setDimension("tier", serverConfig.getTier())
                                          .setDimension("priority", String.valueOf(serverConfig.getPriority()));


      emitter.emit(builder.build("segment/used", used));
      final double usedPercent = serverConfig.getMaxSize() == 0 ? 0 : used / (double) serverConfig.getMaxSize();
      emitter.emit(builder.build("segment/usedPercent", usedPercent));
    }

    for (Map.Entry<String, Long> entry : segmentManager.getDataSourceCounts().entrySet()) {
      String dataSource = entry.getKey();
      long count = entry.getValue();
      final ServiceMetricEvent.Builder builder =
          new ServiceMetricEvent.Builder().setDimension(DruidMetrics.DATASOURCE, dataSource)
                                          .setDimension("tier", serverConfig.getTier())
                                          .setDimension(
                                              "priority",
                                              String.valueOf(serverConfig.getPriority())
                                          );

      emitter.emit(builder.build("segment/count", count));
    }

    return true;
  }
}
