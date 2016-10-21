/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.DruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.collections.CountingMap;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DruidMetrics;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class DruidCoordinatorLogger implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorLogger.class);
  private final DruidCoordinator coordinator;

  public DruidCoordinatorLogger(DruidCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  private <T extends Number> void emitTieredStats(
      final ServiceEmitter emitter,
      final String metricName,
      final Map<String, T> statMap
  )
  {
    if (statMap != null) {
      for (Map.Entry<String, T> entry : statMap.entrySet()) {
        String tier = entry.getKey();
        Number value = entry.getValue();
        emitter.emit(
            new ServiceMetricEvent.Builder()
                .setDimension(DruidMetrics.TIER, tier)
                .build(
                    metricName, value.doubleValue()
                )
        );
      }
    }
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DruidCluster cluster = params.getDruidCluster();
    CoordinatorStats stats = params.getCoordinatorStats();
    ServiceEmitter emitter = params.getEmitter();

    Map<String, AtomicLong> assigned = stats.getPerTierStats().get("assignedCount");
    if (assigned != null) {
      for (Map.Entry<String, AtomicLong> entry : assigned.entrySet()) {
        log.info(
            "[%s] : Assigned %s segments among %,d servers",
            entry.getKey(), entry.getValue().get(), cluster.get(entry.getKey()).size()
        );
      }
    }

    emitTieredStats(
        emitter, "segment/assigned/count",
        assigned
    );

    Map<String, AtomicLong> dropped = stats.getPerTierStats().get("droppedCount");
    if (dropped != null) {
      for (Map.Entry<String, AtomicLong> entry : dropped.entrySet()) {
        log.info(
            "[%s] : Dropped %s segments among %,d servers",
            entry.getKey(), entry.getValue().get(), cluster.get(entry.getKey()).size()
        );
      }
    }

    emitTieredStats(
        emitter, "segment/dropped/count",
        dropped
    );

    emitTieredStats(
        emitter, "segment/cost/raw",
        stats.getPerTierStats().get("initialCost")
    );

    emitTieredStats(
        emitter, "segment/cost/normalization",
        stats.getPerTierStats().get("normalization")
    );

    emitTieredStats(
        emitter, "segment/moved/count",
        stats.getPerTierStats().get("movedCount")
    );

    emitTieredStats(
        emitter, "segment/deleted/count",
        stats.getPerTierStats().get("deletedCount")
    );

    Map<String, AtomicLong> normalized = stats.getPerTierStats().get("normalizedInitialCostTimesOneThousand");
    if (normalized != null) {
      emitTieredStats(
          emitter, "segment/cost/normalized",
          Maps.transformEntries(
              normalized,
              new Maps.EntryTransformer<String, AtomicLong, Number>()
              {
                @Override
                public Number transformEntry(String key, AtomicLong value)
                {
                  return value.doubleValue() / 1000d;
                }
              }
          )
      );
    }

    Map<String, AtomicLong> unneeded = stats.getPerTierStats().get("unneededCount");
    if (unneeded != null) {
      for (Map.Entry<String, AtomicLong> entry : unneeded.entrySet()) {
        log.info(
            "[%s] : Removed %s unneeded segments among %,d servers",
            entry.getKey(), entry.getValue().get(), cluster.get(entry.getKey()).size()
        );
      }
    }

    emitTieredStats(
        emitter, "segment/unneeded/count",
        stats.getPerTierStats().get("unneededCount")
    );

    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "segment/overShadowed/count", stats.getGlobalStats().get("overShadowedCount")
        )
    );

    Map<String, AtomicLong> moved = stats.getPerTierStats().get("movedCount");
    if (moved != null) {
      for (Map.Entry<String, AtomicLong> entry : moved.entrySet()) {
        log.info(
            "[%s] : Moved %,d segment(s)",
            entry.getKey(), entry.getValue().get()
        );
      }
    }
    final Map<String, AtomicLong> unmoved = stats.getPerTierStats().get("unmovedCount");
    if (unmoved != null) {
      for(Map.Entry<String, AtomicLong> entry : unmoved.entrySet()) {
        log.info(
            "[%s] : Let alone %,d segment(s)",
            entry.getKey(), entry.getValue().get()
        );
      }
    }
    log.info("Load Queues:");
    for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serverHolders) {
        ImmutableDruidServer server = serverHolder.getServer();
        LoadQueuePeon queuePeon = serverHolder.getPeon();
        log.info(
            "Server[%s, %s, %s] has %,d left to load, %,d left to drop, %,d bytes queued, %,d bytes served.",
            server.getName(),
            server.getType(),
            server.getTier(),
            queuePeon.getSegmentsToLoad().size(),
            queuePeon.getSegmentsToDrop().size(),
            queuePeon.getLoadQueueSize(),
            server.getCurrSize()
        );
        if (log.isDebugEnabled()) {
          for (DataSegment segment : queuePeon.getSegmentsToLoad()) {
            log.debug("Segment to load[%s]", segment);
          }
          for (DataSegment segment : queuePeon.getSegmentsToDrop()) {
            log.debug("Segment to drop[%s]", segment);
          }
        }
      }
    }

    // Emit coordinator metrics
    final Set<Map.Entry<String, LoadQueuePeon>> peonEntries = params.getLoadManagementPeons().entrySet();
    for (Map.Entry<String, LoadQueuePeon> entry : peonEntries) {
      String serverName = entry.getKey();
      LoadQueuePeon queuePeon = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/loadQueue/size", queuePeon.getLoadQueueSize()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/loadQueue/failed", queuePeon.getAndResetFailedAssignCount()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/loadQueue/count", queuePeon.getSegmentsToLoad().size()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.SERVER, serverName).build(
              "segment/dropQueue/count", queuePeon.getSegmentsToDrop().size()
          )
      );
    }
    for (Map.Entry<String, AtomicLong> entry : coordinator.getSegmentAvailability().entrySet()) {
      String datasource = entry.getKey();
      Long count = entry.getValue().get();
      emitter.emit(
              new ServiceMetricEvent.Builder()
                      .setDimension(DruidMetrics.DATASOURCE, datasource).build(
                      "segment/unavailable/count", count
              )
      );
    }
    for (Map.Entry<String, CountingMap<String>> entry : coordinator.getReplicationStatus().entrySet()) {
      String tier = entry.getKey();
      CountingMap<String> datasourceAvailabilities = entry.getValue();
      for (Map.Entry<String, AtomicLong> datasourceAvailability : datasourceAvailabilities.entrySet()) {
        String datasource = datasourceAvailability.getKey();
        Long count = datasourceAvailability.getValue().get();
        emitter.emit(
                new ServiceMetricEvent.Builder()
                        .setDimension(DruidMetrics.TIER, tier)
                        .setDimension(DruidMetrics.DATASOURCE, datasource).build(
                        "segment/underReplicated/count", count
                )
        );
      }
    }

    // Emit segment metrics
    CountingMap<String> segmentSizes = new CountingMap<String>();
    CountingMap<String> segmentCounts = new CountingMap<String>();
    for (DruidDataSource dataSource : params.getDataSources()) {
      for (DataSegment segment : dataSource.getSegments()) {
        segmentSizes.add(dataSource.getName(), segment.getSize());
        segmentCounts.add(dataSource.getName(), 1L);
      }
    }
    for (Map.Entry<String, Long> entry : segmentSizes.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long size = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
              "segment/size", size
          )
      );
    }
    for (Map.Entry<String, Long> entry : segmentCounts.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long count = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
              "segment/count", count
          )
      );
    }


    return params;
  }
}
