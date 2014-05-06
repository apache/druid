/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.collections.CountingMap;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
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

  private <T extends Number> void emitTieredStats(
      final ServiceEmitter emitter,
      final String formatString,
      final Map<String, T> statMap
  )
  {
    if (statMap != null) {
      for (Map.Entry<String, T> entry : statMap.entrySet()) {
        String tier = entry.getKey();
        Number value = entry.getValue();
        emitter.emit(
            new ServiceMetricEvent.Builder().build(
                String.format(formatString, tier), value.doubleValue()
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
        emitter, "coordinator/%s/cost/raw",
        stats.getPerTierStats().get("initialCost")
    );

    emitTieredStats(
        emitter, "coordinator/%s/cost/normalization",
        stats.getPerTierStats().get("normalization")
    );

    emitTieredStats(
        emitter, "coordinator/%s/moved/count",
        stats.getPerTierStats().get("movedCount")
    );

    emitTieredStats(
        emitter, "coordinator/%s/deleted/count",
        stats.getPerTierStats().get("deletedCount")
    );

    Map<String, AtomicLong> normalized = stats.getPerTierStats().get("normalizedInitialCostTimesOneThousand");
    if (normalized != null) {
      emitTieredStats(
          emitter, "coordinator/%s/cost/normalized",
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

    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "coordinator/overShadowed/count", stats.getGlobalStats().get("overShadowedCount")
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
    log.info("Load Queues:");
    for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
      for (ServerHolder serverHolder : serverHolders) {
        DruidServer server = serverHolder.getServer();
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
              .setUser1(serverName).build(
              "coordinator/loadQueue/size", queuePeon.getLoadQueueSize()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "coordinator/loadQueue/failed", queuePeon.getAndResetFailedAssignCount()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "coordinator/loadQueue/count", queuePeon.getSegmentsToLoad().size()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "coordinator/dropQueue/count", queuePeon.getSegmentsToDrop().size()
          )
      );
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
              .setUser1(dataSource).build(
              "coordinator/segment/size", size
          )
      );
    }
    for (Map.Entry<String, Long> entry : segmentCounts.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long count = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(dataSource).build(
              "coordinator/segment/count", count
          )
      );
    }

    return params;
  }
}
