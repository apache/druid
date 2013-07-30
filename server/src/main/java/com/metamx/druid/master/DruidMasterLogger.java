/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.collect.CountingMap;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class DruidMasterLogger implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterLogger.class);

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
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    DruidCluster cluster = params.getDruidCluster();
    MasterStats stats = params.getMasterStats();
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
        emitter, "master/%s/cost/raw",
        stats.getPerTierStats().get("initialCost")
    );

    emitTieredStats(
        emitter, "master/%s/cost/normalization",
        stats.getPerTierStats().get("normalization")
    );

    emitTieredStats(
        emitter, "master/%s/moved/count",
        stats.getPerTierStats().get("movedCount")
    );

    emitTieredStats(
        emitter, "master/%s/deleted/count",
        stats.getPerTierStats().get("deletedCount")
    );

    Map<String, AtomicLong> normalized = stats.getPerTierStats().get("normalizedInitialCostTimesOneThousand");
    if (normalized != null) {
      emitTieredStats(
          emitter, "master/%s/cost/normalized",
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
            "master/overShadowed/count", stats.getGlobalStats().get("overShadowedCount")
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

    // Emit master metrics
    final Set<Map.Entry<String, LoadQueuePeon>> peonEntries = params.getLoadManagementPeons().entrySet();
    for (Map.Entry<String, LoadQueuePeon> entry : peonEntries) {
      String serverName = entry.getKey();
      LoadQueuePeon queuePeon = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/loadQueue/size", queuePeon.getLoadQueueSize()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/loadQueue/failed", queuePeon.getAndResetFailedAssignCount()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/loadQueue/count", queuePeon.getSegmentsToLoad().size()
          )
      );
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(serverName).build(
              "master/dropQueue/count", queuePeon.getSegmentsToDrop().size()
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
              "master/segment/size", size
          )
      );
    }
    for (Map.Entry<String, Long> entry : segmentCounts.snapshot().entrySet()) {
      String dataSource = entry.getKey();
      Long count = entry.getValue();
      emitter.emit(
          new ServiceMetricEvent.Builder()
              .setUser1(dataSource).build(
              "master/segment/count", count
          )
      );
    }

    return params;
  }
}
