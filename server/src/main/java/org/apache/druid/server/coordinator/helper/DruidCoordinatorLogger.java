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

package org.apache.druid.server.coordinator.helper;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.rules.LoadRule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 */
public class DruidCoordinatorLogger implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorLogger.class);
  private final DruidCoordinator coordinator;
  public static final String TOTAL_CAPACITY = "totalCapacity";
  public static final String TOTAL_HISTORICAL_COUNT = "totalHistoricalCount";
  public static final String MAX_REPLICATION_FACTOR = "maxReplicationFactor";

  public DruidCoordinatorLogger(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  private void emitTieredStat(
      final ServiceEmitter emitter,
      final String metricName,
      final String tier,
      final double value
  )
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.TIER, tier)
            .build(metricName, value)
    );
  }

  private void emitTieredStat(
      final ServiceEmitter emitter,
      final String metricName,
      final String tier,
      final long value
  )
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.TIER, tier)
            .build(metricName, value)
    );
  }

  private void emitTieredStats(
      final ServiceEmitter emitter,
      final String metricName,
      final CoordinatorStats stats,
      final String statName
  )
  {
    stats.forEachTieredStat(
        statName,
        (final String tier, final long count) -> {
          emitTieredStat(emitter, metricName, tier, count);
        }
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DruidCluster cluster = params.getDruidCluster();
    CoordinatorStats stats = params.getCoordinatorStats();
    ServiceEmitter emitter = params.getEmitter();

    stats.forEachTieredStat(
        "assignedCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Assigned %s segments among %,d servers",
              tier, count, cluster.getHistoricalsByTier(tier).size()
          );

          emitTieredStat(emitter, "segment/assigned/count", tier, count);
        }
    );

    stats.forEachTieredStat(
        "droppedCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Dropped %s segments among %,d servers",
              tier, count, cluster.getHistoricalsByTier(tier).size()
          );

          emitTieredStat(emitter, "segment/dropped/count", tier, count);
        }
    );

    emitTieredStats(emitter, "segment/cost/raw", stats, "initialCost");

    emitTieredStats(emitter, "segment/cost/normalization", stats, "normalization");

    emitTieredStats(emitter, "segment/moved/count", stats, "movedCount");

    emitTieredStats(emitter, "segment/deleted/count", stats, "deletedCount");

    stats.forEachTieredStat(
        "normalizedInitialCostTimesOneThousand",
        (final String tier, final long count) -> {
          emitTieredStat(emitter, "segment/cost/normalized", tier, count / 1000d);
        }
    );

    stats.forEachTieredStat(
        "unneededCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Removed %s unneeded segments among %,d servers",
              tier, count, cluster.getHistoricalsByTier(tier).size()
          );
          emitTieredStat(emitter, "segment/unneeded/count", tier, count);
        }
    );

    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "segment/overShadowed/count",
            stats.getGlobalStat("overShadowedCount")
        )
    );

    stats.forEachTieredStat(
        "movedCount",
        (final String tier, final long count) -> {
          log.info("[%s] : Moved %,d segment(s)", tier, count);
        }
    );

    stats.forEachTieredStat(
        "unmovedCount",
        (final String tier, final long count) -> {
          log.info("[%s] : Let alone %,d segment(s)", tier, count);
        }
    );

    log.info("Load Queues:");
    for (Iterable<ServerHolder> serverHolders : cluster.getSortedHistoricalsByTier()) {
      for (ServerHolder serverHolder : serverHolders) {
        ImmutableDruidServer server = serverHolder.getServer();
        LoadQueuePeon queuePeon = serverHolder.getPeon();
        log.info(
            "Server[%s, %s, %s] has %,d left to load, %,d left to drop, %,d bytes queued, %,d bytes served.",
            server.getName(),
            server.getType().toString(),
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
        stats.addToTieredStat(TOTAL_CAPACITY, server.getTier(), server.getMaxSize());
        stats.addToTieredStat(TOTAL_HISTORICAL_COUNT, server.getTier(), 1);
      }
    }


    params.getDatabaseRuleManager()
          .getAllRules()
          .values()
          .forEach(
              rules -> rules.forEach(
                  rule -> {
                    if (rule instanceof LoadRule) {
                      ((LoadRule) rule).getTieredReplicants()
                                       .forEach(
                                           (tier, replica) -> stats.accumulateMaxTieredStat(
                                               MAX_REPLICATION_FACTOR,
                                               tier,
                                               replica
                                           ));
                    }
                  }
              ));

    emitTieredStats(emitter, "tier/required/capacity", stats, LoadRule.REQUIRED_CAPACITY);
    emitTieredStats(emitter, "tier/total/capacity", stats, TOTAL_CAPACITY);

    emitTieredStats(emitter, "tier/replication/factor", stats, MAX_REPLICATION_FACTOR);
    emitTieredStats(emitter, "tier/historical/count", stats, TOTAL_HISTORICAL_COUNT);

    // Emit coordinator metrics
    params
        .getLoadManagementPeons()
        .forEach((final String serverName, final LoadQueuePeon queuePeon) -> {
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
        });

    coordinator.computeNumsUnavailableUsedSegmentsPerDataSource().object2IntEntrySet().forEach(
        (final Object2IntMap.Entry<String> entry) -> {
          final String dataSource = entry.getKey();
          final int numUnavailableUsedSegmentsInDataSource = entry.getIntValue();
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
                  "segment/unavailable/count", numUnavailableUsedSegmentsInDataSource
              )
          );
        }
    );

    coordinator.computeUnderReplicationCountsPerDataSourcePerTier().forEach(
        (final String tier, final Object2LongMap<String> underReplicationCountsPerDataSource) -> {
          for (final Object2LongMap.Entry<String> entry : underReplicationCountsPerDataSource.object2LongEntrySet()) {
            final String dataSource = entry.getKey();
            final long underReplicationCount = entry.getLongValue();

            emitter.emit(
                new ServiceMetricEvent.Builder()
                    .setDimension(DruidMetrics.TIER, tier)
                    .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
                    "segment/underReplicated/count", underReplicationCount
                )
            );
          }
        }
    );

    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            "compact/task/count",
            stats.getGlobalStat(DruidCoordinatorSegmentCompactor.COMPACT_TASK_COUNT)
        )
    );

    stats.forEachDataSourceStat(
        "segmentsWaitCompact",
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/waitCompact/count", count)
          );
        }
    );

    // Emit segment metrics
    final Stream<DataSegment> allSegments = params
        .getUsedSegmentsTimelinesPerDataSource()
        .values()
        .stream()
        .flatMap(timeline -> timeline.getAllTimelineEntries().values().stream())
        .flatMap(entryMap -> entryMap.values().stream())
        .flatMap(entry -> StreamSupport.stream(entry.getPartitionHolder().spliterator(), false))
        .map(PartitionChunk::getObject);

    allSegments
        .collect(Collectors.groupingBy(DataSegment::getDataSource))
        .forEach((final String name, final List<DataSegment> segments) -> {
          final long size = segments.stream().mapToLong(DataSegment::getSize).sum();
          emitter.emit(
              new ServiceMetricEvent.Builder().setDimension(DruidMetrics.DATASOURCE, name).build("segment/size", size)
          );
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DATASOURCE, name)
                  .build("segment/count", segments.size())
          );
        });

    return params;
  }
}
