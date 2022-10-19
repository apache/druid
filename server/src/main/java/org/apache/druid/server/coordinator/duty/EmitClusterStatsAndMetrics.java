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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Emits stats of the cluster and metrics of the coordination (including segment balancing) process.
 */
public class EmitClusterStatsAndMetrics implements CoordinatorDuty
{
  private static final Logger log = new Logger(EmitClusterStatsAndMetrics.class);

  public static final String TOTAL_HISTORICAL_COUNT = "totalHistoricalCount";

  private final DruidCoordinator coordinator;
  private final String groupName;
  private final boolean isContainCompactSegmentDuty;
  private final ServiceEmitter emitter;

  public EmitClusterStatsAndMetrics(
      DruidCoordinator coordinator,
      String groupName,
      boolean isContainCompactSegmentDuty,
      ServiceEmitter emitter
  )
  {
    this.coordinator = coordinator;
    this.groupName = groupName;
    this.isContainCompactSegmentDuty = isContainCompactSegmentDuty;
    this.emitter = emitter;
  }

  private void emitMetricForTier(String metricName, String tier, Number value)
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.TIER, tier)
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(metricName, value)
    );
  }

  private void emitMetricForServer(String metricName, String serverName, Number value)
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.SERVER, serverName)
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(metricName, value)
    );
  }

  private void emitTieredStats(String metricName, CoordinatorStats stats, String statName)
  {
    stats.forEachTieredStat(
        statName,
        (tier, count) -> emitMetricForTier(metricName, tier, count)
    );
  }

  private void emitDatasourceStats(String metricName, CoordinatorStats stats, String statName)
  {
    stats.forEachDataSourceStat(
        statName,
        (datasource, value) ->
            emitMetricWithDimension(metricName, value, DruidMetrics.DATASOURCE, datasource)
    );
  }

  private void emitGlobalStat(String metricName, CoordinatorStats stats, String statName)
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(metricName, stats.getGlobalStat(statName))
    );
  }

  private void emitMetricWithDimension(
      String metricName,
      Number metricValue,
      String dimensionName,
      String dimensionValue
  )
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(dimensionName, dimensionValue)
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(metricName, metricValue)
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final long startTime = System.nanoTime();
    DruidCluster cluster = params.getDruidCluster();
    CoordinatorStats stats = params.getCoordinatorStats();

    if (DruidCoordinator.HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP.equals(groupName)) {
      emitStatsForHistoricalManagementDuties(cluster, stats, params);
      logTierSummaries(cluster.getTierNames(), stats);
    }
    if (isContainCompactSegmentDuty) {
      emitStatsForCompactSegments(stats);
    }

    // Add run time of this duty here to ensure that it gets emitted too
    long runMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    stats.addToDutyStat("runtime", getClass().getName(), runMillis);

    // Emit coordinator runtime stats
    stats.forEachDutyStat(
        "runtime",
        (duty, count) ->
            emitMetricWithDimension("coordinator/time", count, DruidMetrics.DUTY, duty)
    );

    return params;
  }

  private void logTierSummaries(Iterable<String> tierNames, CoordinatorStats stats)
  {
    for (String tierName : tierNames) {
      final StringBuilder summary = new StringBuilder()
          .append("\n").append("Tier [").append(tierName).append("]");

      final AtomicBoolean addNewline = new AtomicBoolean(true);
      stats.getSortedTierStats(tierName).forEach((stat, value) -> {
        if (addNewline.getAndSet(!addNewline.get())) {
          summary.append("\n");
        }
        summary.append(StringUtils.format("%25s:%8d", stat, value));
      });

      log.info("Summary for tier [%s]: %s", tierName, summary.toString());
    }
  }

  private void emitStatsForHistoricalManagementDuties(
      DruidCluster cluster,
      CoordinatorStats stats,
      DruidCoordinatorRuntimeParams params
  )
  {
    stats.forEachTieredStat(
        CoordinatorStats.ASSIGNED_COUNT,
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Assigned %s segments among %,d servers",
              tier,
              count,
              cluster.getNumHistoricalsInTier(tier)
          );

          emitMetricForTier("segment/assigned/count", tier, count);
        }
    );

    stats.forEachTieredStat(
        CoordinatorStats.DROPPED_COUNT,
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Dropped %s segments among %,d servers",
              tier,
              count,
              cluster.getNumHistoricalsInTier(tier)
          );

          emitMetricForTier("segment/dropped/count", tier, count);
        }
    );

    // Emit broadcast metrics
    emitDatasourceStats("segment/broadcastLoad/count", stats, CoordinatorStats.BROADCAST_LOADS);
    emitDatasourceStats("segment/broadcastDrop/count", stats, CoordinatorStats.BROADCAST_DROPS);

    // Emit cancellation metrics
    emitTieredStats("segment/cancelLoad/count", stats, CoordinatorStats.CANCELLED_LOADS);
    emitTieredStats("segment/cancelMove/count", stats, CoordinatorStats.CANCELLED_MOVES);

    emitTieredStats("segment/cost/raw", stats, "initialCost");
    emitTieredStats("segment/cost/normalization", stats, "normalization");

    emitTieredStats("segment/moved/count", stats, CoordinatorStats.MOVED_COUNT);
    emitTieredStats("segment/unmoved/count", stats, CoordinatorStats.UNMOVED_COUNT);
    emitTieredStats("segment/deleted/count", stats, CoordinatorStats.DELETED_COUNT);

    final ReplicationThrottler replicationThrottler = params.getReplicationManager();
    if (replicationThrottler != null) {
      replicationThrottler.getTierToNumThrottled().forEach(
          (tier, numThrottled) ->
              emitMetricForTier("segment/throttledReplica/count", tier, numThrottled)
      );
    }

    stats.forEachTieredStat(
        "normalizedInitialCostTimesOneThousand",
        (tier, count) ->
            emitMetricForTier("segment/cost/normalized", tier, count / 1000d)
    );

    stats.forEachTieredStat(
        "unneededCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Removed %s unneeded segments among %,d servers",
              tier,
              count,
              cluster.getNumHistoricalsInTier(tier)
          );
          emitMetricForTier("segment/unneeded/count", tier, count);
        }
    );

    emitGlobalStat("segment/overShadowed/count", stats, "overShadowedCount");

    // Log load queue status of all replication or broadcast targets
    log.info("Load Queues:");
    for (ServerHolder serverHolder : cluster.getAllServers()) {
      ImmutableDruidServer server = serverHolder.getServer();
      LoadQueuePeon queuePeon = serverHolder.getPeon();
      log.info(
          "Server[%s, %s, %s] has %,d left to load, %,d left to drop, %,d served, %,d bytes queued, %,d bytes served.",
          server.getName(),
          server.getType().toString(),
          server.getTier(),
          queuePeon.getSegmentsToLoad().size(),
          queuePeon.getSegmentsToDrop().size(),
          server.getNumSegments(),
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

    for (Iterable<ServerHolder> historicalTier : cluster.getSortedHistoricalsByTier()) {
      for (ServerHolder historical : historicalTier) {
        final ImmutableDruidServer server = historical.getServer();
        stats.addToTieredStat(CoordinatorStats.TOTAL_CAPACITY, server.getTier(), server.getMaxSize());
        stats.addToTieredStat(TOTAL_HISTORICAL_COUNT, server.getTier(), 1);
      }
    }

    emitTieredStats("tier/required/capacity", stats, CoordinatorStats.REQUIRED_CAPACITY);
    emitTieredStats("tier/total/capacity", stats, CoordinatorStats.TOTAL_CAPACITY);

    emitTieredStats("tier/replication/factor", stats, CoordinatorStats.MAX_REPLICATION_FACTOR);
    emitTieredStats("tier/historical/count", stats, TOTAL_HISTORICAL_COUNT);

    // Emit coordinator metrics
    params.getLoadManagementPeons().forEach((serverName, queuePeon) -> {
      emitMetricForServer("segment/loadQueue/size", serverName, queuePeon.getLoadQueueSize());
      emitMetricForServer("segment/loadQueue/failed", serverName, queuePeon.getAndResetFailedAssignCount());
      emitMetricForServer("segment/loadQueue/count", serverName, queuePeon.getSegmentsToLoad().size());
      emitMetricForServer("segment/dropQueue/count", serverName, queuePeon.getSegmentsToDrop().size());
    });

    coordinator.computeNumsUnavailableUsedSegmentsPerDataSource().forEach(
        (dataSource, numUnavailable) -> emitMetricWithDimension(
            "segment/unavailable/count",
            numUnavailable,
            DruidMetrics.DATASOURCE,
            dataSource
        )
    );

    coordinator.computeUnderReplicationCountsPerDataSourcePerTier().forEach(
        (tier, countsPerDatasource) -> countsPerDatasource.forEach(
            (dataSource, underReplicatedCount) -> emitter.emit(
                new ServiceMetricEvent.Builder()
                    .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                    .setDimension(DruidMetrics.TIER, tier)
                    .setDimension(DruidMetrics.DATASOURCE, dataSource)
                    .build("segment/underReplicated/count", underReplicatedCount)
            )
        )
    );

    // Emit segment metrics
    params.getUsedSegmentsTimelinesPerDataSource().forEach(
        (dataSource, timeline) -> {
          long totalSizeOfUsedSegments = timeline.iterateAllObjects().stream().mapToLong(DataSegment::getSize).sum();
          emitMetricWithDimension("segment/size", totalSizeOfUsedSegments, DruidMetrics.DATASOURCE, dataSource);
          emitMetricWithDimension("segment/count", timeline.getNumObjects(), DruidMetrics.DATASOURCE, dataSource);
        }
    );
  }

  private void emitStatsForCompactSegments(CoordinatorStats stats)
  {
    emitGlobalStat("compact/task/count", stats, CompactSegments.COMPACTION_TASK_COUNT);
    emitGlobalStat("compactTask/maxSlot/count", stats, CompactSegments.MAX_COMPACTION_TASK_SLOT);
    emitGlobalStat("compactTask/availableSlot/count", stats, CompactSegments.AVAILABLE_COMPACTION_TASK_SLOT);

    emitDatasourceStats("segment/waitCompact/bytes", stats, CompactSegments.TOTAL_SIZE_OF_SEGMENTS_AWAITING);
    emitDatasourceStats("segment/waitCompact/count", stats, CompactSegments.TOTAL_COUNT_OF_SEGMENTS_AWAITING);

    emitDatasourceStats("interval/waitCompact/count", stats, CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_AWAITING);
    emitDatasourceStats("interval/skipCompact/count", stats, CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_SKIPPED);
    emitDatasourceStats("interval/compacted/count", stats, CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_COMPACTED);

    emitDatasourceStats("segment/skipCompact/bytes", stats, CompactSegments.TOTAL_SIZE_OF_SEGMENTS_SKIPPED);
    emitDatasourceStats("segment/skipCompact/count", stats, CompactSegments.TOTAL_COUNT_OF_SEGMENTS_SKIPPED);

    emitDatasourceStats("segment/compacted/bytes", stats, CompactSegments.TOTAL_SIZE_OF_SEGMENTS_COMPACTED);
    emitDatasourceStats("segment/compacted/count", stats, CompactSegments.TOTAL_COUNT_OF_SEGMENTS_COMPACTED);
  }
}
