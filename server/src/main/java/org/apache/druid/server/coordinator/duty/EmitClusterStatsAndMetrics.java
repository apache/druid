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
import org.apache.druid.timeline.VersionedIntervalTimeline;

/**
 * Emits stats of the cluster and metrics of the coordination (including segment balancing) process.
 */
public class EmitClusterStatsAndMetrics implements CoordinatorDuty
{
  private static final Logger log = new Logger(EmitClusterStatsAndMetrics.class);

  public static final String TOTAL_CAPACITY = "totalCapacity";
  public static final String TOTAL_HISTORICAL_COUNT = "totalHistoricalCount";
  public static final String MAX_REPLICATION_FACTOR = "maxReplicationFactor";

  private final DruidCoordinator coordinator;
  private final String groupName;
  private final boolean isContainCompactSegmentDuty;

  public EmitClusterStatsAndMetrics(DruidCoordinator coordinator, String groupName, boolean isContainCompactSegmentDuty)
  {
    this.coordinator = coordinator;
    this.groupName = groupName;
    this.isContainCompactSegmentDuty = isContainCompactSegmentDuty;
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
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
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
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
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

  private void emitDutyStat(
      final ServiceEmitter emitter,
      final String metricName,
      final String duty,
      final long value
  )
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY, duty)
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(metricName, value)
    );
  }

  private void emitDutyStats(
      final ServiceEmitter emitter,
      final String metricName,
      final CoordinatorStats stats,
      final String statName
  )
  {
    stats.forEachDutyStat(
        statName,
        (final String duty, final long count) -> {
          emitDutyStat(emitter, metricName, duty, count);
        }
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DruidCluster cluster = params.getDruidCluster();
    CoordinatorStats stats = params.getCoordinatorStats();
    ServiceEmitter emitter = params.getEmitter();

    if (DruidCoordinator.HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP.equals(groupName)) {
      emitStatsForHistoricalManagementDuties(cluster, stats, emitter, params);
    }
    if (isContainCompactSegmentDuty) {
      emitStatsForCompactSegments(cluster, stats, emitter);
    }

    // Emit coordinator runtime stats
    emitDutyStats(emitter, "coordinator/time", stats, "runtime");

    return params;
  }

  private void emitStatsForHistoricalManagementDuties(DruidCluster cluster, CoordinatorStats stats, ServiceEmitter emitter, DruidCoordinatorRuntimeParams params)
  {
    stats.forEachTieredStat(
        "assignedCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Assigned %s segments among %,d servers",
              tier,
              count,
              cluster.getHistoricalsByTier(tier).size()
          );

          emitTieredStat(emitter, "segment/assigned/count", tier, count);
        }
    );

    stats.forEachTieredStat(
        "droppedCount",
        (final String tier, final long count) -> {
          log.info(
              "[%s] : Dropped %s segments among %,d servers",
              tier,
              count,
              cluster.getHistoricalsByTier(tier).size()
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
              tier,
              count,
              cluster.getHistoricalsByTier(tier).size()
          );
          emitTieredStat(emitter, "segment/unneeded/count", tier, count);
        }
    );

    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(
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
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.SERVER, serverName).build(
                  "segment/loadQueue/size", queuePeon.getLoadQueueSize()
              )
          );
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.SERVER, serverName).build(
                  "segment/loadQueue/failed", queuePeon.getAndResetFailedAssignCount()
              )
          );
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.SERVER, serverName).build(
                  "segment/loadQueue/count", queuePeon.getSegmentsToLoad().size()
              )
          );
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
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
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
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
                    .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                    .setDimension(DruidMetrics.TIER, tier)
                    .setDimension(DruidMetrics.DATASOURCE, dataSource).build(
                    "segment/underReplicated/count", underReplicationCount
                )
            );
          }
        }
    );

    // Emit segment metrics
    params.getUsedSegmentsTimelinesPerDataSource().forEach(
        (String dataSource, VersionedIntervalTimeline<String, DataSegment> dataSourceWithUsedSegments) -> {
          long totalSizeOfUsedSegments = dataSourceWithUsedSegments
              .iterateAllObjects()
              .stream()
              .mapToLong(DataSegment::getSize)
              .sum();
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/size", totalSizeOfUsedSegments)
          );
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/count", dataSourceWithUsedSegments.getNumObjects())
          );
        }
    );
  }

  private void emitStatsForCompactSegments(DruidCluster cluster, CoordinatorStats stats, ServiceEmitter emitter)
  {
    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(
            "compact/task/count",
            stats.getGlobalStat(CompactSegments.COMPACTION_TASK_COUNT)
        )
    );

    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(
            "compactTask/maxSlot/count",
            stats.getGlobalStat(CompactSegments.MAX_COMPACTION_TASK_SLOT)
        )
    );

    emitter.emit(
        new ServiceMetricEvent.Builder()
            .setDimension(DruidMetrics.DUTY_GROUP, groupName)
            .build(
            "compactTask/availableSlot/count",
            stats.getGlobalStat(CompactSegments.AVAILABLE_COMPACTION_TASK_SLOT)
        )
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_SIZE_OF_SEGMENTS_AWAITING,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/waitCompact/bytes", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_COUNT_OF_SEGMENTS_AWAITING,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/waitCompact/count", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_AWAITING,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("interval/waitCompact/count", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_SIZE_OF_SEGMENTS_SKIPPED,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/skipCompact/bytes", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_COUNT_OF_SEGMENTS_SKIPPED,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/skipCompact/count", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_SKIPPED,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("interval/skipCompact/count", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_SIZE_OF_SEGMENTS_COMPACTED,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/compacted/bytes", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_COUNT_OF_SEGMENTS_COMPACTED,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("segment/compacted/count", count)
          );
        }
    );

    stats.forEachDataSourceStat(
        CompactSegments.TOTAL_INTERVAL_OF_SEGMENTS_COMPACTED,
        (final String dataSource, final long count) -> {
          emitter.emit(
              new ServiceMetricEvent.Builder()
                  .setDimension(DruidMetrics.DUTY_GROUP, groupName)
                  .setDimension(DruidMetrics.DATASOURCE, dataSource)
                  .build("interval/compacted/count", count)
          );
        }
    );
  }
}
