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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Emits stats of the cluster and metrics of the coordination process.
 */
public class EmitClusterStatsAndMetrics implements CoordinatorDuty
{
  private static final Logger log = new Logger(EmitClusterStatsAndMetrics.class);

  private final DruidCoordinator coordinator;
  private final String groupName;
  private final ServiceEmitter emitter;

  public EmitClusterStatsAndMetrics(
      DruidCoordinator coordinator,
      String groupName,
      ServiceEmitter emitter
  )
  {
    this.coordinator = coordinator;
    this.groupName = groupName;
    this.emitter = emitter;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final long startTime = System.nanoTime();
    DruidCluster cluster = params.getDruidCluster();
    final CoordinatorRunStats stats = params.getCoordinatorStats();
    if (DruidCoordinator.HISTORICAL_MANAGEMENT_DUTIES_DUTY_GROUP.equals(groupName)) {
      collectStatsForHistoricalManagementDuties(cluster, stats, params);
    }

    // Emit all collected stats
    stats.forEachStat(this::emitStatWithDimensions);

    // Emit run time of this duty
    long runMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
    emitStatWithDimensions(
        Stats.Run.DUTY_TIME,
        Collections.singletonMap(DruidMetrics.DUTY, getClass().getName()),
        runMillis
    );

    return params;
  }

  private void emitStatWithDimensions(
      CoordinatorStat stat,
      Map<String, String> dimensionValues,
      long value
  )
  {
    if (stat.equals(Stats.Balancer.NORMALIZED_COST_X_1000)) {
      ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder()
          .setDimension(DruidMetrics.DUTY_GROUP, groupName);

      dimensionValues.forEach(eventBuilder::setDimension);
      emitter.emit(eventBuilder.build(stat.getMetricName(), value / 1000));
    } else if (stat.shouldEmit()) {
      ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder()
          .setDimension(DruidMetrics.DUTY_GROUP, groupName);

      dimensionValues.forEach(eventBuilder::setDimension);
      emitter.emit(eventBuilder.build(stat.getMetricName(), value));
    }
  }

  private void collectStatsForHistoricalManagementDuties(
      DruidCluster cluster,
      CoordinatorRunStats stats,
      DruidCoordinatorRuntimeParams params
  )
  {
    final ReplicationThrottler replicationThrottler = params.getReplicationManager();
    if (replicationThrottler != null) {
      replicationThrottler.getTierToNumThrottled().forEach(
          (tier, numThrottled) ->
              stats.addToTieredStat(Stats.Segments.THROTTLED_REPLICAS, tier, numThrottled)
      );
    }

    // Log load queue status of all replication or broadcast targets
    log.info("Load Queues:");
    for (ServerHolder serverHolder : cluster.getAllServers()) {
      ImmutableDruidServer server = serverHolder.getServer();
      LoadQueuePeon queuePeon = serverHolder.getPeon();
      log.info(
          "Server[%s, %s, %s] has [%,d] left to drop, [%,d (%,d MBs)] left to load, [%,d (%,d MBs)] served.",
          server.getName(),
          server.getType().toString(),
          server.getTier(),
          queuePeon.getSegmentsToDrop().size(),
          queuePeon.getSegmentsToLoad().size(),
          queuePeon.getSizeOfSegmentsToLoad() >> 20,
          server.getNumSegments(),
          server.getCurrSize() >> 20
      );
      if (log.isDebugEnabled()) {
        log.debug(
            "Segments in queue: [%s]",
            CollectionUtils.mapKeys(queuePeon.getSegmentsInQueue(), DataSegment::getId)
        );
      }
      if (log.isTraceEnabled()) {
        log.trace("Segments in queue: [%s]", queuePeon.getSegmentsInQueue());
      }
    }

    cluster.getHistoricals().forEach((tier, historicals) -> {
      long totalCapacity = historicals.stream().map(ServerHolder::getMaxSize).reduce(0L, Long::sum);
      stats.addToTieredStat(Stats.Tier.TOTAL_CAPACITY, tier, totalCapacity);
      stats.addToTieredStat(Stats.Tier.HISTORICAL_COUNT, tier, historicals.size());
    });

    // Collect load queue stats
    coordinator.getLoadManagementPeons().forEach((serverName, queuePeon) -> {
      stats.addToServerStat(Stats.Segments.BYTES_TO_LOAD, serverName, queuePeon.getSizeOfSegmentsToLoad());
      stats.addToServerStat(Stats.Segments.NUM_TO_LOAD, serverName, queuePeon.getSegmentsToLoad().size());
      stats.addToServerStat(Stats.Segments.NUM_TO_DROP, serverName, queuePeon.getSegmentsToDrop().size());
      stats.addToServerStat(Stats.Segments.FAILED_LOADS, serverName, queuePeon.getAndResetFailedAssignCount());
    });

    coordinator.computeNumsUnavailableUsedSegmentsPerDataSource().forEach(
        (dataSource, numUnavailable) ->
            stats.addToDatasourceStat(Stats.Segments.UNAVAILABLE, dataSource, numUnavailable)
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

    // Collect total segment stats
    params.getUsedSegmentsTimelinesPerDataSource().forEach(
        (dataSource, timeline) -> {
          long totalSizeOfUsedSegments = timeline.iterateAllObjects().stream().mapToLong(DataSegment::getSize).sum();
          stats.addToDatasourceStat(Stats.Segments.SIZE, dataSource, totalSizeOfUsedSegments);
          stats.addToDatasourceStat(Stats.Segments.COUNT, dataSource, timeline.getNumObjects());
        }
    );
  }

}
