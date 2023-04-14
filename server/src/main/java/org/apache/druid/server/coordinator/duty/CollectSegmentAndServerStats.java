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
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CollectionUtils;

import java.util.Map;

/**
 * Collects stats pertaining to segment availability on different servers.
 */
public class CollectSegmentAndServerStats implements CoordinatorDuty
{
  private static final Logger log = new Logger(CollectSegmentAndServerStats.class);

  private final DruidCoordinator coordinator;

  public CollectSegmentAndServerStats(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    logServerAndLoadQueueStates(params.getDruidCluster());

    final CoordinatorRunStats stats = collectSegmentStats(params);
    stats.forEachRow(
        (dimValues, statValues) ->
            log.info("Stats for dims[%s] are [%s]", dimValues, statValues)
    );

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private CoordinatorRunStats collectSegmentStats(DruidCoordinatorRuntimeParams params)
  {
    final CoordinatorRunStats stats = new CoordinatorRunStats();

    final ReplicationThrottler replicationThrottler = params.getReplicationManager();
    if (replicationThrottler != null) {
      replicationThrottler.getTierToNumThrottled().forEach(
          (tier, numThrottled) ->
              stats.addToTieredStat(Stats.Segments.THROTTLED_REPLICAS, tier, numThrottled)
      );
    }

    final DruidCluster cluster = params.getDruidCluster();
    cluster.getHistoricals().forEach((tier, historicals) -> {
      long totalCapacity = historicals.stream().map(ServerHolder::getMaxSize).reduce(0L, Long::sum);
      stats.addToTieredStat(Stats.Tier.TOTAL_CAPACITY, tier, totalCapacity);
      stats.addToTieredStat(Stats.Tier.HISTORICAL_COUNT, tier, historicals.size());
    });

    // Collect load queue stats
    coordinator.getLoadManagementPeons().forEach((serverName, queuePeon) -> {
      stats.addToServerStat(Stats.SegmentQueue.BYTES_TO_LOAD, serverName, queuePeon.getSizeOfSegmentsToLoad());
      stats.addToServerStat(Stats.SegmentQueue.NUM_TO_LOAD, serverName, queuePeon.getSegmentsToLoad().size());
      stats.addToServerStat(Stats.SegmentQueue.NUM_TO_DROP, serverName, queuePeon.getSegmentsToDrop().size());

      queuePeon.getAndResetStats().forEachStat(
          (dimValues, stat, statValue) ->
            stats.add(stat, createRowKeyForServer(serverName, dimValues), statValue)
      );
    });

    coordinator.computeNumsUnavailableUsedSegmentsPerDataSource().forEach(
        (dataSource, numUnavailable) ->
            stats.addToDatasourceStat(Stats.Segments.UNAVAILABLE, dataSource, numUnavailable)
    );

    coordinator.computeUnderReplicationCountsPerDataSourcePerTier().forEach(
        (tier, countsPerDatasource) -> countsPerDatasource.forEach(
            (dataSource, underReplicatedCount) ->
                stats.addToSegmentStat(Stats.Segments.UNDER_REPLICATED, tier, dataSource, underReplicatedCount)
        )
    );

    // Collect total segment stats
    params.getUsedSegmentsTimelinesPerDataSource().forEach(
        (dataSource, timeline) -> {
          long totalSizeOfUsedSegments = timeline.iterateAllObjects().stream().mapToLong(DataSegment::getSize).sum();
          stats.addToDatasourceStat(Stats.Segments.USED_BYTES, dataSource, totalSizeOfUsedSegments);
          stats.addToDatasourceStat(Stats.Segments.USED, dataSource, timeline.getNumObjects());
        }
    );

    return stats;
  }

  private RowKey createRowKeyForServer(String serverName, Map<Dimension, String> dimensionValues)
  {
    final RowKey.Builder builder = RowKey.builder();
    dimensionValues.forEach(builder::add);
    builder.add(Dimension.SERVER, serverName);
    return builder.build();
  }

  private void logServerAndLoadQueueStates(DruidCluster cluster)
  {
    log.info("Load Queues:");
    for (ServerHolder serverHolder : cluster.getAllServers()) {
      ImmutableDruidServer server = serverHolder.getServer();
      LoadQueuePeon queuePeon = serverHolder.getPeon();
      log.info(
          "Server[%s, %s, %s] has [%,d] left to drop, [%,d (%,d MBs)] left to load, [%,d (%,d MBs)] served.",
          server.getName(), server.getType().toString(), server.getTier(),
          queuePeon.getSegmentsToDrop().size(), queuePeon.getSegmentsToLoad().size(),
          queuePeon.getSizeOfSegmentsToLoad() >> 20,
          server.getNumSegments(), server.getCurrSize() >> 20
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
  }

}
