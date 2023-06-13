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

import com.google.common.util.concurrent.AtomicDouble;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeon;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    params.getDruidCluster().getHistoricals()
          .forEach(this::logHistoricalTierStats);
    collectSegmentStats(params);

    StrategicSegmentAssigner segmentAssigner = params.getSegmentAssigner();
    segmentAssigner.makeAlerts();

    return params;
  }

  private void collectSegmentStats(DruidCoordinatorRuntimeParams params)
  {
    final CoordinatorRunStats stats = params.getCoordinatorStats();

    final DruidCluster cluster = params.getDruidCluster();
    cluster.getHistoricals().forEach((tier, historicals) -> {
      final RowKey rowKey = RowKey.forTier(tier);
      stats.add(Stats.Tier.HISTORICAL_COUNT, rowKey, historicals.size());
      long totalCapacity = historicals.stream().map(ServerHolder::getMaxSize).reduce(0L, Long::sum);
      stats.add(Stats.Tier.TOTAL_CAPACITY, rowKey, totalCapacity);
    });

    // Collect load queue stats
    coordinator.getLoadManagementPeons().forEach((serverName, queuePeon) -> {
      final RowKey rowKey = RowKey.builder().add(Dimension.SERVER, serverName).build();
      stats.add(Stats.SegmentQueue.BYTES_TO_LOAD, rowKey, queuePeon.getSizeOfSegmentsToLoad());
      stats.add(Stats.SegmentQueue.NUM_TO_LOAD, rowKey, queuePeon.getSegmentsToLoad().size());
      stats.add(Stats.SegmentQueue.NUM_TO_DROP, rowKey, queuePeon.getSegmentsToDrop().size());

      queuePeon.getAndResetStats().forEachStat(
          (dimValues, stat, statValue) ->
              stats.add(stat, createRowKeyForServer(serverName, dimValues), statValue)
      );
    });

    coordinator.getDatasourceToUnavailableSegmentCount().forEach(
        (dataSource, numUnavailable) ->
            stats.addToDatasourceStat(Stats.Segments.UNAVAILABLE, dataSource, numUnavailable)
    );

    coordinator.getTierToDatasourceToUnderReplicatedCount(false).forEach(
        (tier, countsPerDatasource) -> countsPerDatasource.forEach(
            (dataSource, underReplicatedCount) ->
                stats.addToSegmentStat(Stats.Segments.UNDER_REPLICATED, tier, dataSource, underReplicatedCount)
        )
    );

    // Collect total segment stats
    params.getUsedSegmentsTimelinesPerDataSource().forEach(
        (dataSource, timeline) -> {
          long totalSizeOfUsedSegments = timeline.iterateAllObjects().stream()
                                                 .mapToLong(DataSegment::getSize).sum();
          stats.addToDatasourceStat(Stats.Segments.USED_BYTES, dataSource, totalSizeOfUsedSegments);
          stats.addToDatasourceStat(Stats.Segments.USED, dataSource, timeline.getNumObjects());
        }
    );
  }

  private RowKey createRowKeyForServer(String serverName, Map<Dimension, String> dimensionValues)
  {
    final RowKey.Builder builder = RowKey.builder();
    dimensionValues.forEach(builder::add);
    builder.add(Dimension.SERVER, serverName);
    return builder.build();
  }

  private void logHistoricalTierStats(String tier, Set<ServerHolder> historicals)
  {
    final AtomicInteger servedCount = new AtomicInteger();
    final AtomicInteger loadingCount = new AtomicInteger();
    final AtomicInteger droppingCount = new AtomicInteger();

    final AtomicDouble usageSum = new AtomicDouble();
    final AtomicLong currentBytesSum = new AtomicLong();

    historicals.forEach(serverHolder -> {
      final ImmutableDruidServer server = serverHolder.getServer();
      servedCount.addAndGet(server.getNumSegments());
      currentBytesSum.addAndGet(server.getCurrSize());
      usageSum.addAndGet(100.0f * server.getCurrSize() / server.getMaxSize());

      final LoadQueuePeon queuePeon = serverHolder.getPeon();
      loadingCount.addAndGet(queuePeon.getSegmentsToLoad().size());
      droppingCount.addAndGet(queuePeon.getSegmentsToDrop().size());
    });

    final int numHistoricals = historicals.size();
    log.info(
        "Tier [%s] is serving [%,d], loading [%,d] and dropping [%,d] segments"
        + " across [%d] historicals with average usage [%d GBs], [%.1f%%].",
        tier, servedCount.get(), loadingCount.get(), droppingCount.get(), numHistoricals,
        (currentBytesSum.get() >> 30) / numHistoricals, usageSum.get() / numHistoricals
    );
  }

}
