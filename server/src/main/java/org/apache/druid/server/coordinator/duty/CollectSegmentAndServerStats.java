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
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.LoadQueueTaskMaster;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

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

  private final LoadQueueTaskMaster taskMaster;

  public CollectSegmentAndServerStats(LoadQueueTaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    params.getDruidCluster().getHistoricals()
          .forEach(this::logHistoricalTierStats);
    logServerDebuggingInfo(params.getDruidCluster());
    collectLoadQueueStats(params.getCoordinatorStats());

    return params;
  }

  private void collectLoadQueueStats(CoordinatorRunStats stats)
  {
    taskMaster.getAllPeons().forEach((serverName, queuePeon) -> {
      final RowKey rowKey = RowKey.of(Dimension.SERVER, serverName);
      stats.add(Stats.SegmentQueue.BYTES_TO_LOAD, rowKey, queuePeon.getSizeOfSegmentsToLoad());
      stats.add(Stats.SegmentQueue.NUM_TO_LOAD, rowKey, queuePeon.getSegmentsToLoad().size());
      stats.add(Stats.SegmentQueue.NUM_TO_DROP, rowKey, queuePeon.getSegmentsToDrop().size());

      queuePeon.getAndResetStats().forEachStat(
          (stat, key, statValue) ->
              stats.add(stat, createRowKeyForServer(serverName, key.getValues()), statValue)
      );
    });
  }

  private RowKey createRowKeyForServer(String serverName, Map<Dimension, String> dimensionValues)
  {
    final RowKey.Builder builder = RowKey.with(Dimension.SERVER, serverName);
    dimensionValues.forEach(builder::with);
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
        "Tier[%s] is serving [%,d], loading [%,d] and dropping [%,d] segments"
        + " across [%d] historicals with average usage[%d GBs], [%.1f%%].",
        tier, servedCount.get(), loadingCount.get(), droppingCount.get(), numHistoricals,
        (currentBytesSum.get() >> 30) / numHistoricals, usageSum.get() / numHistoricals
    );
  }

  private void logServerDebuggingInfo(DruidCluster cluster)
  {
    if (log.isDebugEnabled()) {
      log.debug("Servers");
      for (ServerHolder serverHolder : cluster.getAllServers()) {
        ImmutableDruidServer druidServer = serverHolder.getServer();
        log.debug("  %s", druidServer);
        log.debug("    -- DataSources");
        for (ImmutableDruidDataSource druidDataSource : druidServer.getDataSources()) {
          log.debug("    %s", druidDataSource);
        }
      }
    }
  }

}
