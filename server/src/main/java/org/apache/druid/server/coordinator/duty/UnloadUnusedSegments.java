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

import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Unloads segments that are no longer marked as used from servers.
 */
public class UnloadUnusedSegments implements CoordinatorDuty
{
  private static final Logger log = new Logger(UnloadUnusedSegments.class);

  private final SegmentLoadQueueManager loadQueueManager;

  public UnloadUnusedSegments(SegmentLoadQueueManager loadQueueManager)
  {
    this.loadQueueManager = loadQueueManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final Map<String, Boolean> broadcastStatusByDatasource = new HashMap<>();
    for (String broadcastDatasource : params.getBroadcastDatasources()) {
      broadcastStatusByDatasource.put(broadcastDatasource, true);
    }

    final CoordinatorRunStats stats = new CoordinatorRunStats();
    params.getDruidCluster().getAllServers().forEach(
        server -> handleUnusedSegmentsForServer(
            server,
            params,
            stats,
            broadcastStatusByDatasource
        )
    );

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private void handleUnusedSegmentsForServer(
      ServerHolder serverHolder,
      DruidCoordinatorRuntimeParams params,
      CoordinatorRunStats stats,
      Map<String, Boolean> broadcastStatusByDatasource
  )
  {
    ImmutableDruidServer server = serverHolder.getServer();
    for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
      boolean isBroadcastDatasource = broadcastStatusByDatasource.computeIfAbsent(
          dataSource.getName(),
          dataSourceName -> isBroadcastDatasource(dataSourceName, params)
      );

      // The coordinator tracks used segments by examining the metadata store.
      // For tasks, the segments they create are unpublished, so those segments will get dropped
      // unless we exclude them here. We currently drop only broadcast segments in that case.
      // This check relies on the assumption that queryable stream tasks will never
      // ingest data to a broadcast datasource. If a broadcast datasource is switched to become a non-broadcast
      // datasource, this will result in the those segments not being dropped from tasks.
      // A more robust solution which requires a larger rework could be to expose
      // the set of segments that were created by a task/indexer here, and exclude them.
      if (serverHolder.isRealtimeServer() && !isBroadcastDatasource) {
        continue;
      }

      int totalUnneededCount = 0;
      final Set<DataSegment> usedSegments = params.getUsedSegments();
      for (DataSegment segment : dataSource.getSegments()) {
        if (!usedSegments.contains(segment)
            && loadQueueManager.dropSegment(segment, serverHolder)) {
          totalUnneededCount++;
          log.info(
              "Dropping uneeded segment [%s] from server [%s] in tier [%s]",
              segment.getId(), server.getName(), server.getTier()
          );
        }
      }

      if (totalUnneededCount > 0) {
        stats.addToSegmentStat(Stats.Segments.UNNEEDED, server.getTier(), dataSource.getName(), totalUnneededCount);
      }
    }
  }

  /**
   * A datasource is considered a broadcast datasource if it has even one broadcast rule.
   */
  private boolean isBroadcastDatasource(String datasource, DruidCoordinatorRuntimeParams params)
  {
    return params.getDatabaseRuleManager().getRulesWithDefault(datasource).stream()
                 .anyMatch(rule -> rule instanceof BroadcastDistributionRule);
  }
}
