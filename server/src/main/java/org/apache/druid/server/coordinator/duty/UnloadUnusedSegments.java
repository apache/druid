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
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 * Unloads segments that are no longer marked as used from servers.
 */
public class UnloadUnusedSegments implements CoordinatorDuty
{
  private static final Logger log = new Logger(UnloadUnusedSegments.class);

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    CoordinatorStats stats = new CoordinatorStats();
    Set<DataSegment> usedSegments = params.getUsedSegments();
    DruidCluster cluster = params.getDruidCluster();

    Map<String, Boolean> broadcastStatusByDatasource = new HashMap<>();
    for (String broadcastDatasource : params.getBroadcastDatasources()) {
      broadcastStatusByDatasource.put(broadcastDatasource, true);
    }

    for (SortedSet<ServerHolder> serverHolders : cluster.getSortedHistoricalsByTier()) {
      for (ServerHolder serverHolder : serverHolders) {
        handleUnusedSegmentsForServer(
            serverHolder,
            usedSegments,
            params,
            stats,
            false,
            broadcastStatusByDatasource
        );
      }
    }

    for (ServerHolder serverHolder : cluster.getBrokers()) {
      handleUnusedSegmentsForServer(
          serverHolder,
          usedSegments,
          params,
          stats,
          false,
          broadcastStatusByDatasource
      );
    }

    for (ServerHolder serverHolder : cluster.getRealtimes()) {
      handleUnusedSegmentsForServer(
          serverHolder,
          usedSegments,
          params,
          stats,
          true,
          broadcastStatusByDatasource
      );
    }

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private void handleUnusedSegmentsForServer(
      ServerHolder serverHolder,
      Set<DataSegment> usedSegments,
      DruidCoordinatorRuntimeParams params,
      CoordinatorStats stats,
      boolean dropBroadcastOnly,
      Map<String, Boolean> broadcastStatusByDatasource
  )
  {
    ImmutableDruidServer server = serverHolder.getServer();
    for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
      boolean isBroadcastDatasource = broadcastStatusByDatasource.computeIfAbsent(
          dataSource.getName(),
          (dataSourceName) -> {
            List<Rule> rules = params.getDatabaseRuleManager().getRulesWithDefault(dataSource.getName());
            for (Rule rule : rules) {
              // A datasource is considered a broadcast datasource if it has any broadcast rules.
              if (rule instanceof BroadcastDistributionRule) {
                return true;
              }
            }
            return false;
          }
      );

      // The coordinator tracks used segments by examining the metadata store.
      // For tasks, the segments they create are unpublished, so those segments will get dropped
      // unless we exclude them here. We currently drop only broadcast segments in that case.
      // This check relies on the assumption that queryable stream tasks will never
      // ingest data to a broadcast datasource. If a broadcast datasource is switched to become a non-broadcast
      // datasource, this will result in the those segments not being dropped from tasks.
      // A more robust solution which requires a larger rework could be to expose
      // the set of segments that were created by a task/indexer here, and exclude them.
      if (dropBroadcastOnly && !isBroadcastDatasource) {
        continue;
      }

      for (DataSegment segment : dataSource.getSegments()) {
        if (!usedSegments.contains(segment)) {
          LoadQueuePeon queuePeon = params.getLoadManagementPeons().get(server.getName());

          if (!queuePeon.getSegmentsToDrop().contains(segment)) {
            queuePeon.dropSegment(segment, () -> {});
            stats.addToTieredStat("unneededCount", server.getTier(), 1);
            log.info(
                "Dropping uneeded segment [%s] from server [%s] in tier [%s]",
                segment.getId(),
                server.getName(),
                server.getTier()
            );
          }
        }
      }
    }
  }
}
