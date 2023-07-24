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
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.rules.BroadcastDistributionRule;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

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

    final List<ServerHolder> allServers = params.getDruidCluster().getAllServers();
    int numCancelledLoads = allServers.stream().mapToInt(
        server -> cancelLoadOfUnusedSegments(server, broadcastStatusByDatasource, params)
    ).sum();

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    int numQueuedDrops = allServers.stream().mapToInt(
        server -> dropUnusedSegments(server, params, stats, broadcastStatusByDatasource)
    ).sum();

    if (numCancelledLoads > 0 || numQueuedDrops > 0) {
      log.info("Cancelled [%d] loads and started [%d] drops of unused segments.", numCancelledLoads, numQueuedDrops);
    }

    return params;
  }

  private int dropUnusedSegments(
      ServerHolder serverHolder,
      DruidCoordinatorRuntimeParams params,
      CoordinatorRunStats stats,
      Map<String, Boolean> broadcastStatusByDatasource
  )
  {
    final Set<DataSegment> usedSegments = params.getUsedSegments();

    final AtomicInteger numQueuedDrops = new AtomicInteger(0);
    final ImmutableDruidServer server = serverHolder.getServer();
    for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
      if (shouldSkipUnload(serverHolder, dataSource.getName(), broadcastStatusByDatasource, params)) {
        continue;
      }

      int totalUnneededCount = 0;
      for (DataSegment segment : dataSource.getSegments()) {
        if (!usedSegments.contains(segment)
            && loadQueueManager.dropSegment(segment, serverHolder)) {
          totalUnneededCount++;
          log.debug(
              "Dropping uneeded segment[%s] from server[%s] in tier[%s]",
              segment.getId(), server.getName(), server.getTier()
          );
        }
      }

      if (totalUnneededCount > 0) {
        stats.addToSegmentStat(Stats.Segments.UNNEEDED, server.getTier(), dataSource.getName(), totalUnneededCount);
        numQueuedDrops.addAndGet(totalUnneededCount);
      }
    }

    return numQueuedDrops.get();
  }

  private int cancelLoadOfUnusedSegments(
      ServerHolder server,
      Map<String, Boolean> broadcastStatusByDatasource,
      DruidCoordinatorRuntimeParams params
  )
  {
    final Set<DataSegment> usedSegments = params.getUsedSegments();

    final AtomicInteger cancelledOperations = new AtomicInteger(0);
    server.getQueuedSegments().forEach((segment, action) -> {
      if (shouldSkipUnload(server, segment.getDataSource(), broadcastStatusByDatasource, params)) {
        // do nothing
      } else if (usedSegments.contains(segment)) {
        // do nothing
      } else if (action.isLoad() && server.cancelOperation(action, segment)) {
        cancelledOperations.incrementAndGet();
      }
    });

    return cancelledOperations.get();
  }

  /**
   * Returns true if the given server is a realtime server AND the datasource is
   * NOT a broadcast datasource.
   * <p>
   * Realtime tasks work with unpublished segments and the tasks themselves are
   * responsible for dropping those segments. However, segments belonging to a
   * broadcast datasource should still be dropped by the Coordinator as realtime
   * tasks do not ingest data to a broadcast datasource and are thus not
   * responsible for the load/unload of those segments.
   */
  private boolean shouldSkipUnload(
      ServerHolder server,
      String dataSource,
      Map<String, Boolean> broadcastStatusByDatasource,
      DruidCoordinatorRuntimeParams params
  )
  {
    boolean isBroadcastDatasource = broadcastStatusByDatasource
        .computeIfAbsent(dataSource, ds -> isBroadcastDatasource(ds, params));
    return server.isRealtimeServer() && !isBroadcastDatasource;
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
