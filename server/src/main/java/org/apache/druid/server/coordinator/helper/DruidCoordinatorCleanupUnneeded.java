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

import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.Set;
import java.util.SortedSet;

/**
 */
public class DruidCoordinatorCleanupUnneeded implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorCleanupUnneeded.class);

  private final DruidCoordinator coordinator;

  public DruidCoordinatorCleanupUnneeded(
      DruidCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    CoordinatorStats stats = new CoordinatorStats();
    Set<DataSegment> availableSegments = params.getAvailableSegments();
    DruidCluster cluster = params.getDruidCluster();

    // Drop segments that no longer exist in the available segments configuration, *if* it has been populated. (It might
    // not have been loaded yet since it's filled asynchronously. But it's also filled atomically, so if there are any
    // segments at all, we should have all of them.)
    // Note that if metadata store has no segments, then availableSegments will stay empty and nothing will be dropped.
    // This is done to prevent a race condition in which the coordinator would drop all segments if it started running
    // cleanup before it finished polling the metadata storage for available segments for the first time.
    if (!availableSegments.isEmpty()) {
      for (SortedSet<ServerHolder> serverHolders : cluster.getSortedHistoricalsByTier()) {
        for (ServerHolder serverHolder : serverHolders) {
          ImmutableDruidServer server = serverHolder.getServer();

          for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
            for (DataSegment segment : dataSource.getSegments()) {
              if (!availableSegments.contains(segment)) {
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
    } else {
      log.info(
          "Found 0 availableSegments, skipping the cleanup of segments from historicals. This is done to prevent a race condition in which the coordinator would drop all segments if it started running cleanup before it finished polling the metadata storage for available segments for the first time."
      );
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }


}
