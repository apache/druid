/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.MinMaxPriorityQueue;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Set;

/**
 */
public class DruidCoordinatorCleanupUnneeded implements DruidCoordinatorHelper
{
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

    // Drop segments that no longer exist in the available segments configuration, if it has been populated. (It might
    // not have been loaded yet since it's filled asynchronously. But it's also filled atomically, so if there are any
    // segments at all, we should have all of them.)
    if (!availableSegments.isEmpty()) {
      for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
        for (ServerHolder serverHolder : serverHolders) {
          ImmutableDruidServer server = serverHolder.getServer();

          for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
            for (DataSegment segment : dataSource.getSegments()) {
              if (!availableSegments.contains(segment)) {
                LoadQueuePeon queuePeon = params.getLoadManagementPeons().get(server.getName());

                if (!queuePeon.getSegmentsToDrop().contains(segment)) {
                  queuePeon.dropSegment(
                      segment, new LoadPeonCallback()
                      {
                        @Override
                        public void execute()
                        {
                        }
                      }
                  );
                  stats.addToTieredStat("unneededCount", server.getTier(), 1);
                }
              }
            }
          }
        }
      }
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }


}
