/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
