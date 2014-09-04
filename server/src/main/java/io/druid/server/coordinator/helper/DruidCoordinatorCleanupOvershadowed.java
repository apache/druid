/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.guava.Comparators;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;

import java.util.Map;

public class DruidCoordinatorCleanupOvershadowed implements DruidCoordinatorHelper
{
  private final DruidCoordinator coordinator;

  public DruidCoordinatorCleanupOvershadowed(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    CoordinatorStats stats = new CoordinatorStats();

    // Delete segments that are old
    // Unservice old partitions if we've had enough time to make sure we aren't flapping with old data
    if (params.hasDeletionWaitTimeElapsed()) {
      DruidCluster cluster = params.getDruidCluster();
      Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = Maps.newHashMap();

      for (MinMaxPriorityQueue<ServerHolder> serverHolders : cluster.getSortedServersByTier()) {
        for (ServerHolder serverHolder : serverHolders) {
          ImmutableDruidServer server = serverHolder.getServer();

          for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
            VersionedIntervalTimeline<String, DataSegment> timeline = timelines.get(dataSource.getName());
            if (timeline == null) {
              timeline = new VersionedIntervalTimeline<>(Comparators.comparable());
              timelines.put(dataSource.getName(), timeline);
            }

            for (DataSegment segment : dataSource.getSegments()) {
              timeline.add(
                  segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment)
              );
            }
          }
        }
      }

      for (VersionedIntervalTimeline<String, DataSegment> timeline : timelines.values()) {
        for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
          for (DataSegment dataSegment : holder.getObject().payloads()) {
            coordinator.removeSegment(dataSegment);
            stats.addToGlobalStat("overShadowedCount", 1);
          }
        }
      }
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }
}
