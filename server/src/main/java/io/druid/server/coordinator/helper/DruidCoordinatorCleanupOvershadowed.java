/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;

import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
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

        for (LoadQueuePeon loadQueue : coordinator.getLoadManagementPeons().values()) {
          for (DataSegment dataSegment : loadQueue.getSegmentsToLoad()) {
            timeline = timelines.get(dataSegment.getDataSource());
            if (timeline == null) {
              continue;
            }
            // Temporarily add queued segments to the timeline to see if they still need to be loaded.
            timeline.add(
                dataSegment.getInterval(),
                dataSegment.getVersion(),
                dataSegment.getShardSpec().createChunk(dataSegment)
            );
            for (TimelineObjectHolder<String, DataSegment> holder : timeline.findOvershadowed()) {
              for (DataSegment segmentToRemove : holder.getObject().payloads()) {
                if (segmentToRemove == dataSegment) {
                  coordinator.removeSegment(dataSegment);
                  stats.addToGlobalStat("overShadowedCount", 1);
                }
              }
            }
            // Removing it to make sure that if two segment to load and they overshadow both get loaded.
            timeline.remove(
                dataSegment.getInterval(),
                dataSegment.getVersion(),
                dataSegment.getShardSpec().createChunk(dataSegment)
            );
          }
        }
      }
    }
    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }
}
