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
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

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
      Map<String, VersionedIntervalTimeline<String, DataSegment>> timelines = new HashMap<>();

      for (SortedSet<ServerHolder> serverHolders : cluster.getSortedHistoricalsByTier()) {
        for (ServerHolder serverHolder : serverHolders) {
          ImmutableDruidServer server = serverHolder.getServer();

          for (ImmutableDruidDataSource dataSource : server.getDataSources()) {
            VersionedIntervalTimeline<String, DataSegment> timeline = timelines.get(dataSource.getName());
            if (timeline == null) {
              timeline = new VersionedIntervalTimeline<>(Comparator.naturalOrder());
              timelines.put(dataSource.getName(), timeline);
            }

            VersionedIntervalTimeline.addSegments(timeline, dataSource.getSegments().iterator());
          }
        }
      }

      //Remove all segments in db that are overshadowed by served segments
      for (DataSegment dataSegment : params.getAvailableSegments()) {
        VersionedIntervalTimeline<String, DataSegment> timeline = timelines.get(dataSegment.getDataSource());
        if (timeline != null && timeline.isOvershadowed(dataSegment.getInterval(), dataSegment.getVersion())) {
          coordinator.removeSegment(dataSegment);
          stats.addToGlobalStat("overShadowedCount", 1);
        }
      }
    }
    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }
}
