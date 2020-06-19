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

package org.apache.druid.server.coordinator.rules;

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class BroadcastDistributionRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(BroadcastDistributionRule.class);

  @Override
  public CoordinatorStats run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment)
  {
    final Set<ServerHolder> dropServerHolders = new HashSet<>();

    // Find servers where we need to load the broadcast segments
    final Set<ServerHolder> loadServerHolders =
        params.getDruidCluster().getAllServers()
              .stream()
              .filter(
                  (serverHolder) -> {
                    ServerType serverType = serverHolder.getServer().getType();
                    if (!serverType.isSegmentBroadcastTarget()) {
                      return false;
                    }

                    final boolean isServingSegment =
                        serverHolder.isServingSegment(segment);

                    if (serverHolder.isDecommissioning()) {
                      if (isServingSegment && !serverHolder.isDroppingSegment(segment)) {
                        dropServerHolders.add(serverHolder);
                      }
                      return false;
                    }

                    return !isServingSegment && !serverHolder.isLoadingSegment(segment);
                  }
              )
              .collect(Collectors.toSet());

    final CoordinatorStats stats = new CoordinatorStats();
    return stats.accumulate(assign(loadServerHolders, segment))
                .accumulate(drop(dropServerHolders, segment));
  }

  @Override
  public boolean canLoadSegments()
  {
    return true;
  }

  @Override
  public void updateUnderReplicated(
      Map<String, Object2LongMap<String>> underReplicatedPerTier,
      SegmentReplicantLookup segmentReplicantLookup,
      DataSegment segment
  )
  {
    Object2LongMap<String> underReplicatedBroadcastTiers = segmentReplicantLookup.getBroadcastUnderReplication(segment.getId());
    for (final Object2LongMap.Entry<String> entry : underReplicatedBroadcastTiers.object2LongEntrySet()) {
      final String tier = entry.getKey();
      final long underReplicatedCount = entry.getLongValue();
      underReplicatedPerTier.compute(tier, (_tier, existing) -> {
        Object2LongMap<String> underReplicationPerDataSource = existing;
        if (existing == null) {
          underReplicationPerDataSource = new Object2LongOpenHashMap<>();
        }
        underReplicationPerDataSource.compute(
            segment.getDataSource(),
            (_datasource, count) -> count != null ? count + underReplicatedCount : underReplicatedCount
        );
        return underReplicationPerDataSource;
      });
    }
  }

  private CoordinatorStats assign(
      final Set<ServerHolder> serverHolders,
      final DataSegment segment
  )
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(LoadRule.ASSIGNED_COUNT, 0);

    for (ServerHolder holder : serverHolders) {
      if (segment.getSize() > holder.getAvailableSize()) {
        log.makeAlert("Failed to broadcast segment for [%s]", segment.getDataSource())
           .addData("segmentId", segment.getId())
           .addData("segmentSize", segment.getSize())
           .addData("hostName", holder.getServer().getHost())
           .addData("availableSize", holder.getAvailableSize())
           .emit();
      } else {
        if (!holder.isLoadingSegment(segment)) {
          holder.getPeon().loadSegment(
              segment,
              null
          );

          stats.addToGlobalStat(LoadRule.ASSIGNED_COUNT, 1);
        }
      }
    }

    return stats;
  }

  private CoordinatorStats drop(
      final Set<ServerHolder> serverHolders,
      final DataSegment segment
  )
  {
    CoordinatorStats stats = new CoordinatorStats();

    for (ServerHolder holder : serverHolders) {
      holder.getPeon().dropSegment(segment, null);
      stats.addToGlobalStat(LoadRule.DROPPED_COUNT, 1);
    }

    return stats;
  }
}
