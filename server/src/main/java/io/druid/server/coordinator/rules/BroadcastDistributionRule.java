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

package io.druid.server.coordinator.rules;

import com.metamx.emitter.EmittingLogger;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class BroadcastDistributionRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(BroadcastDistributionRule.class);

  @Override
  public CoordinatorStats run(
      DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment
  )
  {
    // Find servers which holds the segments of co-located data source
    final Set<ServerHolder> loadServerHolders = new HashSet<>();
    final Set<ServerHolder> dropServerHolders = new HashSet<>();
    final List<String> colocatedDataSources = getColocatedDataSources();
    if (colocatedDataSources == null || colocatedDataSources.isEmpty()) {
      loadServerHolders.addAll(params.getDruidCluster().getAllServers());
    } else {
      params.getDruidCluster().getAllServers().forEach(
          eachHolder -> {
            if (colocatedDataSources.stream()
                                    .anyMatch(source -> eachHolder.getServer().getDataSource(source) != null)) {
              loadServerHolders.add(eachHolder);
            } else if (eachHolder.isServingSegment(segment)) {
              if (!eachHolder.getPeon().getSegmentsToDrop().contains(segment)) {
                dropServerHolders.add(eachHolder);
              }
            }
          }
      );
    }

    final CoordinatorStats stats = new CoordinatorStats();

    return stats.accumulate(assign(loadServerHolders, segment))
                .accumulate(drop(dropServerHolders, segment));
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
           .addData("segmentId", segment.getIdentifier())
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

  public abstract List<String> getColocatedDataSources();
}
