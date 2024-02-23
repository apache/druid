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

package org.apache.druid.server.coordinator.balancer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.Collections;
import java.util.Set;

/**
 * @deprecated This is currently being used only in tests for benchmarking purposes
 * and will be removed in future releases.
 */
@Deprecated
public class CachingCostBalancerStrategy extends CostBalancerStrategy
{
  private final ClusterCostCache clusterCostCache;

  public CachingCostBalancerStrategy(ClusterCostCache clusterCostCache, ListeningExecutorService exec)
  {
    super(exec);
    this.clusterCostCache = Preconditions.checkNotNull(clusterCostCache);
  }

  @Override
  protected double computePlacementCost(DataSegment proposalSegment, ServerHolder server)
  {
    final String serverName = server.getServer().getName();

    double cost = clusterCostCache.computeCost(serverName, proposalSegment);

    // add segments that will be loaded to the cost
    cost += costCacheForLoadingSegments(server).computeCost(serverName, proposalSegment);

    // minus the cost of the segment itself
    if (server.isServingSegment(proposalSegment) || server.isLoadingSegment(proposalSegment)) {
      cost -= costCacheForSegments(server, Collections.singleton(proposalSegment))
          .computeCost(serverName, proposalSegment);
    }

    // minus the costs of segments that are being dropped
    cost -= costCacheForSegments(server, server.getPeon().getSegmentsToDrop())
        .computeCost(serverName, proposalSegment);

    // minus the costs of segments that are marked to be dropped
    cost -= costCacheForSegments(server, server.getPeon().getSegmentsMarkedToDrop())
        .computeCost(serverName, proposalSegment);

    if (server.getAvailableSize() <= 0) {
      return Double.POSITIVE_INFINITY;
    }

    // This is probably intentional. The tests fail if this is changed to floating point division.
    //noinspection IntegerDivisionInFloatingPointContext
    return cost * (server.getMaxSize() / server.getAvailableSize());
  }

  private ClusterCostCache costCacheForLoadingSegments(ServerHolder server)
  {
    return costCacheForSegments(server, server.getPeon().getSegmentsToLoad());
  }

  private ClusterCostCache costCacheForSegments(ServerHolder server, Set<DataSegment> segments)
  {
    return ClusterCostCache.builder(ImmutableMap.of(server.getServer().getName(), segments)).build();
  }

}
