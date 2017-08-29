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

package io.druid.server.coordinator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.server.coordinator.cost.ClusterCostCache;
import io.druid.timeline.DataSegment;

import java.util.Set;


public class CachingCostBalancerStrategy extends CostBalancerStrategy
{

  private final ClusterCostCache clusterCostCache;

  public CachingCostBalancerStrategy(ClusterCostCache clusterCostCache, ListeningExecutorService exec)
  {
    super(exec);
    this.clusterCostCache = Preconditions.checkNotNull(clusterCostCache);
  }

  @Override
  protected double computeCost(
      DataSegment proposalSegment, ServerHolder server, boolean includeCurrentServer
  )
  {
    final long proposalSegmentSize = proposalSegment.getSize();

    // (optional) Don't include server if it is already serving segment
    if (!includeCurrentServer && server.isServingSegment(proposalSegment)) {
      return Double.POSITIVE_INFINITY;
    }

    // Don't calculate cost if the server doesn't have enough space or is loading the segment
    if (proposalSegmentSize > server.getAvailableSize() || server.isLoadingSegment(proposalSegment)) {
      return Double.POSITIVE_INFINITY;
    }

    final String serverName = server.getServer().getName();

    double cost = clusterCostCache.computeCost(serverName, proposalSegment);

    // add segments that will be loaded to the cost
    cost += costCacheForLoadingSegments(server).computeCost(serverName, proposalSegment);

    return cost;
  }

  private ClusterCostCache costCacheForLoadingSegments(ServerHolder server)
  {
    final Set<DataSegment> loadingSegments = server.getPeon().getSegmentsToLoad();
    return ClusterCostCache.builder(ImmutableMap.of(server.getServer().getName(), loadingSegments)).build();
  }

}
