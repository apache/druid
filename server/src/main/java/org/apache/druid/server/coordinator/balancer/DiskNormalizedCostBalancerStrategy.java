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
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.SegmentAction;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.function.ToDoubleFunction;

/**
 * A {@link BalancerStrategy} which penalizes the cost of placing a segment on a
 * server when the server's projected disk utilization is higher than the least
 * utilized candidate by more than the configured utilization threshold.
 * <pre>
 * adjustedCost = cost, when utilizationGap <= utilizationThreshold
 * adjustedCost = cost * exp((utilizationGap - utilizationThreshold) / utilizationThreshold), otherwise
 *     where utilizationGap = projectedUsageRatio - minProjectedUsageRatio
 *     and projectedUsageRatio = (diskUsed + segmentSizeIfNotAlreadyProjected) / totalDiskSpace
 * </pre>
 * Servers inside the threshold band use the normal cost strategy, which avoids
 * using small utilization differences as a reason to move segments back and
 * forth. Servers outside the band are penalized exponentially by the amount
 * they exceed it.
 */
public class DiskNormalizedCostBalancerStrategy extends CostBalancerStrategy
{
  private final double utilizationThreshold;

  public DiskNormalizedCostBalancerStrategy(ListeningExecutorService exec)
  {
    this(exec, DiskNormalizedCostBalancerStrategyConfig.DEFAULT_UTILIZATION_THRESHOLD);
  }

  public DiskNormalizedCostBalancerStrategy(ListeningExecutorService exec, double utilizationThreshold)
  {
    super(exec);
    Preconditions.checkArgument(
        utilizationThreshold > 0.0 && utilizationThreshold < 1.0,
        "utilizationThreshold[%s] must be in (0.0, 1.0)",
        utilizationThreshold
    );
    this.utilizationThreshold = utilizationThreshold;
  }

  @Override
  protected ToDoubleFunction<ServerHolder> makePlacementCostFunction(
      final DataSegment proposalSegment,
      final List<ServerHolder> serverHolders,
      final SegmentAction action
  )
  {
    final double minProjectedUsageRatio = serverHolders.stream()
                                                       .filter(server -> isCandidateForUtilizationBaseline(
                                                           proposalSegment,
                                                           server,
                                                           action
                                                       ))
                                                       .mapToDouble(server -> computeProjectedUsageRatio(
                                                           proposalSegment,
                                                           server
                                                       ))
                                                       .filter(Double::isFinite)
                                                       .min()
                                                       .orElse(0.0);

    return server -> {
      final double cost = super.computePlacementCost(proposalSegment, server);

      if (cost == Double.POSITIVE_INFINITY) {
        return cost;
      }

      final double projectedUsageRatio = computeProjectedUsageRatio(proposalSegment, server);
      if (!Double.isFinite(projectedUsageRatio)) {
        return cost;
      }

      final double utilizationGap = projectedUsageRatio - minProjectedUsageRatio;
      if (utilizationGap <= utilizationThreshold) {
        return cost;
      }

      return cost * Math.exp((utilizationGap - utilizationThreshold) / utilizationThreshold);
    };
  }

  private static boolean isCandidateForUtilizationBaseline(
      final DataSegment proposalSegment,
      final ServerHolder server,
      final SegmentAction action
  )
  {
    return action != SegmentAction.LOAD || server.canLoadSegment(proposalSegment);
  }

  private static double computeProjectedUsageRatio(
      final DataSegment proposalSegment,
      final ServerHolder server
  )
  {
    final long maxSize = server.getMaxSize();
    if (maxSize <= 0) {
      return Double.POSITIVE_INFINITY;
    }

    final boolean alreadyProjected = server.isProjectedSegment(proposalSegment);
    final long projectedSizeUsed = server.getSizeUsed() + (alreadyProjected ? 0 : proposalSegment.getSize());
    return (double) projectedSizeUsed / maxSize;
  }
}
