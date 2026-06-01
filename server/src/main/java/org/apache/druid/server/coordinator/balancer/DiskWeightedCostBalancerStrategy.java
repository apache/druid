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
import org.apache.druid.timeline.DataSegment;

/**
 * A {@link BalancerStrategy} which normalizes the cost of placing a segment on a
 * server as calculated by {@link CostBalancerStrategy} by dividing by the
 * server's projected available disk headroom.
 * <pre>
 * normalizedCost = cost / max(EPSILON, 1 - projectedUsageRatio)
 *     where projectedUsageRatio = (diskUsed + segmentSizeIfNotAlreadyProjected) / totalDiskSpace
 * </pre>
 * The denominator diverges as a server approaches full, so disk fullness has
 * more weight over the placement decision when servers are nearly full,
 * regardless of asymmetries in the locality cost. {@link #EPSILON} is a small
 * numerical floor on the divisor to guard against division by zero (or by
 * negative values during in-flight loads).
 * <p>
 * To prevent oscillation when servers have similar headroom, any server that
 * is already projected to hold the segment (the source on a move, or a currently
 * serving node on a drop) receives a cost discount equal to
 * {@link DiskWeightedCostBalancerStrategyConfig.DEFAULT_MOVE_COST_SAVINGS_THRESHOLD}. A move therefore fires only when
 * the destination saves at least this fraction of the source's cost. The default
 * is configurable via
 * {@code druid.coordinator.balancer.diskWeighted.moveCostSavingsThreshold}.
 */
public class DiskWeightedCostBalancerStrategy extends CostBalancerStrategy
{
  /**
   * Numerical floor on the headroom divisor to prevent division by zero or by
   * negative values when {@code usageRatio >= 1.0} (possible for over-allocated
   * servers or during in-flight loads).
   */
  static final double EPSILON = 1e-6;

  private final double sourceCostMultiplier;

  public DiskWeightedCostBalancerStrategy(ListeningExecutorService exec)
  {
    this(exec, DiskWeightedCostBalancerStrategyConfig.DEFAULT_MOVE_COST_SAVINGS_THRESHOLD);
  }

  public DiskWeightedCostBalancerStrategy(ListeningExecutorService exec, double moveCostSavingsThreshold)
  {
    super(exec);
    Preconditions.checkArgument(
        moveCostSavingsThreshold >= 0.0 && moveCostSavingsThreshold < 1.0,
        "moveCostSavingsThreshold[%s] must be in [0.0, 1.0)",
        moveCostSavingsThreshold
    );
    this.sourceCostMultiplier = 1.0 - moveCostSavingsThreshold;
  }

  @Override
  protected double computePlacementCost(
      final DataSegment proposalSegment,
      final ServerHolder server
  )
  {
    double cost = super.computePlacementCost(proposalSegment, server);

    if (cost == Double.POSITIVE_INFINITY) {
      return cost;
    }

    // A server with non-positive maxSize cannot hold anything and will be
    // rejected by canLoadSegment; return the raw cost to avoid NaN propagation.
    final long maxSize = server.getMaxSize();
    if (maxSize <= 0) {
      return cost;
    }

    final boolean alreadyProjected = server.isProjectedSegment(proposalSegment);
    final long projectedSizeUsed = server.getSizeUsed() + (alreadyProjected ? 0 : proposalSegment.getSize());
    final double usageRatio = (double) projectedSizeUsed / maxSize;
    final double headroom = Math.max(EPSILON, 1.0 - usageRatio);
    double normalizedCost = cost / headroom;

    if (alreadyProjected) {
      normalizedCost *= sourceCostMultiplier;
    }

    return normalizedCost;
  }
}
