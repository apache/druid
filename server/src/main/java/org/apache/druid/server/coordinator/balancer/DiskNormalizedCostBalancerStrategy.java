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
 * server as calculated by {@link CostBalancerStrategy} by multiplying it by the
 * server's disk usage ratio.
 * <pre>
 * normalizedCost = cost * usageRatio
 *     where usageRatio = diskUsed / totalDiskSpace
 * </pre>
 * This penalizes servers that are more full, driving disk utilization to equalize
 * across the tier. When all servers have equal disk usage, the behavior is identical
 * to {@link CostBalancerStrategy}. When historicals have different disk capacities,
 * this naturally accounts for both fill level and total capacity.
 * <p>
 * To prevent oscillation when servers have similar utilization, any server that
 * is already projected to hold the segment (the source on a move, or a currently
 * serving node on a drop) receives a cost discount equal to
 * {@link #DEFAULT_MOVE_COST_SAVINGS_THRESHOLD}. A move therefore fires only when
 * the destination saves at least this fraction of the source's cost. The default
 * is configurable via
 * {@code druid.coordinator.balancer.diskNormalized.moveCostSavingsThreshold}.
 */
public class DiskNormalizedCostBalancerStrategy extends CostBalancerStrategy
{
  /**
   * Default minimum fractional cost reduction required before a segment will
   * be moved off a server that is already projected to hold it. A value of
   * {@code 0.05} means the destination must be at least 5% cheaper than the
   * source for the move to happen.
   */
  static final double DEFAULT_MOVE_COST_SAVINGS_THRESHOLD = 0.05;

  private final double sourceCostMultiplier;

  public DiskNormalizedCostBalancerStrategy(ListeningExecutorService exec)
  {
    this(exec, DEFAULT_MOVE_COST_SAVINGS_THRESHOLD);
  }

  public DiskNormalizedCostBalancerStrategy(ListeningExecutorService exec, double moveCostSavingsThreshold)
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

    // Guard against NaN propagation in the cost comparator if a server
    // somehow reports a non-positive maxSize. Such a server cannot hold
    // anything and will be rejected by canLoadSegment, so returning the
    // raw cost is safe.
    final long maxSize = server.getMaxSize();
    if (maxSize <= 0) {
      return cost;
    }

    double usageRatio = (double) server.getSizeUsed() / maxSize;
    double normalizedCost = cost * usageRatio;

    if (server.isProjectedSegment(proposalSegment)) {
      normalizedCost *= sourceCostMultiplier;
    }

    return normalizedCost;
  }
}
