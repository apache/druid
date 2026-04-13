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
 * To prevent oscillation when servers have similar utilization, a segment that is
 * already placed on a server receives a 5% cost discount ({@link #MOVE_THRESHOLD}).
 * This means a move only occurs when the destination is meaningfully cheaper.
 */
public class DiskNormalizedCostBalancerStrategy extends CostBalancerStrategy
{
  /**
   * Cost multiplier applied when a segment is already on the server. This
   * creates a "stickiness" that prevents oscillation: a segment only moves
   * when the destination's disk-normalized cost is at least 5% lower than
   * the current server's.
   */
  private static final double MOVE_THRESHOLD = 0.95;

  public DiskNormalizedCostBalancerStrategy(ListeningExecutorService exec)
  {
    super(exec);
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

    double usageRatio = (double) server.getSizeUsed() / server.getMaxSize();
    double normalizedCost = cost * usageRatio;

    if (server.isProjectedSegment(proposalSegment)) {
      normalizedCost *= MOVE_THRESHOLD;
    }

    return normalizedCost;
  }
}

