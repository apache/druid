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
 * A {@link BalancerStrategy} which can be used when historicals in a tier have
 * varying disk capacities. This strategy normalizes the cost of placing a segment on
 * a server as calculated by {@link CostBalancerStrategy} by doing the following:
 * <ul>
 * <li>Divide the cost by the number of segments on the server. This ensures that
 * cost does not increase just because the number of segments on a server is higher.</li>
 * <li>Multiply the resulting value by disk usage ratio. This ensures that all
 * hosts have equivalent levels of percentage disk utilization.</li>
 * </ul>
 * i.e. to place a segment on a given server
 * <pre>
 * cost = as computed by CostBalancerStrategy
 * normalizedCost = (cost / numSegments) * usageRatio
 *                = (cost / numSegments) * (diskUsed / totalDiskSpace)
 * </pre>
 */
public class DiskNormalizedCostBalancerStrategy extends CostBalancerStrategy
{
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

    int nSegments = 1;
    if (server.getServer().getNumSegments() > 0) {
      nSegments = server.getServer().getNumSegments();
    }

    double normalizedCost = cost / nSegments;
    double usageRatio = (double) server.getSizeUsed() / (double) server.getServer().getMaxSize();

    return normalizedCost * usageRatio;
  }
}

