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

package org.apache.druid.server.coordinator;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.server.coordinator.balancer.ClusterCostComputer;
import org.apache.druid.server.coordinator.balancer.CostBalancerStrategy;
import org.apache.druid.timeline.DataSegment;

/**
 * This cost strategy relies on dividing segments into time buckets
 * The intervals within a bucket are sorted to allow for faster cost computation
 * This strategy is based on ideas utilized in CachingCostBalancerStrategy
 * However, CachingCostBalancerStrategy has 2 major issues which this strategy tries to fix:
 * 1) The value / decisions may differ when segments of multiple granularities are present
 * 2) A cache is slow to build : O(N ^ 2)
 * This strategy tries to fix it while also being as just as fast in computing the cost
 */
public class SortingCostBalancerStrategy extends CostBalancerStrategy
{
  private final ClusterCostComputer costComputer;

  public SortingCostBalancerStrategy(ServerInventoryView serverInventoryView, ListeningExecutorService exec)
  {
    super(exec);
    costComputer = new ClusterCostComputer(serverInventoryView);
  }

  @Override
  protected double computePlacementCost(DataSegment segment, ServerHolder server)
  {
    final String serverName = server.getServer().getName();
    double cost = costComputer.computeCost(serverName, segment);
    if (server.isProjectedSegment(segment)) {
      cost -= intervalCost(segment.getInterval(), segment.getInterval());
    }
    return cost;
  }
}
