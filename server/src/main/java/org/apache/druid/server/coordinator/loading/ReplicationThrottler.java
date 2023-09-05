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

package org.apache.druid.server.coordinator.loading;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import java.util.Map;

/**
 * The ReplicationThrottler is used to throttle the number of segment replicas
 * that are assigned to a load queue in a single run. This is achieved by setting
 * the values of the following configs:
 * <ul>
 *   <li>{@link CoordinatorDynamicConfig#getReplicationThrottleLimit()} - Maximum
 *   number of replicas that can be assigned to a tier in a single run.</li>
 * </ul>
 */
public class ReplicationThrottler
{
  private final int replicationThrottleLimit;

  private final Object2IntOpenHashMap<String> tierToNumAssigned = new Object2IntOpenHashMap<>();
  private final Object2IntOpenHashMap<String> tierToMaxAssignments = new Object2IntOpenHashMap<>();

  /**
   * Creates a new ReplicationThrottler for use during a single coordinator run.
   * The number of replicas loading on a tier must always be within the current
   * {@code replicationThrottleLimit}. Thus, if a tier was already loading {@code k}
   * replicas at the start of a coordinator run, it may be assigned only
   * {@code replicationThrottleLimit - k} more replicas during the run.
   *
   * @param tierToLoadingReplicaCount Map from tier name to number of replicas
   *                                  already being loaded.
   * @param replicationThrottleLimit  Maximum number of replicas that can be
   *                                  assigned to a single tier in the current run.
   */
  public ReplicationThrottler(
      Map<String, Integer> tierToLoadingReplicaCount,
      int replicationThrottleLimit
  )
  {
    this.replicationThrottleLimit = replicationThrottleLimit;

    if (tierToLoadingReplicaCount != null) {
      tierToLoadingReplicaCount.forEach(
          (tier, numLoadingReplicas) -> tierToMaxAssignments.addTo(
              tier,
              Math.max(0, replicationThrottleLimit - numLoadingReplicas)
          )
      );
    }
  }

  public boolean isReplicationThrottledForTier(String tier)
  {
    return tierToNumAssigned.getOrDefault(tier, 0)
           >= tierToMaxAssignments.getOrDefault(tier, replicationThrottleLimit);
  }

  public void incrementAssignedReplicas(String tier)
  {
    tierToNumAssigned.addTo(tier, 1);
  }

}
