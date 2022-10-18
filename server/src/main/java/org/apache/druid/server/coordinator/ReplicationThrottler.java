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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * The ReplicationThrottler is used to throttle the number of segment replicas
 * that are assigned to a load queue in a single run.
 * This is achieved by setting the values of these configs:
 * <ul>
 *   <li>{@link CoordinatorDynamicConfig#getReplicationThrottleLimit()}, and</li>
 *   <li>{@link CoordinatorDynamicConfig#getMaxNonPrimaryReplicantsToLoad()}</li>
 * </ul>
 */
public class ReplicationThrottler
{
  private final Set<String> eligibleTiers;
  private final int replicationThrottleLimit;
  private final int maxLifetime;
  private final int maxTotalReplicasPerRun;

  private final Map<String, Integer> tierToNumAssigned = new HashMap<>();
  private final Map<String, Integer> tierToNumThrottled = new HashMap<>();

  private int totalNumAssignedReplicas;

  /**
   * Creates a new ReplicationThrottler for use during a single coordiantor run.
   *
   * @param eligibleTiers            Set of tiers eligible for replication.
   * @param replicationThrottleLimit Maximum number of replicas that can be
   *                                 actively loading on a tier at any given time.
   * @param maxLifetime              Number of coordinator runs after which a
   *                                 replica remaining in the queue is considered
   *                                 to be stuck and triggers an alert.
   * @param maxTotalReplicasPerRun   Maximum number of replicas that can be
   *                                 assigned for loading in a single coordinator run.
   */
  public ReplicationThrottler(
      Set<String> eligibleTiers,
      int replicationThrottleLimit,
      int maxLifetime,
      int maxTotalReplicasPerRun
  )
  {
    this.eligibleTiers = eligibleTiers;
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.maxLifetime = maxLifetime;
    this.maxTotalReplicasPerRun = maxTotalReplicasPerRun;
    this.totalNumAssignedReplicas = 0;
  }

  public boolean canAssignReplica(String tier, int numProcessingSegmentsInTier)
  {
    return totalNumAssignedReplicas < maxTotalReplicasPerRun
           && numProcessingSegmentsInTier < replicationThrottleLimit
           && eligibleTiers.contains(tier);
  }

  public void incrementAssignedReplicas(String tier)
  {
    ++totalNumAssignedReplicas;
    tierToNumAssigned.compute(tier, (t, count) -> (count == null) ? 1 : count + 1);
  }

  public void incrementThrottledReplicas(String tier)
  {
    tierToNumThrottled.compute(tier, (t, count) -> (count == null) ? 1 : count + 1);
  }

  public Map<String, Integer> getTierToNumThrottled()
  {
    return tierToNumThrottled;
  }

  public int getMaxLifetime()
  {
    return maxLifetime;
  }

}
