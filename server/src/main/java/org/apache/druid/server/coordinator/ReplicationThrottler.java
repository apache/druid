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

import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.SegmentId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The ReplicationThrottler is used to throttle the number of replicants that are created.
 */
public class ReplicationThrottler
{
  private static final EmittingLogger log = new EmittingLogger(ReplicationThrottler.class);

  /**
   * Tiers that are already replicating segments are not allowed to queue new items.
   */
  private final Set<String> busyTiers = new HashSet<>();

  // This needs to be thread-safe as it is called by callbacks via unregister
  private final ConcurrentHashMap<String, TierLoadingState> currentlyReplicating = new ConcurrentHashMap<>();

  // These fields need not be thread-safe as they are only accessed by the
  // coordinator duties and not callbacks.
  private int replicationThrottleLimit;
  private int maxLifetime;
  private int maxTotalReplicasPerRun;
  private int numReplicasAssignedInRun;

  /**
   * Updates the replication state for all the tiers for a new coordinator run.
   * This involves:
   * <ul>
   *   <li>Updating the replication throttling parameters with the given values.</li>
   *   <li>Emitting alerts for tiers that have replicas stuck in the load queue.</li>
   *   <li>Identifying the tiers that already have some active replication in
   *   progress. These tiers will not be considered eligible for replication
   *   in this run.</li>
   * </ul>
   *
   * @param replicationThrottleLimit Maximum number of replicas that can be
   *                                 actively loading on a tier at any given time.
   * @param maxLifetime              Number of coordinator runs after which a
   *                                 replica remaining in the queue is considered
   *                                 to be stuck and triggers an alert.
   * @param maxTotalReplicasPerRun   Maximum number of replicas that can be
   *                                 assigned for loading in a single coordinator run.
   */
  public void updateReplicationState(int replicationThrottleLimit, int maxLifetime, int maxTotalReplicasPerRun)
  {
    this.replicationThrottleLimit = replicationThrottleLimit;
    this.maxLifetime = maxLifetime;
    this.maxTotalReplicasPerRun = maxTotalReplicasPerRun;
    this.numReplicasAssignedInRun = 0;

    // Identify the busy and active tiers
    busyTiers.clear();

    final Set<String> inactiveTiers = new HashSet<>();
    currentlyReplicating.forEach((tier, holder) -> {
      if (holder.getNumProcessingSegments() == 0) {
        inactiveTiers.add(tier);
      } else {
        busyTiers.add(tier);
        updateReplicationState(tier, holder);
      }
    });

    // Reset state for inactive tiers
    inactiveTiers.forEach(currentlyReplicating::remove);
  }

  /**
   * Reduces the lifetime of segments replicating in the given tier, if any.
   * Triggers an alert if the replication has timed out.
   */
  private void updateReplicationState(String tier, TierLoadingState holder)
  {
    log.info(
        "[%s]: Replicant create queue still has %d segments. Lifetime[%d]. Segments %s",
        tier,
        holder.getNumProcessingSegments(),
        holder.getLifetime(),
        holder.getCurrentlyProcessingSegmentsAndHosts()
    );
    holder.reduceLifetime();

    if (holder.getLifetime() < 0) {
      log.makeAlert("[%s]: Replicant create queue stuck after %d+ runs!", tier, maxLifetime)
         .addData("segments", holder.getCurrentlyProcessingSegmentsAndHosts())
         .emit();
    }
  }

  public boolean canCreateReplicant(String tier)
  {
    if (numReplicasAssignedInRun >= maxTotalReplicasPerRun
        || busyTiers.contains(tier)) {
      return false;
    }

    TierLoadingState holder = currentlyReplicating.get(tier);
    return holder == null || holder.getNumProcessingSegments() < replicationThrottleLimit;
  }

  public void registerReplicantCreation(String tier, SegmentId segmentId, String serverId)
  {
    ++numReplicasAssignedInRun;
    currentlyReplicating.computeIfAbsent(tier, t -> new TierLoadingState(maxLifetime))
                        .addSegment(segmentId, serverId);
  }

  public void unregisterReplicantCreation(String tier, SegmentId segmentId)
  {
    TierLoadingState tierReplication = currentlyReplicating.get(tier);
    if (tierReplication != null) {
      tierReplication.removeSegment(segmentId);
    }
  }

}
