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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final ReplicatorSegmentHolder currentlyReplicating = new ReplicatorSegmentHolder();

  private volatile int maxReplicasPerTier;
  private volatile int maxLifetime;
  private volatile int maxTotalReplicasPerRun;
  private final AtomicInteger numAssignedReplicas = new AtomicInteger();

  /**
   * Resets the replication throttling parameters for a new coordinator run.
   *
   * @param replicationThrottleLimit Maximum number of replicas that can be
   *                                 actively loading on a tier at any given time.
   * @param maxLifetime              Number of coordinator runs after which
   *                                 replica remaining in the queue is considered
   *                                 to be stuck and causes an alert.
   * @param maxTotalReplicasPerRun   Maximum number of replicas that can be
   *                                 assigned for loading in a single coordinator run.
   */
  public void resetParams(int replicationThrottleLimit, int maxLifetime, int maxTotalReplicasPerRun)
  {
    this.maxReplicasPerTier = replicationThrottleLimit;
    this.maxLifetime = maxLifetime;
    this.maxTotalReplicasPerRun = maxTotalReplicasPerRun;
    this.numAssignedReplicas.set(0);
  }

  /**
   * Updates the replication state for all the tiers for a new coordinator run.
   * This involves:
   * <ul>
   *   <li>Sending alerts for tiers that have a replica stuck in the load queue.</li>
   *   <li>Identifying the tiers that already have some active replication in
   *   progress. These tiers will not be considered eligible for replication
   *   in this run.</li>
   * </ul>
   */
  public void updateReplicationState()
  {
    busyTiers.clear();
    currentlyReplicating.currentlyProcessingSegments
        .keySet().forEach(this::updateReplicationState);
  }

  private void updateReplicationState(String tier)
  {
    final ReplicatorSegmentHolder holder = currentlyReplicating;
    int size = holder.getNumProcessing(tier);
    if (size != 0) {
      log.info(
          "[%s]: Replicant create queue still has %d segments. Lifetime[%d]. Segments %s",
          tier,
          size,
          holder.getLifetime(tier),
          holder.getCurrentlyProcessingSegmentsAndHosts(tier)
      );
      holder.reduceLifetime(tier);

      if (holder.getLifetime(tier) < 0) {
        log.makeAlert("[%s]: Replicant create queue stuck after %d+ runs!", tier, maxLifetime)
           .addData("segments", holder.getCurrentlyProcessingSegmentsAndHosts(tier))
           .emit();
      }
      busyTiers.add(tier);
    } else {
      log.info("[%s]: Replicant create queue is empty.", tier);
      holder.resetLifetime(tier);
    }
  }

  public boolean canCreateReplicant(String tier)
  {
    return numAssignedReplicas.get() < maxTotalReplicasPerRun
           && !busyTiers.contains(tier)
           && !currentlyReplicating.isAtMaxReplicants(tier);
  }

  public void registerReplicantCreation(String tier, SegmentId segmentId, String serverId)
  {
    numAssignedReplicas.incrementAndGet();
    currentlyReplicating.addSegment(tier, segmentId, serverId);
  }

  public void unregisterReplicantCreation(String tier, SegmentId segmentId)
  {
    currentlyReplicating.removeSegment(tier, segmentId);
  }

  private class ReplicatorSegmentHolder
  {
    private final Map<String, ConcurrentHashMap<SegmentId, String>> currentlyProcessingSegments = new HashMap<>();
    private final Map<String, Integer> lifetimes = new HashMap<>();

    boolean isAtMaxReplicants(String tier)
    {
      final ConcurrentHashMap<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      return (segments != null && segments.size() >= maxReplicasPerTier);
    }

    void addSegment(String tier, SegmentId segmentId, String serverId)
    {
      ConcurrentHashMap<SegmentId, String> segments =
          currentlyProcessingSegments.computeIfAbsent(tier, t -> new ConcurrentHashMap<>());

      if (!isAtMaxReplicants(tier)) {
        segments.put(segmentId, serverId);
      }
    }

    void removeSegment(String tier, SegmentId segmentId)
    {
      ConcurrentMap<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      if (segments != null) {
        segments.remove(segmentId);
      }
    }

    int getNumProcessing(String tier)
    {
      ConcurrentMap<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      return (segments == null) ? 0 : segments.size();
    }

    int getLifetime(String tier)
    {
      Integer lifetime = lifetimes.putIfAbsent(tier, maxLifetime);
      return lifetime != null ? lifetime : maxLifetime;
    }

    void reduceLifetime(String tier)
    {
      lifetimes.compute(
          tier,
          (t, lifetime) -> {
            if (lifetime == null) {
              return maxLifetime - 1;
            }
            return lifetime - 1;
          }
      );
    }

    void resetLifetime(String tier)
    {
      lifetimes.put(tier, maxLifetime);
    }

    List<String> getCurrentlyProcessingSegmentsAndHosts(String tier)
    {
      ConcurrentMap<SegmentId, String> segments = currentlyProcessingSegments.get(tier);
      List<String> segmentsAndHosts = new ArrayList<>();
      segments.forEach((segmentId, serverId) -> segmentsAndHosts.add(segmentId + " ON " + serverId));
      return segmentsAndHosts;
    }
  }
}
