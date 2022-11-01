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

import com.google.common.collect.Sets;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Used by the coordinator in each run for segment loading, dropping, balancing
 * and broadcasting.
 * <p>
 * An instance of this class is freshly created for each coordinator run.
 */
public class SegmentLoader
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoader.class);

  private final SegmentStateManager stateManager;
  private final DruidCluster cluster;
  private final CoordinatorStats stats = new CoordinatorStats();
  private final SegmentReplicantLookup replicantLookup;
  private final ReplicationThrottler replicationThrottler;
  private final BalancerStrategy strategy;

  private final Set<String> emptyTiers = new HashSet<>();

  public SegmentLoader(
      SegmentStateManager stateManager,
      DruidCluster cluster,
      SegmentReplicantLookup replicantLookup,
      ReplicationThrottler replicationThrottler,
      BalancerStrategy strategy
  )
  {
    this.cluster = cluster;
    this.strategy = strategy;
    this.stateManager = stateManager;
    this.replicantLookup = replicantLookup;
    this.replicationThrottler = replicationThrottler;
  }

  public CoordinatorStats getStats()
  {
    return stats;
  }

  public void makeAlerts()
  {
    if (!emptyTiers.isEmpty()) {
      log.makeAlert("Tiers %s have no servers! Check your cluster configuration.", emptyTiers).emit();
    }
  }

  /**
   * Moves the given segment from serverA to serverB in the same tier.
   */
  public boolean moveSegment(DataSegment segment, ServerHolder serverA, ServerHolder serverB)
  {
    final String tier = serverA.getServer().getTier();
    if (!serverB.getServer().getTier().equals(tier)
        || !serverB.canLoadSegment(segment)) {
      return false;
    }

    if (serverA.isLoadingSegment(segment)) {
      // Cancel the load on serverA and load on serverB instead
      if (serverA.cancelOperation(SegmentAction.LOAD, segment)) {
        stats.addToTieredStat(CoordinatorStats.CANCELLED_LOADS, tier, 1);
        int loadedCountOnTier = replicantLookup.getServedReplicas(segment.getId(), tier);
        return stateManager.loadSegment(segment, serverB, loadedCountOnTier < 1, replicationThrottler);
      }

      // Cancel failed, let the segment load on serverA and fail the move operation
      return false;
    } else if (serverA.isServingSegment(segment)) {
      return stateManager.moveSegment(segment, serverA, serverB, replicationThrottler.getMaxLifetime());
    } else {
      return false;
    }
  }

  /**
   * Queues load or drop of replicas of the given segment to achieve the
   * target replication level on all the tiers.
   */
  public void updateReplicas(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    // Identify empty tiers and determine total required replicas
    final AtomicInteger requiredTotalReplicas = new AtomicInteger(0);
    final Set<String> allTiers = Sets.newHashSet(cluster.getTierNames());
    tierToReplicaCount.forEach((tier, requiredReplicas) -> {
      reportTierCapacityStats(segment, requiredReplicas, tier);
      if (allTiers.contains(tier)) {
        requiredTotalReplicas.addAndGet(requiredReplicas);
      } else {
        emptyTiers.add(tier);
      }
    });

    final int totalOverReplication =
        replicantLookup.getTotalServedReplicas(segment.getId()) - requiredTotalReplicas.get();

    // Update replicas in every tier
    int totalDropsQueued = 0;
    for (String tier : allTiers) {
      totalDropsQueued += updateReplicasInTier(
          segment,
          tier,
          tierToReplicaCount.getOrDefault(tier, 0),
          totalOverReplication - totalDropsQueued
      );
    }
  }

  /**
   * Queues load or drop operations on this tier based on the required
   * number of replicas and the current state.
   * <p>
   * The {@code maxReplicasToDrop} helps to maintain the required level of
   * replication in the cluster. This ensures that segment read concurrency does
   * not suffer during a tier shift or load rule change.
   * <p>
   * Returns the number of new drop operations queued on this tier.
   */
  private int updateReplicasInTier(
      DataSegment segment,
      String tier,
      int requiredReplicas,
      int maxReplicasToDrop
  )
  {
    final int projectedReplicas = replicantLookup.getProjectedReplicas(segment.getId(), tier);
    final int movingReplicas = replicantLookup.getMovingReplicas(segment.getId(), tier);
    final boolean shouldCancelMoves = requiredReplicas == 0 && movingReplicas > 0;

    // Check if there is any action required on this tier
    if (projectedReplicas == requiredReplicas && !shouldCancelMoves) {
      return 0;
    }

    SegmentTierStatus segmentStatus = new SegmentTierStatus(segment, cluster.getHistoricalsByTier(tier));

    // Cancel all moves in this tier if it does not need to have replicas
    if (shouldCancelMoves) {
      int cancelledMoves =
          cancelOperations(SegmentAction.MOVE_TO, movingReplicas, segment, segmentStatus);
      stats.addToTieredStat(CoordinatorStats.CANCELLED_MOVES, tier, cancelledMoves);
    }

    // Cancel drops and queue loads if the projected count is below the requirement
    if (projectedReplicas < requiredReplicas) {
      int replicaDeficit = requiredReplicas - projectedReplicas;
      int cancelledDrops =
          cancelOperations(SegmentAction.DROP, replicaDeficit, segment, segmentStatus);
      stats.addToTieredStat(CoordinatorStats.CANCELLED_DROPS, tier, cancelledDrops);

      // Cancelled drops can be counted as loaded replicas, thus reducing deficit
      int numReplicasToLoad = replicaDeficit - cancelledDrops;
      if (numReplicasToLoad > 0) {
        boolean isFirstLoadOnTier = replicantLookup.getServedReplicas(segment.getId(), tier)
                                    + cancelledDrops < 1;
        int numLoadsQueued = loadReplicas(numReplicasToLoad, segment, tier, segmentStatus, isFirstLoadOnTier);
        stats.addToTieredStat(CoordinatorStats.ASSIGNED_COUNT, tier, numLoadsQueued);
        stats.addToDataSourceStat(CoordinatorStats.UNDER_REPLICATED_COUNT, segment.getDataSource(), numReplicasToLoad);
      }
    }

    // Cancel loads and queue drops if the projected count exceeds the requirement
    if (projectedReplicas > requiredReplicas) {
      int replicaSurplus = projectedReplicas - requiredReplicas;
      int cancelledLoads =
          cancelOperations(SegmentAction.LOAD, replicaSurplus, segment, segmentStatus);
      stats.addToTieredStat(CoordinatorStats.CANCELLED_LOADS, tier, cancelledLoads);

      int numReplicasToDrop = Math.min(replicaSurplus - cancelledLoads, maxReplicasToDrop);
      if (numReplicasToDrop > 0) {
        int dropsQueuedOnTier = dropReplicas(numReplicasToDrop, segment, tier, segmentStatus);
        stats.addToTieredStat(CoordinatorStats.DROPPED_COUNT, tier, dropsQueuedOnTier);
        return dropsQueuedOnTier;
      }
    }

    return 0;
  }

  private void reportTierCapacityStats(DataSegment segment, int requiredReplicas, String tier)
  {
    stats.accumulateMaxTieredStat(
        CoordinatorStats.MAX_REPLICATION_FACTOR,
        tier,
        requiredReplicas
    );
    stats.addToTieredStat(
        CoordinatorStats.REQUIRED_CAPACITY,
        tier,
        segment.getSize() * requiredReplicas
    );
  }

  /**
   * Broadcasts the given segment to all servers that are broadcast targets and
   * queues a drop of the segment from decommissioning servers.
   */
  public void broadcastSegment(DataSegment segment)
  {
    int assignedCount = 0;
    int droppedCount = 0;
    for (ServerHolder server : cluster.getAllServers()) {
      // Ignore servers which are not broadcast targets
      if (!server.getServer().getType().isSegmentBroadcastTarget()) {
        continue;
      }

      if (server.isDecommissioning()) {
        droppedCount += dropBroadcastSegment(segment, server) ? 1 : 0;
      } else {
        assignedCount += loadBroadcastSegment(segment, server) ? 1 : 0;
      }
    }

    if (assignedCount > 0) {
      stats.addToDataSourceStat(CoordinatorStats.BROADCAST_LOADS, segment.getDataSource(), assignedCount);
    }
    if (droppedCount > 0) {
      stats.addToDataSourceStat(CoordinatorStats.BROADCAST_DROPS, segment.getDataSource(), droppedCount);
    }
  }

  /**
   * Marks the given segment as unused.
   */
  public void deleteSegment(DataSegment segment)
  {
    stateManager.deleteSegment(segment);
    stats.addToGlobalStat(CoordinatorStats.DELETED_COUNT, 1);
  }

  /**
   * Loads the broadcast segment if it is not loaded on the given server.
   * Returns true only if the segment was successfully queued for load on the server.
   */
  private boolean loadBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    if (server.isServingSegment(segment) || server.isLoadingSegment(segment)) {
      return false;
    } else if (server.isDroppingSegment(segment)) {
      return server.cancelOperation(SegmentAction.DROP, segment);
    }

    if (server.canLoadSegment(segment)
        && stateManager.loadSegment(segment, server, true, replicationThrottler)) {
      return true;
    } else {
      log.makeAlert("Failed to assign broadcast segment for datasource [%s]", segment.getDataSource())
         .addData("segmentId", segment.getId())
         .addData("segmentSize", segment.getSize())
         .addData("hostName", server.getServer().getHost())
         .addData("availableSize", server.getAvailableSize())
         .emit();
      return false;
    }
  }

  /**
   * Drops the broadcast segment if it is loaded on the given server.
   * Returns true only if the segment was successfully queued for drop on the server.
   */
  private boolean dropBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    if (server.isLoadingSegment(segment)) {
      return server.cancelOperation(SegmentAction.LOAD, segment);
    } else if (server.isServingSegment(segment)) {
      return stateManager.dropSegment(segment, server);
    } else {
      return false;
    }
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from a tier.
   * Tries to drop replicas first from decommissioning servers and then from
   * active servers.
   * <p>
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(
      int numToDrop,
      DataSegment segment,
      String tier,
      SegmentTierStatus segmentStatus
  )
  {
    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToDrop();
    if (eligibleServers.isEmpty() || numToDrop <= 0) {
      return 0;
    }

    final TreeSet<ServerHolder> eligibleLiveServers = new TreeSet<>();
    final TreeSet<ServerHolder> eligibleDyingServers = new TreeSet<>();
    for (ServerHolder server : eligibleServers) {
      if (server.isDecommissioning()) {
        eligibleDyingServers.add(server);
      } else {
        eligibleLiveServers.add(server);
      }
    }

    // Drop as many replicas as possible from decommissioning servers
    int remainingNumToDrop = numToDrop;
    int numDropsQueued = dropReplicasFromServers(remainingNumToDrop, segment, eligibleDyingServers.iterator());

    // Drop more replicas if required from active servers
    if (numToDrop > numDropsQueued) {
      remainingNumToDrop = numToDrop - numDropsQueued;
      Iterator<ServerHolder> serverIterator =
          eligibleLiveServers.size() >= remainingNumToDrop
          ? eligibleLiveServers.iterator()
          : strategy.pickServersToDrop(segment, eligibleLiveServers);
      numDropsQueued += dropReplicasFromServers(remainingNumToDrop, segment, serverIterator);
    }

    if (numToDrop > numDropsQueued) {
      stats.addToTieredStat(CoordinatorStats.DROP_SKIP_COUNT, tier, numToDrop - numDropsQueued);
      log.debug(
          "Queued only %d of %d drops of segment [%s] on tier [%s] due to failures.",
          numDropsQueued,
          numToDrop,
          segment.getId(),
          tier
      );
    }

    return numDropsQueued;
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from the servers.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicasFromServers(int numToDrop, DataSegment segment, Iterator<ServerHolder> serverIterator)
  {
    int numDropsQueued = 0;
    while (numToDrop > numDropsQueued && serverIterator.hasNext()) {
      ServerHolder holder = serverIterator.next();
      numDropsQueued += stateManager.dropSegment(segment, holder) ? 1 : 0;
    }

    return numDropsQueued;
  }

  /**
   * Queues load of {@code numToLoad} replicas of the segment on a tier.
   */
  private int loadReplicas(
      int numToLoad,
      DataSegment segment,
      String tier,
      SegmentTierStatus segmentStatus,
      boolean isFirstLoadOnTier
  )
  {
    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToLoad();
    if (eligibleServers.isEmpty()) {
      log.warn("No eligible server to load replica of segment [%s]", segment.getId());
      return 0;
    }

    final Iterator<ServerHolder> serverIterator =
        strategy.findNewSegmentHomeReplicator(segment, eligibleServers);
    if (!serverIterator.hasNext()) {
      log.warn("No candidate server to load replica of segment [%s]", segment.getId());
      return 0;
    }

    // Load the replicas on this tier
    int numLoadsQueued = 0;
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      boolean queueSuccess =
          stateManager.loadSegment(segment, serverIterator.next(), isFirstLoadOnTier, replicationThrottler);
      numLoadsQueued += queueSuccess ? 1 : 0;
    }

    if (numToLoad > numLoadsQueued) {
      stats.addToTieredStat(CoordinatorStats.ASSIGN_SKIP_COUNT, tier, numToLoad - numLoadsQueued);
      log.debug(
          "Queued only %d of %d loads of segment [%s] on tier [%s] due to throttling or failures.",
          numLoadsQueued,
          numToLoad,
          segment.getId(),
          tier
      );
    }

    return numLoadsQueued;
  }

  private int cancelOperations(
      SegmentAction action,
      int maxNumToCancel,
      DataSegment segment,
      SegmentTierStatus segmentStatus
  )
  {
    final List<ServerHolder> servers = segmentStatus.getServersPerforming(action);
    if (servers.isEmpty() || maxNumToCancel <= 0) {
      return 0;
    }

    int numCancelled = 0;
    for (int i = 0; i < servers.size() && numCancelled < maxNumToCancel; ++i) {
      numCancelled += servers.get(i).cancelOperation(action, segment) ? 1 : 0;
    }
    return numCancelled;
  }

}
