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
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.loadqueue.SegmentAction;
import org.apache.druid.server.coordinator.loadqueue.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Used by the coordinator in each run for segment loading, dropping, balancing
 * and broadcasting.
 * <p>
 * An instance of this class is freshly created for each coordinator run.
 */
public class StrategicSegmentAssigner implements SegmentActionHandler
{
  private static final EmittingLogger log = new EmittingLogger(StrategicSegmentAssigner.class);

  private final SegmentLoadQueueManager loadQueueManager;
  private final DruidCluster cluster;
  private final CoordinatorRunStats stats;
  private final SegmentReplicantLookup replicantLookup;
  private final ReplicationThrottler replicationThrottler;
  private final RoundRobinServerSelector serverSelector;
  private final BalancerStrategy strategy;

  private final boolean useRoundRobinAssignment;

  private final Set<String> tiersWithNoServer = new HashSet<>();

  public StrategicSegmentAssigner(
      SegmentLoadQueueManager loadQueueManager,
      DruidCluster cluster,
      SegmentReplicantLookup replicantLookup,
      ReplicationThrottler replicationThrottler,
      BalancerStrategy strategy,
      CoordinatorDynamicConfig dynamicConfig
  )
  {
    this.cluster = cluster;
    this.strategy = strategy;
    this.loadQueueManager = loadQueueManager;
    this.replicantLookup = replicantLookup;
    this.replicationThrottler = replicationThrottler;
    this.useRoundRobinAssignment = dynamicConfig.isUseRoundRobinSegmentAssignment();
    this.stats = new CoordinatorRunStats(dynamicConfig.getValidatedDebugDimensions());
    this.serverSelector = useRoundRobinAssignment ? new RoundRobinServerSelector(cluster) : null;
  }

  public CoordinatorRunStats getStats()
  {
    return stats;
  }

  public void makeAlerts()
  {
    if (!tiersWithNoServer.isEmpty()) {
      log.makeAlert("Tiers [%s] have no servers! Check your cluster configuration.", tiersWithNoServer).emit();
    }
  }

  /**
   * Moves the given segment from the source server to an eligible destination
   * server.
   * <p>
   * An eligible destination server must:
   * <ul>
   *   <li>be present in the given list of destination servers</li>
   *   <li>belong to the same tier as the source server</li>
   *   <li>not already be serving or loading a replica of the segment</li>
   *   <li>have enough space to load the segment</li>
   * </ul>
   * <p>
   * The segment is not moved if:
   * <ul>
   *   <li>there is no eligible destination server, or</li>
   *   <li>skipIfOptimallyPlaced is true and segment is already optimally placed, or</li>
   *   <li>some other error occurs</li>
   * </ul>
   */
  public boolean moveSegment(
      DataSegment segment,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers,
      boolean skipIfOptimallyPlaced
  )
  {
    final String tier = sourceServer.getServer().getTier();
    final List<ServerHolder> eligibleDestinationServers =
        destinationServers.stream()
                          .filter(s -> s.getServer().getTier().equals(tier))
                          .filter(s -> s.canLoadSegment(segment))
                          .collect(Collectors.toList());

    if (eligibleDestinationServers.isEmpty()) {
      incrementStat(Error.NO_ELIGIBLE_SERVER_FOR_MOVE, segment, tier);
      return false;
    }

    // Add the source server as an eligible server if skipping is allowed
    if (skipIfOptimallyPlaced) {
      eligibleDestinationServers.add(sourceServer);
    }

    final ServerHolder destination =
        strategy.findDestinationServerToMoveSegment(segment, sourceServer, eligibleDestinationServers);

    if (destination == null || destination.getServer().equals(sourceServer.getServer())) {
      incrementStat(Error.MOVE_SKIPPED_OPTIMALLY_PLACED, segment, tier);
      return false;
    } else if (moveSegment(segment, sourceServer, destination)) {
      incrementStat(Stats.Segments.MOVED, segment, tier);
      return true;
    } else {
      incrementStat(Error.MOVE_FAILED, segment, tier);
      return false;
    }
  }

  /**
   * Moves the given segment from serverA to serverB.
   */
  private boolean moveSegment(DataSegment segment, ServerHolder serverA, ServerHolder serverB)
  {
    final String tier = serverA.getServer().getTier();
    if (serverA.isLoadingSegment(segment)) {
      // Cancel the load on serverA and load on serverB instead
      if (serverA.cancelOperation(SegmentAction.LOAD, segment)) {
        int loadedCountOnTier = replicantLookup.getServedReplicas(segment.getId(), tier);
        return loadQueueManager.loadSegment(segment, serverB, loadedCountOnTier < 1, replicationThrottler);
      }

      // Could not cancel load, let the segment load on serverA and count it as unmoved
      return false;
    } else if (serverA.isServingSegment(segment)) {
      return loadQueueManager.moveSegment(segment, serverA, serverB);
    } else {
      return false;
    }
  }

  @Override
  public void updateSegmentReplicasInTiers(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    // Identify empty tiers and determine total required replicas
    final AtomicInteger requiredTotalReplicas = new AtomicInteger(0);
    final Set<String> allTiers = Sets.newHashSet(cluster.getTierNames());
    tierToReplicaCount.forEach((tier, requiredReplicas) -> {
      reportTierCapacityStats(segment, requiredReplicas, tier);
      if (allTiers.contains(tier)) {
        requiredTotalReplicas.addAndGet(requiredReplicas);
      } else {
        tiersWithNoServer.add(tier);
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

    final SegmentTierStatus segmentStatus =
        new SegmentTierStatus(segment, cluster.getHistoricalsByTier(tier));

    // Cancel all moves in this tier if it does not need to have replicas
    if (shouldCancelMoves) {
      cancelOperations(SegmentAction.MOVE_TO, movingReplicas, segment, segmentStatus);
    }

    // Cancel drops and queue loads if the projected count is below the requirement
    if (projectedReplicas < requiredReplicas) {
      int replicaDeficit = requiredReplicas - projectedReplicas;
      int cancelledDrops =
          cancelOperations(SegmentAction.DROP, replicaDeficit, segment, segmentStatus);

      // Cancelled drops can be counted as loaded replicas, thus reducing deficit
      int numReplicasToLoad = replicaDeficit - cancelledDrops;
      if (numReplicasToLoad > 0) {
        boolean isFirstLoadOnTier = replicantLookup.getServedReplicas(segment.getId(), tier)
                                    + cancelledDrops < 1;
        int numLoadsQueued = loadReplicas(numReplicasToLoad, segment, tier, segmentStatus, isFirstLoadOnTier);
        incrementStat(Stats.Segments.ASSIGNED, segment, tier, numLoadsQueued);
        incrementStat(Stats.Segments.UNDER_REPLICATED, segment, tier, numReplicasToLoad);
      }
    }

    // Cancel loads and queue drops if the projected count exceeds the requirement
    if (projectedReplicas > requiredReplicas) {
      int replicaSurplus = projectedReplicas - requiredReplicas;
      int cancelledLoads =
          cancelOperations(SegmentAction.LOAD, replicaSurplus, segment, segmentStatus);

      int numReplicasToDrop = Math.min(replicaSurplus - cancelledLoads, maxReplicasToDrop);
      if (numReplicasToDrop > 0) {
        int dropsQueuedOnTier = dropReplicas(numReplicasToDrop, segment, tier, segmentStatus);
        incrementStat(Stats.Segments.DROPPED, segment, tier, dropsQueuedOnTier);
        return dropsQueuedOnTier;
      }
    }

    return 0;
  }

  private void reportTierCapacityStats(DataSegment segment, int requiredReplicas, String tier)
  {
    final RowKey rowKey = RowKey.forTier(tier);
    stats.updateMax(Stats.Tier.REPLICATION_FACTOR, rowKey, requiredReplicas);
    stats.add(Stats.Tier.REQUIRED_CAPACITY, rowKey, segment.getSize() * requiredReplicas);
  }

  @Override
  public void broadcastSegment(DataSegment segment)
  {
    for (ServerHolder server : cluster.getAllServers()) {
      // Ignore servers which are not broadcast targets
      if (!server.getServer().getType().isSegmentBroadcastTarget()) {
        continue;
      }

      // Drop from decommissioning servers and load on active servers
      final String tier = server.getServer().getTier();
      if (server.isDecommissioning() && dropBroadcastSegment(segment, server)) {
        incrementStat(Stats.Segments.DROPPED_BROADCAST, segment, tier);
      }
      if (!server.isDecommissioning() && loadBroadcastSegment(segment, server)) {
        incrementStat(Stats.Segments.ASSIGNED_BROADCAST, segment, tier);
      }
    }
  }

  @Override
  public void deleteSegment(DataSegment segment)
  {
    loadQueueManager.deleteSegment(segment);
    stats.addToDatasourceStat(Stats.Segments.DELETED, segment.getDataSource(), 1);
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
        && loadQueueManager.loadSegment(segment, server, true, replicationThrottler)) {
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
      return loadQueueManager.dropSegment(segment, server);
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
      final int numToDrop,
      DataSegment segment,
      String tier,
      SegmentTierStatus segmentStatus
  )
  {
    if (numToDrop <= 0) {
      return 0;
    }

    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToDrop();
    if (eligibleServers.isEmpty()) {
      incrementStat(Error.NO_ELIGIBLE_SERVER_FOR_DROP, segment, tier);
      return 0;
    }

    // Keep eligible servers sorted by most full first
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
    int numDropsQueued =
        dropReplicasFromServers(remainingNumToDrop, segment, eligibleDyingServers.iterator(), tier);

    // Drop replicas from active servers if required
    if (numToDrop > numDropsQueued) {
      remainingNumToDrop = numToDrop - numDropsQueued;
      Iterator<ServerHolder> serverIterator =
          (useRoundRobinAssignment || eligibleLiveServers.size() >= remainingNumToDrop)
          ? eligibleLiveServers.iterator()
          : strategy.pickServersToDrop(segment, eligibleLiveServers);
      numDropsQueued += dropReplicasFromServers(remainingNumToDrop, segment, serverIterator, tier);
    }

    return numDropsQueued;
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from the servers.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicasFromServers(
      int numToDrop,
      DataSegment segment,
      Iterator<ServerHolder> serverIterator,
      String tier
  )
  {
    int numDropsQueued = 0;
    while (numToDrop > numDropsQueued && serverIterator.hasNext()) {
      ServerHolder holder = serverIterator.next();
      boolean dropped = loadQueueManager.dropSegment(segment, holder);

      if (dropped) {
        ++numDropsQueued;
      } else {
        incrementStat(Error.DROP_FAILED, segment, tier);
      }
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
      incrementStat(Error.NO_ELIGIBLE_SERVER_FOR_LOAD, segment, tier);
      return 0;
    }

    final Iterator<ServerHolder> serverIterator =
        useRoundRobinAssignment
        ? serverSelector.getServersInTierToLoadSegment(tier, segment)
        : strategy.findServerToLoadSegment(segment, eligibleServers);
    if (!serverIterator.hasNext()) {
      incrementStat(Error.NO_STRATEGIC_SERVER_FOR_LOAD, segment, tier);
      return 0;
    }

    // Load the replicas on this tier
    int numLoadsQueued = 0;
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      boolean queueSuccess =
          loadQueueManager.loadSegment(segment, serverIterator.next(), isFirstLoadOnTier, replicationThrottler);

      if (queueSuccess) {
        ++numLoadsQueued;
      } else if (isFirstLoadOnTier) {
        incrementStat(Error.LOAD_FAILED, segment, tier);
      } else {
        incrementStat(Error.REPLICA_THROTTLED, segment, tier);
      }
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

  private void incrementStat(CoordinatorStat stat, DataSegment segment, String tier)
  {
    incrementStat(stat, segment, tier, 1);
  }

  private void incrementStat(CoordinatorStat stat, DataSegment segment, String tier, long value)
  {
    stats.addToSegmentStat(stat, tier, segment.getDataSource(), value);
  }

  /**
   * These errors are tracked for debugging purposes only. They can be logged by
   * the respective duties.
   */
  private static class Error
  {
    static final CoordinatorStat MOVE_FAILED
        = new CoordinatorStat("failed to start move", CoordinatorStat.Level.ERROR);
    static final CoordinatorStat NO_ELIGIBLE_SERVER_FOR_MOVE
        = new CoordinatorStat("no eligible server for move", CoordinatorStat.Level.INFO);
    static final CoordinatorStat MOVE_SKIPPED_OPTIMALLY_PLACED
        = new CoordinatorStat("move skipped (optimally placed)");

    static final CoordinatorStat DROP_FAILED
        = new CoordinatorStat("failed to start drop", CoordinatorStat.Level.ERROR);
    static final CoordinatorStat NO_ELIGIBLE_SERVER_FOR_DROP
        = new CoordinatorStat("no eligible server for drop", CoordinatorStat.Level.INFO);

    static final CoordinatorStat LOAD_FAILED
        = new CoordinatorStat("failed to start load", CoordinatorStat.Level.ERROR);
    static final CoordinatorStat REPLICA_THROTTLED
        = new CoordinatorStat("replica throttled");
    static final CoordinatorStat NO_ELIGIBLE_SERVER_FOR_LOAD
        = new CoordinatorStat("no eligible server for load", CoordinatorStat.Level.INFO);
    static final CoordinatorStat NO_STRATEGIC_SERVER_FOR_LOAD
        = new CoordinatorStat("no strategic server for load", CoordinatorStat.Level.INFO);
  }

}
