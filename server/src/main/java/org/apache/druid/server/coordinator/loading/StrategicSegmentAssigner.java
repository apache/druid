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

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Used by the coordinator in each run for segment loading, dropping, balancing
 * and broadcasting.
 * <p>
 * An instance of this class is freshly created for each coordinator run.
 */
@NotThreadSafe
public class StrategicSegmentAssigner implements SegmentActionHandler
{
  private final SegmentLoadQueueManager loadQueueManager;
  private final DruidCluster cluster;
  private final CoordinatorRunStats stats;
  private final SegmentReplicaCountMap replicaCountMap;
  private final ReplicationThrottler replicationThrottler;
  private final RoundRobinServerSelector serverSelector;
  private final BalancerStrategy strategy;

  private final boolean useRoundRobinAssignment;

  private final Map<String, Set<String>> datasourceToInvalidLoadTiers = new HashMap<>();
  private final Map<String, Integer> tierToHistoricalCount = new HashMap<>();
  private final Map<String, Set<SegmentId>> segmentsToDelete = new HashMap<>();
  private final Map<String, Set<DataSegment>> segmentsWithZeroRequiredReplicas = new HashMap<>();
  private final Set<DataSegment> broadcastSegments = new HashSet<>();

  public StrategicSegmentAssigner(
      SegmentLoadQueueManager loadQueueManager,
      DruidCluster cluster,
      BalancerStrategy strategy,
      SegmentLoadingConfig loadingConfig,
      CoordinatorRunStats stats
  )
  {
    this.stats = stats;
    this.cluster = cluster;
    this.strategy = strategy;
    this.loadQueueManager = loadQueueManager;
    this.replicaCountMap = SegmentReplicaCountMap.create(cluster);
    this.replicationThrottler = createReplicationThrottler(cluster, loadingConfig);
    this.useRoundRobinAssignment = loadingConfig.isUseRoundRobinSegmentAssignment();
    this.serverSelector = useRoundRobinAssignment ? new RoundRobinServerSelector(cluster) : null;

    cluster.getManagedHistoricals().forEach(
        (tier, historicals) -> tierToHistoricalCount.put(tier, historicals.size())
    );
  }

  public SegmentReplicationStatus getReplicationStatus()
  {
    return replicaCountMap.toReplicationStatus();
  }

  public Map<String, Set<SegmentId>> getSegmentsToDelete()
  {
    return segmentsToDelete;
  }

  public Map<String, Set<DataSegment>> getSegmentsWithZeroRequiredReplicas()
  {
    return segmentsWithZeroRequiredReplicas;
  }

  public Map<String, Set<String>> getDatasourceToInvalidLoadTiers()
  {
    return datasourceToInvalidLoadTiers;
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
   *   <li>there is no eligible destination server</li>
   *   <li>or segment is already optimally placed</li>
   *   <li>or some other error occurs</li>
   * </ul>
   */
  public boolean moveSegment(
      DataSegment segment,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers
  )
  {
    final String tier = sourceServer.getServer().getTier();
    final List<ServerHolder> eligibleDestinationServers =
        destinationServers.stream()
                          .filter(s -> s.getServer().getTier().equals(tier))
                          .filter(s -> s.canLoadSegment(segment))
                          .collect(Collectors.toList());

    if (eligibleDestinationServers.isEmpty()) {
      incrementSkipStat(Stats.Segments.MOVE_SKIPPED, "No eligible server", segment, tier);
      return false;
    }

    // If the source server is not decommissioning, move can be skipped if the
    // segment is already optimally placed
    if (!sourceServer.isDecommissioning()) {
      eligibleDestinationServers.add(sourceServer);
    }

    final ServerHolder destination =
        strategy.findDestinationServerToMoveSegment(segment, sourceServer, eligibleDestinationServers);

    if (destination == null || destination.getServer().equals(sourceServer.getServer())) {
      incrementSkipStat(Stats.Segments.MOVE_SKIPPED, "Optimally placed", segment, tier);
      return false;
    } else if (moveSegment(segment, sourceServer, destination)) {
      incrementStat(Stats.Segments.MOVED, segment, tier, 1);
      return true;
    } else {
      incrementSkipStat(Stats.Segments.MOVE_SKIPPED, "Encountered error", segment, tier);
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
        int loadedCountOnTier = replicaCountMap.get(segment.getId(), tier)
                                               .loadedNotDropping();
        if (loadedCountOnTier >= 1) {
          return replicateSegment(segment, serverB);
        } else {
          return loadSegment(segment, serverB);
        }
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
  public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    final Set<String> allTiersInCluster = Sets.newHashSet(cluster.getTierNames());

    if (tierToReplicaCount.isEmpty()) {
      // Track the counts for a segment even if it requires 0 replicas on all tiers
      replicaCountMap.computeIfAbsent(segment.getId(), DruidServer.DEFAULT_TIER);
    } else {
      // Identify empty tiers and determine total required replicas
      tierToReplicaCount.forEach((tier, requiredReplicas) -> {
        reportTierCapacityStats(segment, requiredReplicas, tier);

        SegmentReplicaCount replicaCount = replicaCountMap.computeIfAbsent(segment.getId(), tier);
        replicaCount.setRequired(requiredReplicas, tierToHistoricalCount.getOrDefault(tier, 0));

        if (!allTiersInCluster.contains(tier)) {
          datasourceToInvalidLoadTiers.computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
                                      .add(tier);
        }
      });
    }

    SegmentReplicaCount replicaCountInCluster = replicaCountMap.getTotal(segment.getId());
    if (replicaCountInCluster.required() <= 0) {
      segmentsWithZeroRequiredReplicas
          .computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
          .add(segment);
    }

    final int replicaSurplus = replicaCountInCluster.loadedNotDropping()
                               - replicaCountInCluster.requiredAndLoadable();

    // Update replicas in every tier
    int dropsQueued = 0;
    for (String tier : allTiersInCluster) {
      dropsQueued += updateReplicasInTier(
          segment,
          tier,
          tierToReplicaCount.getOrDefault(tier, 0),
          replicaSurplus - dropsQueued
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
    final SegmentReplicaCount replicaCountOnTier
        = replicaCountMap.get(segment.getId(), tier);

    final int projectedReplicas = replicaCountOnTier.loadedNotDropping()
                                  + replicaCountOnTier.loading()
                                  - Math.max(0, replicaCountOnTier.moveCompletedPendingDrop());

    final int movingReplicas = replicaCountOnTier.moving();
    final boolean shouldCancelMoves = requiredReplicas == 0 && movingReplicas > 0;

    // Check if there is any action required on this tier
    if (projectedReplicas == requiredReplicas && !shouldCancelMoves) {
      return 0;
    }

    final SegmentStatusInTier segmentStatus =
        new SegmentStatusInTier(segment, cluster.getManagedHistoricalsByTier(tier));

    // Cancel all moves in this tier if it does not need to have replicas
    if (shouldCancelMoves) {
      cancelOperations(SegmentAction.MOVE_TO, movingReplicas, segment, segmentStatus);
      cancelOperations(SegmentAction.MOVE_FROM, movingReplicas, segment, segmentStatus);
    }

    // Cancel drops and queue loads if the projected count is below the requirement
    if (projectedReplicas < requiredReplicas) {
      int replicaDeficit = requiredReplicas - projectedReplicas;
      int cancelledDrops =
          cancelOperations(SegmentAction.DROP, replicaDeficit, segment, segmentStatus);

      // Cancelled drops can be counted as loaded replicas, thus reducing deficit
      int numReplicasToLoad = replicaDeficit - cancelledDrops;
      if (numReplicasToLoad > 0) {
        int numLoadedReplicas = replicaCountOnTier.loadedNotDropping() + cancelledDrops;
        int numLoadsQueued = loadReplicas(numReplicasToLoad, numLoadedReplicas, segment, tier, segmentStatus);
        incrementStat(Stats.Segments.ASSIGNED, segment, tier, numLoadsQueued);
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
    final RowKey rowKey = RowKey.of(Dimension.TIER, tier);
    stats.updateMax(Stats.Tier.REPLICATION_FACTOR, rowKey, requiredReplicas);
    stats.add(Stats.Tier.REQUIRED_CAPACITY, rowKey, segment.getSize() * requiredReplicas);
  }

  @Override
  public void broadcastSegment(DataSegment segment)
  {
    final Object2IntOpenHashMap<String> tierToRequiredReplicas = new Object2IntOpenHashMap<>();
    for (ServerHolder server : cluster.getAllManagedServers()) {
      // Ignore servers which are not broadcast targets
      if (!server.getServer().getType().isSegmentBroadcastTarget()) {
        continue;
      }

      final String tier = server.getServer().getTier();

      // Drop from decommissioning servers and load on active servers
      int numDropsQueued = 0;
      int numLoadsQueued = 0;
      if (server.isDecommissioning()) {
        numDropsQueued += dropBroadcastSegment(segment, server) ? 1 : 0;
      } else {
        tierToRequiredReplicas.addTo(tier, 1);
        numLoadsQueued += loadBroadcastSegment(segment, server) ? 1 : 0;
      }

      if (numLoadsQueued > 0) {
        incrementStat(Stats.Segments.ASSIGNED, segment, tier, numLoadsQueued);
      }
      if (numDropsQueued > 0) {
        incrementStat(Stats.Segments.DROPPED, segment, tier, numDropsQueued);
      }
    }

    // Update required replica counts
    tierToRequiredReplicas.object2IntEntrySet().fastForEach(
        entry -> replicaCountMap.computeIfAbsent(segment.getId(), entry.getKey())
                                .setRequired(entry.getIntValue(), entry.getIntValue())
    );

    broadcastSegments.add(segment);
  }

  @Override
  public void deleteSegment(DataSegment segment)
  {
    segmentsToDelete
        .computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
        .add(segment.getId());
  }

  /**
   * Loads the broadcast segment if it is not already loaded on the given server.
   * Returns true only if the segment was successfully queued for load on the server.
   */
  private boolean loadBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    if (server.isServingSegment(segment) || server.isLoadingSegment(segment)) {
      return false;
    } else if (server.isDroppingSegment(segment)) {
      return server.cancelOperation(SegmentAction.DROP, segment);
    } else if (server.canLoadSegment(segment)) {
      return loadSegment(segment, server);
    }

    final String skipReason;
    if (server.getAvailableSize() < segment.getSize()) {
      skipReason = "Not enough disk space";
    } else if (server.isLoadQueueFull()) {
      skipReason = "Load queue is full";
    } else {
      skipReason = "Unknown error";
    }

    incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, skipReason, segment, server.getServer().getTier());
    return false;
  }

  public Set<DataSegment> getBroadcastSegments()
  {
    return broadcastSegments;
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
      SegmentStatusInTier segmentStatus
  )
  {
    if (numToDrop <= 0) {
      return 0;
    }

    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToDrop();
    if (eligibleServers.isEmpty()) {
      incrementSkipStat(Stats.Segments.DROP_SKIPPED, "No eligible server", segment, tier);
      return 0;
    }

    // Keep eligible servers sorted by most full first
    final TreeSet<ServerHolder> eligibleLiveServers = new TreeSet<>(Comparator.reverseOrder());
    final TreeSet<ServerHolder> eligibleDyingServers = new TreeSet<>(Comparator.reverseOrder());
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
          (useRoundRobinAssignment || eligibleLiveServers.size() <= remainingNumToDrop)
          ? eligibleLiveServers.iterator()
          : strategy.findServersToDropSegment(segment, new ArrayList<>(eligibleLiveServers));
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
        incrementSkipStat(Stats.Segments.DROP_SKIPPED, "Encountered error", segment, tier);
      }
    }

    return numDropsQueued;
  }

  /**
   * Queues load of {@code numToLoad} replicas of the segment on a tier.
   */
  private int loadReplicas(
      int numToLoad,
      int numLoadedReplicas,
      DataSegment segment,
      String tier,
      SegmentStatusInTier segmentStatus
  )
  {
    final boolean isAlreadyLoadedOnTier = numLoadedReplicas >= 1;

    // Do not assign replicas if tier is already busy loading some
    if (isAlreadyLoadedOnTier && replicationThrottler.isReplicationThrottledForTier(tier)) {
      return 0;
    }

    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToLoad();
    if (eligibleServers.isEmpty()) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "No eligible server", segment, tier);
      return 0;
    }

    final Iterator<ServerHolder> serverIterator =
        useRoundRobinAssignment
        ? serverSelector.getServersInTierToLoadSegment(tier, segment)
        : strategy.findServersToLoadSegment(segment, eligibleServers);
    if (!serverIterator.hasNext()) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "No strategic server", segment, tier);
      return 0;
    }

    // Load the replicas on this tier
    int numLoadsQueued = 0;
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      ServerHolder server = serverIterator.next();
      boolean queuedSuccessfully = isAlreadyLoadedOnTier ? replicateSegment(segment, server)
                                                         : loadSegment(segment, server);
      numLoadsQueued += queuedSuccessfully ? 1 : 0;
    }

    return numLoadsQueued;
  }

  private boolean loadSegment(DataSegment segment, ServerHolder server)
  {
    final String tier = server.getServer().getTier();
    final boolean assigned = loadQueueManager.loadSegment(segment, server, SegmentAction.LOAD);

    if (!assigned) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "Encountered error", segment, tier);
    }

    return assigned;
  }

  private boolean replicateSegment(DataSegment segment, ServerHolder server)
  {
    final String tier = server.getServer().getTier();
    if (replicationThrottler.isReplicationThrottledForTier(tier)) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "Throttled replication", segment, tier);
      return false;
    }

    final boolean assigned = loadQueueManager.loadSegment(segment, server, SegmentAction.REPLICATE);
    if (!assigned) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "Encountered error", segment, tier);
    } else {
      replicationThrottler.incrementAssignedReplicas(tier);
    }

    return assigned;
  }

  private static ReplicationThrottler createReplicationThrottler(
      DruidCluster cluster,
      SegmentLoadingConfig loadingConfig
  )
  {
    final Map<String, Integer> tierToLoadingReplicaCount = new HashMap<>();

    cluster.getManagedHistoricals().forEach(
        (tier, historicals) -> {
          int numLoadingReplicas = historicals.stream().mapToInt(ServerHolder::getNumLoadingReplicas).sum();
          tierToLoadingReplicaCount.put(tier, numLoadingReplicas);
        }
    );
    return new ReplicationThrottler(
        tierToLoadingReplicaCount,
        loadingConfig.getReplicationThrottleLimit()
    );
  }

  private int cancelOperations(
      SegmentAction action,
      int maxNumToCancel,
      DataSegment segment,
      SegmentStatusInTier segmentStatus
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

  private void incrementSkipStat(CoordinatorStat stat, String reason, DataSegment segment, String tier)
  {
    final RowKey key = RowKey.with(Dimension.TIER, tier)
                             .with(Dimension.DATASOURCE, segment.getDataSource())
                             .and(Dimension.DESCRIPTION, reason);
    stats.add(stat, key, 1);
  }

  private void incrementStat(CoordinatorStat stat, DataSegment segment, String tier, long value)
  {
    stats.addToSegmentStat(stat, tier, segment.getDataSource(), value);
  }

}
