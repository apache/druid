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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
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
    this.stateManager = stateManager;
    this.cluster = cluster;
    this.replicantLookup = replicantLookup;
    this.replicationThrottler = replicationThrottler;
    this.strategy = strategy;
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
   * Moves the given segment between two servers of the same tier.
   * <p>
   * See if we can move balancing here.
   */
  public boolean moveSegment(DataSegment segment, ServerHolder fromServer, ServerHolder toServer)
  {
    final String tier = toServer.getServer().getTier();
    if (!fromServer.getServer().getTier().equals(tier)) {
      return false;
    }

    // fromServer must be loading or serving the segment
    // and toServer must be able to load it
    if (!(fromServer.isLoadingSegment(segment) || fromServer.isServingSegment(segment))
        || !toServer.canLoadSegment(segment)) {
      return false;
    }

    final boolean loadCancelledOnFromServer =
        stateManager.cancelOperation(SegmentState.LOADING, segment, fromServer);
    if (loadCancelledOnFromServer) {
      stats.addToTieredStat(CoordinatorStats.CANCELLED_LOADS, tier, 1);
      int loadedCountOnTier = replicantLookup.getLoadedReplicants(segment.getId(), tier);
      return stateManager.loadSegment(segment, toServer, loadedCountOnTier < 1, replicationThrottler);
    } else {
      return stateManager.moveSegment(segment, fromServer, toServer, replicationThrottler.getMaxLifetime());
    }
  }

  /**
   * Queues load or drop of replicas of the given segment to achieve the
   * target replication level in all the tiers.
   */
  public void updateReplicas(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    // Handle every target tier
    tierToReplicaCount.forEach((tier, numReplicas) -> {
      updateReplicasOnTargetTier(segment, tier, tierToReplicaCount.get(tier));
      stats.addToTieredStat(CoordinatorStats.REQUIRED_CAPACITY, tier, segment.getSize() * numReplicas);
    });

    // Find the total level of replication required on target tiers
    final int requiredTotalReplication = tierToReplicaCount.values().stream()
                                                           .reduce(0, Integer::sum);

    // To ensure that segment read concurrency does not suffer during a tier shift,
    // drop old replicas while always maintaining total required replication
    int loadedReplicas = replicantLookup.getLoadedReplicants(segment.getId());
    if (loadedReplicas <= requiredTotalReplication) {
      return;
    }

    // If there are more replicas than required, try to drop from unneeded tiers
    final Set<String> allTiers = Sets.newHashSet(cluster.getTierNames());
    allTiers.removeAll(tierToReplicaCount.keySet());
    final Iterator<String> unneededTiers = allTiers.iterator();

    int numDropsQueued = 0;
    final int numReplicasToDrop = loadedReplicas - requiredTotalReplication;
    while (numReplicasToDrop > numDropsQueued && unneededTiers.hasNext()) {
      numDropsQueued +=
          dropReplicasFromUnneededTier(numReplicasToDrop - numDropsQueued, segment, unneededTiers.next());
    }
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
    stats.addToGlobalStat(CoordinatorStats.DELETED_SEGMENTS, 1);
  }

  /**
   * Loads the broadcast segment if it is not loaded on the given server.
   * Returns true only if the segment was successfully queued for load on the server.
   */
  private boolean loadBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    final SegmentState state = server.getSegmentState(segment);
    if (state == SegmentState.LOADED || state == SegmentState.LOADING) {
      return false;
    }

    // Cancel drop if it is in progress
    boolean dropCancelled = stateManager.cancelOperation(SegmentState.DROPPING, segment, server);
    if (dropCancelled) {
      return false;
    }

    if (server.canLoadSegment(segment)
        && stateManager.loadSegment(segment, server, true, replicationThrottler)) {
      return true;
    } else {
      log.makeAlert("Failed to broadcast segment for [%s]", segment.getDataSource())
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
    final SegmentState state = server.getSegmentState(segment);
    if (state == SegmentState.NONE || state == SegmentState.DROPPING) {
      return false;
    }

    // Cancel load if it is in progress
    boolean loadCancelled = stateManager.cancelOperation(SegmentState.LOADING, segment, server);
    if (loadCancelled) {
      return false;
    }

    return stateManager.dropSegment(segment, server);
  }

  /**
   * Loads or drops the replicas of a segment on a target tier, i.e. a tier
   * that specifies non-zero replication in the matched load rule.
   */
  private void updateReplicasOnTargetTier(DataSegment segment, String tier, int targetCount)
  {
    // Do not proceed if tier is empty, send an alert for empty tiers later
    if (emptyTiers.contains(tier)) {
      return;
    }

    final Map<SegmentState, List<ServerHolder>> serversByState = getSegmentStateInTier(segment, tier);
    if (serversByState.isEmpty()) {
      emptyTiers.add(tier);
      return;
    }

    final int currentCount = serversByState.get(SegmentState.LOADED).size()
                             + serversByState.get(SegmentState.LOADING).size();
    if (targetCount == currentCount) {
      return;
    }

    if (targetCount > currentCount) {
      cancelDropOrLoadReplicas(targetCount - currentCount, segment, tier, serversByState);
    } else {
      cancelLoadOrDropReplicas(currentCount - targetCount, segment, tier, serversByState);
    }
  }

  private Map<SegmentState, List<ServerHolder>> getSegmentStateInTier(DataSegment segment, String tier)
  {
    final Set<ServerHolder> historicals = cluster.getHistoricalsByTier(tier);
    if (historicals == null || historicals.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<SegmentState, List<ServerHolder>> serversByState = new EnumMap<>(SegmentState.class);
    Arrays.stream(SegmentState.values())
          .forEach(state -> serversByState.put(state, new ArrayList<>()));
    historicals.forEach(
        serverHolder -> serversByState
            .get(serverHolder.getSegmentState(segment))
            .add(serverHolder)
    );
    return serversByState;
  }

  /**
   * Drops replicas of the segment from an unneeded tier, i.e. a tier that
   * specifies no replication in the matched load rule.
   */
  private int dropReplicasFromUnneededTier(int numReplicasToDrop, DataSegment segment, String tier)
  {
    final Map<SegmentState, List<ServerHolder>> serversByState = getSegmentStateInTier(segment, tier);
    if (serversByState.isEmpty()) {
      return 0;
    }

    // Cancel all in-progress balancing moves on this tier, if any
    final List<ServerHolder> movingServers = serversByState.get(SegmentState.MOVING_TO);
    if (!movingServers.isEmpty()) {
      int cancelledMoves =
          cancelOperations(SegmentState.MOVING_TO, segment, movingServers, movingServers.size());
      stats.addToTieredStat(CoordinatorStats.CANCELLED_MOVES, tier, cancelledMoves);
    }

    // Cancel all in-progress loads on this tier, if any
    final List<ServerHolder> loadingServers = serversByState.get(SegmentState.LOADING);
    if (!loadingServers.isEmpty()) {
      int cancelledLoads =
          cancelOperations(SegmentState.LOADING, segment, loadingServers, loadingServers.size());
      stats.addToTieredStat(CoordinatorStats.CANCELLED_LOADS, tier, cancelledLoads);
    }

    // Drop as many replicas as required from this tier
    int successfullyQueuedDrops =
        dropReplicasDecommissioningFirst(numReplicasToDrop, segment, serversByState.get(SegmentState.LOADED));
    stats.addToTieredStat(CoordinatorStats.DROPPED_COUNT, tier, successfullyQueuedDrops);
    return successfullyQueuedDrops;
  }

  private void cancelDropOrLoadReplicas(
      int numReplicasToLoad,
      DataSegment segment,
      String tier,
      Map<SegmentState, List<ServerHolder>> serversByState
  )
  {
    // Try to cancel in-progress drop operations
    final int cancelledDrops = cancelOperations(
        SegmentState.DROPPING,
        segment,
        serversByState.get(SegmentState.DROPPING),
        numReplicasToLoad
    );

    // Successfully cancelled drops can be counted as loaded replicas
    numReplicasToLoad -= cancelledDrops;
    final int totalReplicas = serversByState.get(SegmentState.LOADED).size()
                              + serversByState.get(SegmentState.LOADING).size()
                              + cancelledDrops;
    if (numReplicasToLoad > 0) {
      int successfulLoadsQueued = loadReplicas(
          numReplicasToLoad,
          segment,
          serversByState.get(SegmentState.NONE),
          totalReplicas > 0
      );

      stats.addToTieredStat(CoordinatorStats.ASSIGNED_COUNT, tier, successfulLoadsQueued);
      if (numReplicasToLoad > successfulLoadsQueued) {
        log.warn(
            "Queued %d of %d loads of segment [%s] on tier [%s].",
            successfulLoadsQueued,
            numReplicasToLoad,
            segment.getId(),
            tier
        );
      }
    }
  }

  /**
   * Cancels in-progress loads of this segment and queues new drops as required.
   *
   * @param numReplicasToDrop Total required number of cancelled load operations
   *                          and queued drop operations.
   */
  private void cancelLoadOrDropReplicas(
      int numReplicasToDrop,
      DataSegment segment,
      String tier,
      Map<SegmentState, List<ServerHolder>> serversByState
  )
  {
    // Try to cancel in-progress loads
    final int cancelledLoads;
    final List<ServerHolder> loadingServers = serversByState.get(SegmentState.LOADING);
    if (loadingServers != null && !loadingServers.isEmpty()) {
      cancelledLoads =
          cancelOperations(SegmentState.LOADING, segment, loadingServers, numReplicasToDrop);
      stats.addToTieredStat(CoordinatorStats.CANCELLED_LOADS, tier, cancelledLoads);
    } else {
      cancelledLoads = 0;
    }

    // Successfully cancelled loads can be counted as drops
    numReplicasToDrop -= cancelledLoads;
    if (numReplicasToDrop > 0) {
      final int successfulDropsQueued = dropReplicasDecommissioningFirst(
          numReplicasToDrop,
          segment,
          serversByState.get(SegmentState.LOADED)
      );

      stats.addToTieredStat(CoordinatorStats.DROPPED_COUNT, tier, successfulDropsQueued);
      if (numReplicasToDrop > successfulDropsQueued) {
        log.warn(
            "Queued %d of %d loads of segment [%s] on tier [%s].",
            successfulDropsQueued,
            numReplicasToDrop,
            segment.getId(),
            tier
        );
      }
    }
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from a tier.
   * Tries to drop replicas first from decommissioning servers and then from
   * active servers.
   * <p>
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicasDecommissioningFirst(
      int numToDrop,
      DataSegment segment,
      List<ServerHolder> eligibleServers
  )
  {
    if (eligibleServers == null || eligibleServers.isEmpty()) {
      return 0;
    }

    final TreeSet<ServerHolder> eligibleLiveServers = new TreeSet<>();
    final TreeSet<ServerHolder> eligibleDyingServers = new TreeSet<>();
    for (ServerHolder server : eligibleServers) {
      if (!server.isServingSegment(segment)) {
        // ignore this server
      } else if (server.isDecommissioning()) {
        eligibleDyingServers.add(server);
      } else {
        eligibleLiveServers.add(server);
      }
    }

    // Drop as many replicas as possible from decommissioning servers
    int remainingNumToDrop = numToDrop;
    int numDropsQueued = dropReplicas(remainingNumToDrop, segment, eligibleDyingServers.iterator());

    // Drop more replicas if required from active servers
    if (numToDrop > numDropsQueued) {
      remainingNumToDrop = numToDrop - numDropsQueued;
      Iterator<ServerHolder> serverIterator =
          eligibleLiveServers.size() >= remainingNumToDrop
          ? eligibleLiveServers.iterator()
          : strategy.pickServersToDrop(segment, eligibleLiveServers);
      numDropsQueued += dropReplicas(remainingNumToDrop, segment, serverIterator);
    }

    return numDropsQueued;
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from the servers.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(int numToDrop, DataSegment segment, Iterator<ServerHolder> serverIterator)
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
   *
   * @param isSegmentAvailableOnTier true if there is atleast one replica of the
   *                                 segment already loaded on this tier.
   * @return The number of successfully queued load operations.
   */
  private int loadReplicas(
      int numToLoad,
      DataSegment segment,
      List<ServerHolder> candidateServers,
      boolean isSegmentAvailableOnTier
  )
  {
    final List<ServerHolder> eligibleServers =
        candidateServers.stream()
                        .filter(server -> server.canLoadSegment(segment))
                        .collect(Collectors.toList());
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

    // Load the primary on this tier
    int numLoadsQueued = 0;
    if (!isSegmentAvailableOnTier) {
      boolean queueSuccess =
          stateManager.loadSegment(segment, serverIterator.next(), true, replicationThrottler);
      numLoadsQueued += queueSuccess ? 1 : 0;
    }

    // Load the remaining replicas
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      boolean queueSuccess =
          stateManager.loadSegment(segment, serverIterator.next(), false, replicationThrottler);
      numLoadsQueued += queueSuccess ? 1 : 0;
    }
    return numLoadsQueued;
  }

  private int cancelOperations(
      SegmentState state,
      DataSegment segment,
      List<ServerHolder> servers,
      int maxNumToCancel
  )
  {
    int numCancelled = 0;
    for (int i = 0; i < servers.size() && numCancelled < maxNumToCancel; ++i) {
      numCancelled += stateManager.cancelOperation(state, segment, servers.get(i)) ? 1 : 0;
    }
    return numCancelled;
  }

}
