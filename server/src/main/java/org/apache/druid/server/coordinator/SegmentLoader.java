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
import java.util.EnumMap;
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
  private final BalancerStrategy strategy;

  public SegmentLoader(SegmentStateManager stateManager, DruidCoordinatorRuntimeParams runParams)
  {
    this.stateManager = stateManager;
    this.strategy = runParams.getBalancerStrategy();
    this.cluster = runParams.getDruidCluster();
    this.replicantLookup = runParams.getSegmentReplicantLookup();
  }

  public CoordinatorStats getStats()
  {
    return stats;
  }

  /**
   * Moves the given segment between two servers of the same tier.
   * <p>
   * See if we can move balancing here.
   */
  public boolean moveSegment(DataSegment segment, ServerHolder fromServer, ServerHolder toServer)
  {
    if (!fromServer.getServer().getTier().equals(toServer.getServer().getTier())) {
      return false;
    }

    // fromServer must be loading or serving the segment
    // and toServer must be able to load it
    final SegmentState stateOnSrc = fromServer.getSegmentState(segment);
    if ((stateOnSrc != SegmentState.LOADING && stateOnSrc != SegmentState.LOADED)
        || !toServer.canLoadSegment(segment)) {
      return false;
    }

    final boolean cancelSuccess = stateOnSrc == SegmentState.LOADING
                                  && stateManager.cancelOperation(SegmentState.LOADING, segment, fromServer);

    if (cancelSuccess) {
      int loadedCountOnTier = replicantLookup
          .getLoadedReplicants(segment.getId(), toServer.getServer().getTier());
      stateManager.loadSegment(segment, toServer, loadedCountOnTier < 1);
    } else {
      return stateManager.moveSegment(segment, fromServer, toServer);
    }

    return true;
  }

  /**
   * Queues load or drop of replicas of the given segment to achieve the
   * target replication level in all the tiers.
   */
  public void updateReplicas(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    // Handle every target tier
    tierToReplicaCount.forEach((tier, numReplicas) -> {
      updateReplicasOnTier(segment, tier, tierToReplicaCount.get(tier));
      stats.addToTieredStat(CoordinatorStats.REQUIRED_CAPACITY, tier, segment.getSize() * numReplicas);
    });

    // Find the minimum number of segments required for fault tolerance
    final int totalTargetReplicas = tierToReplicaCount.values().stream()
                                                      .reduce(0, Integer::sum);
    final int minLoadedSegments = totalTargetReplicas > 1 ? 2 : 1;

    // Drop segment from unneeded tiers if requirement is met across target tiers
    int loadedTargetReplicas = 0;
    final Set<String> targetTiers = tierToReplicaCount.keySet();
    for (String tier : targetTiers) {
      loadedTargetReplicas += replicantLookup.getLoadedReplicants(segment.getId(), tier);
    }
    if (loadedTargetReplicas < minLoadedSegments) {
      return;
    }

    final Set<String> dropTiers = Sets.newHashSet(cluster.getTierNames());
    dropTiers.removeAll(targetTiers);
    for (String dropTier : dropTiers) {
      updateReplicasOnTier(segment, dropTier, 0);
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
        && stateManager.loadSegment(segment, server, true)) {
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

  private void updateReplicasOnTier(DataSegment segment, String tier, int targetCount)
  {
    final Map<SegmentState, List<ServerHolder>> serversByState = new EnumMap<>(SegmentState.class);
    Arrays.stream(SegmentState.values())
          .forEach(state -> serversByState.put(state, new ArrayList<>()));

    final Set<ServerHolder> historicals = cluster.getHistoricalsByTier(tier);
    if (historicals == null || historicals.isEmpty()) {
      log.makeAlert("Tier [%s] has no servers! Check your cluster configuration.", tier).emit();
      return;
    }

    historicals.forEach(
        serverHolder -> serversByState
            .get(serverHolder.getSegmentState(segment))
            .add(serverHolder)
    );

    final int currentCount = serversByState.get(SegmentState.LOADED).size()
                             + serversByState.get(SegmentState.LOADING).size();
    if (targetCount == currentCount) {
      return;
    }

    final int movingCount = serversByState.get(SegmentState.MOVING_TO).size();
    if (targetCount == 0 && movingCount > 0) {
      // Cancel the segment balancing moves, if any
      int cancelledMoves = cancelOperations(
          SegmentState.MOVING_TO,
          segment,
          serversByState.get(SegmentState.MOVING_TO),
          movingCount
      );
      stats.addToTieredStat(CoordinatorStats.CANCELLED_MOVES, tier, cancelledMoves);
    }

    if (targetCount > currentCount) {
      cancelDropOrLoadReplicas(targetCount - currentCount, segment, tier, serversByState);
    } else {
      cancelLoadOrDropReplicas(currentCount - targetCount, segment, tier, serversByState);
    }
  }

  private void cancelDropOrLoadReplicas(
      int numReplicasToLoad,
      DataSegment segment,
      String tier,
      Map<SegmentState, List<ServerHolder>> serversByState
  )
  {
    final int cancelledDrops = cancelOperations(
        SegmentState.DROPPING,
        segment,
        serversByState.get(SegmentState.DROPPING),
        numReplicasToLoad
    );

    numReplicasToLoad -= cancelledDrops;
    int totalReplicas = serversByState.get(SegmentState.LOADED).size()
                        + serversByState.get(SegmentState.LOADING).size()
                        + cancelledDrops;
    boolean primaryExists = totalReplicas > 0;
    if (numReplicasToLoad > 0) {
      int successfulLoadsQueued = loadReplicas(
          numReplicasToLoad,
          segment,
          serversByState.get(SegmentState.NONE),
          primaryExists
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

    numReplicasToDrop -= cancelledLoads;
    if (numReplicasToDrop > 0) {
      final int successfulDropsQueued = dropReplicas(
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
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(int numToDrop, DataSegment segment, List<ServerHolder> eligibleServers)
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
    if (numToDrop > numDropsQueued) {
      remainingNumToDrop = numToDrop - numDropsQueued;
      Iterator<ServerHolder> serverIterator =
          eligibleLiveServers.size() >= remainingNumToDrop
          ? eligibleLiveServers.iterator()
          : strategy.pickServersToDrop(segment, eligibleLiveServers);
      numDropsQueued += dropReplicas(remainingNumToDrop, segment, serverIterator);
    }
    if (numToDrop > numDropsQueued) {
      log.warn("Queued only %d of %d drops of segment [%s].", numDropsQueued, numToDrop, segment.getId());
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
   * Returns the number of successfully queued load operations.
   */
  private int loadReplicas(
      int numToLoad,
      DataSegment segment,
      List<ServerHolder> candidateServers,
      boolean primaryExists
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
    if (!primaryExists) {
      numLoadsQueued += stateManager.loadSegment(segment, serverIterator.next(), true) ? 1 : 0;
    }

    // Load the remaining replicas
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      numLoadsQueued += stateManager.loadSegment(segment, serverIterator.next(), false) ? 1 : 0;
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
