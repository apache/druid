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
  private final int maxLoadQueueSize;

  public SegmentLoader(SegmentStateManager stateManager, DruidCoordinatorRuntimeParams runParams)
  {
    this.stateManager = stateManager;
    this.strategy = runParams.getBalancerStrategy();
    this.cluster = runParams.getDruidCluster();
    this.replicantLookup = runParams.getSegmentReplicantLookup();
    this.maxLoadQueueSize = runParams.getCoordinatorDynamicConfig()
                                     .getMaxSegmentsInNodeLoadingQueue();
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
        || !canLoadSegment(toServer, segment)) {
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
    final Set<String> targetTiers = tierToReplicaCount.keySet();
    for (String tier : targetTiers) {
      int numReplicas = tierToReplicaCount.get(tier);
      stats.addToTieredStat(Metrics.REQUIRED_CAPACITY, tier, segment.getSize() * numReplicas);
      updateReplicasOnTier(segment, tier, tierToReplicaCount.get(tier));
    }

    // Find the minimum number of segments required for fault tolerance
    final int totalTargetReplicas = tierToReplicaCount.values().stream()
                                                      .reduce(0, Integer::sum);
    final int minLoadedSegments = totalTargetReplicas > 1 ? 2 : 1;

    // Drop segment from unneeded tiers if requirement is met across target tiers
    int loadedTargetReplicas = 0;
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
    int broadcastLoadCount = 0;
    int broadcastDropCount = 0;
    for (ServerHolder server : cluster.getAllServers()) {
      if (!server.getServer().getType().isSegmentBroadcastTarget()) {
        // ignore this server
      } else if (server.isDecommissioning()) {
        broadcastDropCount += dropBroadcastSegment(segment, server) ? 1 : 0;
      } else {
        broadcastLoadCount += loadBroadcastSegment(segment, server) ? 1 : 0;
      }
    }

    if (broadcastLoadCount > 0) {
      stats.addToDataSourceStat(Metrics.BROADCAST_LOADS, segment.getDataSource(), broadcastLoadCount);
      log.debug("Broadcast load of segment [%s] to [%d] servers.", segment.getId(), broadcastLoadCount);
    }
    if (broadcastDropCount > 0) {
      stats.addToDataSourceStat(Metrics.BROADCAST_DROPS, segment.getDataSource(), broadcastDropCount);
      log.debug("Broadcast drop of segment [%s] to [%d] servers.", segment.getId(), broadcastDropCount);
    }
  }

  /**
   * Marks the given segment as unused.
   */
  public void deleteSegment(DataSegment segment)
  {
    stateManager.deleteSegment(segment);
    stats.addToGlobalStat(Metrics.DELETED_SEGMENTS, 1);
  }

  /**
   * Checks if the server can load the given segment.
   * <p>
   * A load is possible only if the server meets all of the following criteria:
   * <ul>
   *   <li>is not already serving or loading the segment</li>
   *   <li>is not being decommissioned</li>
   *   <li>has not already exceeded the load queue limit in this run</li>
   *   <li>has available disk space</li>
   * </ul>
   */
  public boolean canLoadSegment(ServerHolder server, DataSegment segment)
  {
    return server.canLoadSegment(segment)
           && (maxLoadQueueSize == 0 || maxLoadQueueSize > server.getSegmentsQueuedForLoad());
  }

  private boolean loadBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    final SegmentState state = server.getSegmentState(segment);
    if (state == SegmentState.LOADED || state == SegmentState.LOADING) {
      return false;
    }

    boolean dropCancelled = state == SegmentState.DROPPING
                            && stateManager.cancelOperation(SegmentState.DROPPING, segment, server);

    final String tier = server.getServer().getTier();
    if (dropCancelled) {
      stats.addToTieredStat(Metrics.CANCELLED_DROPS, tier, 1);
    } else if (canLoadSegment(server, segment)
               && stateManager.loadSegment(segment, server, false)) {
      stats.addToTieredStat(Metrics.QUEUED_LOADS, tier, 1);
    } else {
      log.makeAlert("Failed to broadcast segment for [%s]", segment.getDataSource())
         .addData("segmentId", segment.getId())
         .addData("segmentSize", segment.getSize())
         .addData("hostName", server.getServer().getHost())
         .addData("availableSize", server.getAvailableSize())
         .emit();
      return false;
    }

    return true;
  }

  private boolean dropBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    final SegmentState state = server.getSegmentState(segment);
    if (state == SegmentState.NONE || state == SegmentState.DROPPING) {
      return false;
    }

    boolean loadCancelled = state == SegmentState.LOADING
                            && stateManager.cancelOperation(SegmentState.LOADING, segment, server);

    final String tier = server.getServer().getTier();
    if (loadCancelled) {
      stats.addToTieredStat(Metrics.CANCELLED_LOADS, tier, 1);
    } else if (stateManager.dropSegment(segment, server)) {
      stats.addToTieredStat(Metrics.QUEUED_DROPS, tier, 1);
    } else {
      return false;
    }

    return true;
  }

  private void updateReplicasOnTier(DataSegment segment, String tier, int targetCount)
  {
    final Map<SegmentState, List<ServerHolder>> serversByState = new EnumMap<>(SegmentState.class);
    Arrays.stream(SegmentState.values())
          .forEach(state -> serversByState.put(state, new ArrayList<>()));

    Set<ServerHolder> historicals = cluster.getHistoricalsByTier(tier);
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
      stats.addToTieredStat(Metrics.CANCELLED_MOVES, tier, cancelledMoves);
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
    stats.addToTieredStat(Metrics.CANCELLED_DROPS, tier, cancelledDrops);

    numReplicasToLoad -= cancelledDrops;
    int numLoaded = serversByState.get(SegmentState.LOADED).size();
    boolean primaryExists = numLoaded + cancelledDrops > 0;
    if (numReplicasToLoad > 0) {
      int successfulLoadsQueued = loadReplicas(
          numReplicasToLoad,
          segment,
          serversByState.get(SegmentState.NONE),
          primaryExists
      );

      stats.addToTieredStat(Metrics.QUEUED_LOADS, tier, successfulLoadsQueued);
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
    final int cancelledLoads = cancelOperations(
        SegmentState.LOADING,
        segment,
        serversByState.get(SegmentState.LOADING),
        numReplicasToDrop
    );
    stats.addToTieredStat(Metrics.CANCELLED_LOADS, tier, cancelledLoads);

    numReplicasToDrop -= cancelledLoads;
    if (numReplicasToDrop > 0) {
      final int successfulDropsQueued = dropReplicas(
          numReplicasToDrop,
          segment,
          serversByState.get(SegmentState.LOADED)
      );

      stats.addToTieredStat(Metrics.QUEUED_DROPS, tier, successfulDropsQueued);
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
                        .filter(server -> canLoadSegment(server, segment))
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
