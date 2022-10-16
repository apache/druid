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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerSegmentHolder;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentStateManager;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 */
public class BalanceSegments implements CoordinatorDuty
{
  protected static final EmittingLogger log = new EmittingLogger(BalanceSegments.class);
  private final SegmentStateManager stateManager;

  public BalanceSegments(SegmentStateManager stateManager)
  {
    this.stateManager = stateManager;
  }

  /**
   * Reduces the lifetimes of segments currently being moved in all the tiers.
   * Returns the set of tiers that are currently moving some segments and won't be
   * eligible for assigning more balancing moves in this run.
   */
  private Set<String> reduceLifetimesAndGetBusyTiers(int maxLifetime)
  {
    final Set<String> busyTiers = new HashSet<>();
    stateManager.reduceLifetimesOfMovingSegments().forEach((tier, movingState) -> {
      int numMovingSegments = movingState.getNumProcessingSegments();
      if (numMovingSegments <= 0) {
        return;
      }

      busyTiers.add(tier);
      log.info(
          "Skipping balance for tier [%s] as it still has %,d segments in queue with lifetime [%d / %d].",
          tier,
          numMovingSegments,
          movingState.getLifetime(),
          maxLifetime
      );

      // Create alerts for stuck tiers
      if (movingState.getLifetime() <= 0) {
        log.makeAlert("Balancing queue for tier [%s] has [%d] segments stuck .", tier, numMovingSegments)
           .addData("segments", movingState.getCurrentlyProcessingSegmentsAndHosts())
           .emit();
      }
    });

    return busyTiers;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (params.getUsedSegments().isEmpty()) {
      log.info("Skipping balance as there are no used segments.");
      return params;
    }
    int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();
    if (maxSegmentsToMove <= 0) {
      log.info("Skipping balance as maxSegmentsToMove is %d.", maxSegmentsToMove);
      return params;
    }

    final CoordinatorStats stats = new CoordinatorStats();
    final SegmentLoader loader = new SegmentLoader(
        stateManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        params.getReplicationManager(),
        params.getBalancerStrategy()
    );

    final Set<String> busyTiers = reduceLifetimesAndGetBusyTiers(
        params.getCoordinatorDynamicConfig().getReplicantLifetime()
    );
    params.getDruidCluster().getHistoricals().forEach((tier, servers) -> {
      if (!busyTiers.contains(tier)) {
        balanceTier(params, tier, servers, stats, loader);
      }
    });

    stats.accumulate(loader.getStats());
    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private void balanceTier(
      DruidCoordinatorRuntimeParams params,
      String tier,
      SortedSet<ServerHolder> servers,
      CoordinatorStats stats,
      SegmentLoader loader
  )
  {
    /*
      Take as many segments from decommissioning servers as decommissioningMaxPercentOfMaxSegmentsToMove allows and find
      the best location for them on active servers. After that, balance segments within active servers pool.
     */
    Map<Boolean, List<ServerHolder>> partitions =
        servers.stream().collect(Collectors.partitioningBy(ServerHolder::isDecommissioning));
    final List<ServerHolder> decommissioningServers = partitions.get(true);
    final List<ServerHolder> activeServers = partitions.get(false);
    log.info(
        "Balancing segments in tier [%s] with %d active servers, %d decommissioning servers",
        tier,
        activeServers.size(),
        decommissioningServers.size()
    );

    if ((decommissioningServers.isEmpty() && activeServers.size() <= 1) || activeServers.isEmpty()) {
      log.warn("Skipping balance for tier [%s] as there are insufficient active servers.", tier);
      // suppress emit zero stats
      return;
    }

    int numSegments = 0;
    for (ServerHolder sourceHolder : servers) {
      numSegments += sourceHolder.getServer().getNumSegments();
    }

    if (numSegments == 0) {
      log.info("Skipping balance for tier [%s] as there are no served segments.", tier);
      // suppress emit zero stats
      return;
    }

    final int maxSegmentsToMove = Math.min(params.getCoordinatorDynamicConfig().getMaxSegmentsToMove(), numSegments);

    // Prioritize moving segments from decomissioning servers.
    int decommissioningMaxPercentOfMaxSegmentsToMove =
        params.getCoordinatorDynamicConfig().getDecommissioningMaxPercentOfMaxSegmentsToMove();
    int maxSegmentsToMoveFromDecommissioningNodes =
        (int) Math.ceil(maxSegmentsToMove * (decommissioningMaxPercentOfMaxSegmentsToMove / 100.0));
    log.info(
        "Processing %d segments for moving from decommissioning servers",
        maxSegmentsToMoveFromDecommissioningNodes
    );
    Pair<Integer, Integer> decommissioningResult =
        balanceServers(params, decommissioningServers, activeServers, maxSegmentsToMoveFromDecommissioningNodes, loader);

    // After moving segments from decomissioning servers, move the remaining segments from the rest of the servers.
    int maxGeneralSegmentsToMove = maxSegmentsToMove - decommissioningResult.lhs;
    log.info("Processing %d segments for balancing between active servers", maxGeneralSegmentsToMove);
    Pair<Integer, Integer> generalResult =
        balanceServers(params, activeServers, activeServers, maxGeneralSegmentsToMove, loader);

    int moved = generalResult.lhs + decommissioningResult.lhs;
    int unmoved = generalResult.rhs + decommissioningResult.rhs;
    if (unmoved == maxSegmentsToMove) {
      // Cluster should be alive and constantly adjusting
      log.info("No good moves found in tier [%s]", tier);
    }
    stats.addToTieredStat("unmovedCount", tier, unmoved);
    stats.addToTieredStat("movedCount", tier, moved);

    if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
      final BalancerStrategy strategy = params.getBalancerStrategy();
      strategy.emitStats(tier, stats, Lists.newArrayList(servers));
    }
    log.info("[%s]: Segments Moved: [%d] Segments Let Alone: [%d]", tier, moved, unmoved);
  }

  private Pair<Integer, Integer> balanceServers(
      DruidCoordinatorRuntimeParams params,
      List<ServerHolder> toMoveFrom,
      List<ServerHolder> toMoveTo,
      int maxSegmentsToMove,
      SegmentLoader loader
  )
  {
    if (maxSegmentsToMove <= 0) {
      log.debug("maxSegmentsToMove is 0; no balancing work can be performed.");
      return new Pair<>(0, 0);
    } else if (toMoveFrom.isEmpty()) {
      log.debug("toMoveFrom is empty; no balancing work can be performed.");
      return new Pair<>(0, 0);
    } else if (toMoveTo.isEmpty()) {
      log.debug("toMoveTo is empty; no balancing work can be peformed.");
      return new Pair<>(0, 0);
    }

    final BalancerStrategy strategy = params.getBalancerStrategy();
    final int maxIterations = 2 * maxSegmentsToMove;
    int moved = 0, unmoved = 0;

    Iterator<BalancerSegmentHolder> segmentsToMove;
    // The pick method depends on if the operator has enabled batched segment sampling in the Coorinator dynamic config.
    if (params.getCoordinatorDynamicConfig().useBatchedSegmentSampler()) {
      segmentsToMove = strategy.pickSegmentsToMove(
          toMoveFrom,
          params.getBroadcastDatasources(),
          maxSegmentsToMove
      );
    } else {
      segmentsToMove = strategy.pickSegmentsToMove(
          toMoveFrom,
          params.getBroadcastDatasources(),
          params.getCoordinatorDynamicConfig().getPercentOfSegmentsToConsiderPerMove()
      );
    }

    //noinspection ForLoopThatDoesntUseLoopVariable
    for (int iter = 0; (moved + unmoved) < maxSegmentsToMove; ++iter) {
      if (!segmentsToMove.hasNext()) {
        log.info("All servers to move segments from are empty, ending run.");
        break;
      }
      final BalancerSegmentHolder segmentToMoveHolder = segmentsToMove.next();

      // DruidCoordinatorRuntimeParams.getUsedSegments originate from SegmentsMetadataManager, i. e. that's a set of segments
      // that *should* be loaded. segmentToMoveHolder.getSegment originates from ServerInventoryView,  i. e. that may be
      // any segment that happens to be loaded on some server, even if it is not used. (Coordinator closes such
      // discrepancies eventually via UnloadUnusedSegments). Therefore the picked segmentToMoveHolder's segment may not
      // need to be balanced.
      boolean needToBalancePickedSegment = params.getUsedSegments().contains(segmentToMoveHolder.getSegment());
      if (needToBalancePickedSegment) {
        final DataSegment segmentToMove = segmentToMoveHolder.getSegment();
        final ImmutableDruidServer fromServer = segmentToMoveHolder.getFromServer().getServer();
        // we want to leave the server the segment is currently on in the list...
        // but filter out replicas that are already serving the segment, and servers with a full load queue
        final List<ServerHolder> toMoveToWithLoadQueueCapacityAndNotServingSegment =
            toMoveTo.stream()
                    .filter(s -> s.getServer().equals(fromServer)
                                 || s.canLoadSegment(segmentToMove))
                    .collect(Collectors.toList());

        if (toMoveToWithLoadQueueCapacityAndNotServingSegment.size() > 0) {
          final ServerHolder destinationHolder =
              strategy.findNewSegmentHomeBalancer(segmentToMove, toMoveToWithLoadQueueCapacityAndNotServingSegment);

          if (destinationHolder != null && !destinationHolder.getServer().equals(fromServer)) {
            if (moveSegment(segmentToMoveHolder, destinationHolder, loader)) {
              moved++;
            } else {
              unmoved++;
            }
          } else {
            log.debug("Segment [%s] is 'optimally' placed.", segmentToMove.getId());
            unmoved++;
          }
        } else {
          log.debug("No valid movement destinations for segment [%s].", segmentToMove.getId());
          unmoved++;
        }
      }
      if (iter >= maxIterations) {
        log.info(
            "Unable to select %d remaining candidate segments out of %d total to balance "
            + "after %d iterations, ending run.",
            (maxSegmentsToMove - moved - unmoved),
            maxSegmentsToMove,
            iter
        );
        break;
      }
    }
    return new Pair<>(moved, unmoved);
  }

  protected boolean moveSegment(
      final BalancerSegmentHolder segmentHolder,
      final ServerHolder toServer,
      final SegmentLoader loader
  )
  {
    try {
      return loader.moveSegment(
          segmentHolder.getSegment(),
          segmentHolder.getFromServer(),
          toServer
      );
    }
    catch (Exception e) {
      log.makeAlert(e, "[%s] : Moving exception", segmentHolder.getSegment().getId()).emit();
      return false;
    }
  }
}
