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
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerSegmentHolder;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentStateManager;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 *
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
          "Skipping balance for tier [%s] as it still has [%,d] segments in queue with lifetime [%d / %d].",
          tier,
          numMovingSegments,
          movingState.getLifetime(),
          maxLifetime
      );

      // Create alerts for stuck tiers
      if (movingState.getLifetime() <= 0) {
        log.makeAlert("Balancing queue for tier [%s] has [%d] segments stuck.", tier, numMovingSegments)
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
    if (maxSegmentsToMove == 0) {
      log.info("Skipping balance as maxSegmentsToMove is [%d].", maxSegmentsToMove);
      return params;
    } else if (maxSegmentsToMove < 0) {
      log.info("Auto-calculating maxSegmentsToMove for every tier since configured value is [%d].", maxSegmentsToMove);
    }

    final CoordinatorRunStats stats = new CoordinatorRunStats();
    final SegmentLoader loader = new SegmentLoader(
        stateManager,
        params.getDruidCluster(),
        params.getSegmentReplicantLookup(),
        params.getReplicationManager(),
        params.getBalancerStrategy(),
        params.getCoordinatorDynamicConfig().isUseRoundRobinSegmentAssignment()
    );

    final Set<String> busyTiers = reduceLifetimesAndGetBusyTiers(
        params.getCoordinatorDynamicConfig().getReplicantLifetime()
    );
    params.getDruidCluster().getHistoricals().forEach((tier, servers) -> {
      if (!busyTiers.contains(tier)) {
        balanceTier(params, tier, servers, stats, loader);
      }
    });

    loader.makeAlerts();
    stats.accumulate(loader.getStats());

    stats.forEachRowKey(
        (dimensionValues, statValues) ->
            log.info("Stats for dimensions [%s] are [%s]", dimensionValues, statValues)
    );

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  private void balanceTier(
      DruidCoordinatorRuntimeParams params,
      String tier,
      SortedSet<ServerHolder> servers,
      CoordinatorRunStats stats,
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
        "Balancing segments in tier [%s] with [%d] active servers, [%d] decommissioning servers.",
        tier,
        activeServers.size(),
        decommissioningServers.size()
    );

    if ((decommissioningServers.isEmpty() && activeServers.size() <= 1) || activeServers.isEmpty()) {
      log.warn("Skipping balance for tier [%s] as there are insufficient active servers.", tier);
      // suppress emit zero stats
      return;
    }

    final int maxSegmentsToMove = calculateMaxSegmentsToMoveInTier(tier, servers, params);
    if (maxSegmentsToMove <= 0) {
      return;
    }

    // Prioritize moving segments from decomissioning servers.
    int decommissioningMaxPercentOfMaxSegmentsToMove =
        params.getCoordinatorDynamicConfig().getDecommissioningMaxPercentOfMaxSegmentsToMove();
    int maxSegmentsToMoveFromDecommissioningNodes =
        (int) Math.ceil(maxSegmentsToMove * (decommissioningMaxPercentOfMaxSegmentsToMove / 100.0));
    log.info(
        "Processing [%d] segments for moving from decommissioning servers.",
        maxSegmentsToMoveFromDecommissioningNodes
    );
    Pair<Integer, Integer> decommissioningResult =
        balanceServers(
            params,
            decommissioningServers,
            activeServers,
            maxSegmentsToMoveFromDecommissioningNodes,
            loader
        );

    // After moving segments from decomissioning servers, move the remaining segments from the rest of the servers.
    int maxGeneralSegmentsToMove = maxSegmentsToMove - decommissioningResult.lhs;
    log.info("Processing [%d] segments for balancing between active servers", maxGeneralSegmentsToMove);
    Pair<Integer, Integer> generalResult =
        balanceServers(params, activeServers, activeServers, maxGeneralSegmentsToMove, loader);

    int moved = generalResult.lhs + decommissioningResult.lhs;
    int unmoved = generalResult.rhs + decommissioningResult.rhs;
    if (unmoved == maxSegmentsToMove) {
      // Cluster should be alive and constantly adjusting
      log.info("No good moves found in tier [%s]", tier);
    }
    stats.addForTier(Stats.Segments.UNMOVED, tier, unmoved);
    stats.addForTier(Stats.Segments.MOVED, tier, moved);

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
    if (maxSegmentsToMove <= 0 || toMoveFrom.isEmpty() || toMoveTo.isEmpty()) {
      return new Pair<>(0, 0);
    }

    final Iterator<BalancerSegmentHolder> segmentsToMove;
    final BalancerStrategy strategy = params.getBalancerStrategy();
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

    int moved = 0, unmoved = 0;
    while (segmentsToMove.hasNext() && (moved + unmoved) < maxSegmentsToMove) {
      final BalancerSegmentHolder segmentToMoveHolder = segmentsToMove.next();

      // DruidCoordinatorRuntimeParams.getUsedSegments originate from SegmentsMetadataManager, i. e. that's a set of segments
      // that *should* be loaded. segmentToMoveHolder.getSegment originates from ServerInventoryView,  i. e. that may be
      // any segment that happens to be loaded on some server, even if it is not used. (Coordinator closes such
      // discrepancies eventually via UnloadUnusedSegments). Therefore the picked segmentToMoveHolder's segment may not
      // need to be balanced.
      final DataSegment segmentToMove = getLoadableSegment(segmentToMoveHolder.getSegment(), params);
      if (segmentToMove != null) {
        // Keep the current server in the eligible list but filter out
        // servers already serving a replica or having a full load queue
        final ServerHolder source = segmentToMoveHolder.getServer();
        final List<ServerHolder> eligibleDestinationServers =
            toMoveTo.stream()
                    .filter(
                        s -> s.getServer().equals(source.getServer())
                             || s.canLoadSegment(segmentToMove))
                    .collect(Collectors.toList());

        if (eligibleDestinationServers.isEmpty()) {
          log.debug("No valid movement destinations for segment [%s].", segmentToMove.getId());
          unmoved++;
        } else {
          final ServerHolder destination =
              strategy.findNewSegmentHomeBalancer(segmentToMove, eligibleDestinationServers);

          if (destination == null || destination.getServer().equals(source.getServer())) {
            log.debug("Segment [%s] is already 'optimally' placed.", segmentToMove.getId());
            unmoved++;
          } else if (moveSegment(segmentToMove, source, destination, loader)) {
            moved++;
          } else {
            unmoved++;
          }
        }
      } else {
        unmoved++;
      }
    }
    return new Pair<>(moved, unmoved);
  }

  protected boolean moveSegment(
      DataSegment segment,
      ServerHolder fromServer,
      ServerHolder toServer,
      SegmentLoader loader
  )
  {
    try {
      return loader.moveSegment(segment, fromServer, toServer);
    }
    catch (Exception e) {
      log.makeAlert(e, "[%s] : Moving exception", segment.getId()).emit();
      return false;
    }
  }

  private int calculateMaxSegmentsToMoveInTier(
      String tier,
      Set<ServerHolder> serversInTier,
      DruidCoordinatorRuntimeParams params
  )
  {
    int numSegmentsInTier = 0;
    for (ServerHolder sourceHolder : serversInTier) {
      numSegmentsInTier += sourceHolder.getServer().getNumSegments()
                           + sourceHolder.getPeon().getNumberOfSegmentsToLoad();
    }

    final int configuredMaxSegmentsToMove =
        params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();
    final int calculatedMaxSegmentsToMove;
    if (configuredMaxSegmentsToMove < 0) {
      calculatedMaxSegmentsToMove = (int) (numSegmentsInTier / 100.0);
    } else {
      calculatedMaxSegmentsToMove = Math.min(configuredMaxSegmentsToMove, numSegmentsInTier);
    }

    log.info(
        "Tier [%s]: Number of loaded + loading segments=[%d],"
        + " configured maxSegmentsToMove=[%d], calculated maxSegmentsToMove=[%d]",
        tier,
        numSegmentsInTier,
        configuredMaxSegmentsToMove,
        calculatedMaxSegmentsToMove
    );
    return calculatedMaxSegmentsToMove;
  }

  /**
   * Returns a DataSegment with the correct value of loadSpec (as obtained from
   * metadata store). This method may return null if there is no snapshot available
   * for the underlying datasource or if the segment is unused.
   */
  private DataSegment getLoadableSegment(DataSegment segmentToMove, DruidCoordinatorRuntimeParams params)
  {
    final SegmentId segmentId = segmentToMove.getId();
    if (!params.getUsedSegments().contains(segmentToMove)) {
      log.warn("Not moving segment [%s] because it is an unused segment.", segmentId);
      return null;
    }

    ImmutableDruidDataSource datasource = params.getDataSourcesSnapshot().getDataSource(segmentToMove.getDataSource());
    if (datasource == null) {
      log.warn("Not moving segment [%s] because datasource snapshot does not exist.", segmentId);
      return null;
    }

    DataSegment loadableSegment = datasource.getSegment(segmentId);
    if (loadableSegment == null) {
      log.warn("Not moving segment [%s] as its metadata could not be found.", segmentId);
      return null;
    }

    return loadableSegment;
  }
}
