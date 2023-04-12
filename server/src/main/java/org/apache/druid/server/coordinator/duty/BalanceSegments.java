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
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerSegmentHolder;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentStateManager;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
   * Raises alerts for segments stuck in the queue.
   */
  private void reduceLifetimesAndAlert()
  {
    stateManager.reduceLifetimesOfMovingSegments().forEach((tier, movingState) -> {
      int numMovingSegments = movingState.getNumProcessingSegments();
      if (numMovingSegments <= 0) {
        return;
      }

      // Create alerts for stuck tiers
      if (movingState.getMinLifetime() <= 0) {
        log.makeAlert(
            "Balancing queue for tier [%s] has [%d] segments stuck.",
            tier,
            movingState.getNumExpiredSegments()
        ).addData("segments", movingState.getExpiredSegments()).emit();
      }
    });
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    reduceLifetimesAndAlert();

    if (params.getUsedSegments().isEmpty()) {
      log.info("Skipping balance as there are no used segments.");
      return params;
    }

    final DruidCluster cluster = params.getDruidCluster();
    final int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();
    if (maxSegmentsToMove <= 0) {
      log.info("Skipping balance as maxSegmentsToMove is [%d].", maxSegmentsToMove);
      return params;
    } else {
      int maxLifetime = params.getCoordinatorDynamicConfig().getReplicantLifetime();
      log.info(
          "Balancing segments in tiers [%s] with maxSegmentsToMove=[%d], maxLifetime=[%d].",
          cluster.getTierNames(), maxSegmentsToMove, maxLifetime
      );
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

    cluster.getHistoricals().forEach(
        (tier, servers) -> balanceTier(params, tier, servers, stats, loader)
    );

    loader.makeAlerts();
    stats.accumulate(loader.getStats());

    stats.forEachRowKey(
        (dimensionValues, statValues) ->
            log.info("Stats for dimensions [%s] are [%s]", dimensionValues, statValues)
    );

    return params.buildFromExisting().withCoordinatorStats(stats).build();
  }

  /**
   * Moves as many segments as possible from decommissioning to active servers,
   * then moves segments amongst active servers.
   */
  private void balanceTier(
      DruidCoordinatorRuntimeParams params,
      String tier,
      SortedSet<ServerHolder> servers,
      CoordinatorRunStats stats,
      SegmentLoader loader
  )
  {
    Map<Boolean, List<ServerHolder>> partitions =
        servers.stream().collect(Collectors.partitioningBy(ServerHolder::isDecommissioning));
    final List<ServerHolder> decommissioningServers = partitions.get(true);
    final List<ServerHolder> activeServers = partitions.get(false);

    log.info(
        "Balancing segments in tier [%s] with [%d] activeServers and [%d] decommissioning servers.",
        tier, activeServers.size(), decommissioningServers.size()
    );

    if ((decommissioningServers.isEmpty() && activeServers.size() <= 1) || activeServers.isEmpty()) {
      log.warn("Skipping balance for tier [%s] as there are [%d] active servers.", tier, activeServers.size());
      return;
    }

    final int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();
    if (maxSegmentsToMove <= 0) {
      return;
    }

    // Move segments from decommissioning to active servers
    int maxDecommPercentToMove = params.getCoordinatorDynamicConfig()
                                       .getDecommissioningMaxPercentOfMaxSegmentsToMove();
    int maxDecommSegmentsToMove = (int) Math.ceil(maxSegmentsToMove * (maxDecommPercentToMove / 100.0));

    log.info("Moving [%d] segments from decommissioning servers to active servers.", maxDecommSegmentsToMove);
    final MoveStats decommissioningResult =
        balanceServers(params, decommissioningServers, activeServers, maxDecommSegmentsToMove, loader);

    // Move segments amongst active servers
    int maxGeneralSegmentsToMove = maxSegmentsToMove - decommissioningResult.movedCount;
    log.info("Moving [%d] segments between active servers", maxGeneralSegmentsToMove);
    final MoveStats generalResult =
        balanceServers(params, activeServers, activeServers, maxGeneralSegmentsToMove, loader);

    int totalMoved = generalResult.movedCount + decommissioningResult.movedCount;
    int totalUnmoved = generalResult.unmovedCount + decommissioningResult.unmovedCount;
    stats.addToTieredStat(Stats.Segments.UNMOVED, tier, totalUnmoved);
    stats.addToTieredStat(Stats.Segments.MOVED, tier, totalMoved);
    log.info("In tier [%s], moved [%d] segments and left [%d] segments unmoved.", tier, totalMoved, totalUnmoved);

    if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
      final BalancerStrategy strategy = params.getBalancerStrategy();
      strategy.emitStats(tier, stats, Lists.newArrayList(servers));
    }
  }

  private MoveStats balanceServers(
      DruidCoordinatorRuntimeParams params,
      List<ServerHolder> toMoveFrom,
      List<ServerHolder> toMoveTo,
      int maxSegmentsToMove,
      SegmentLoader loader
  )
  {
    if (maxSegmentsToMove <= 0 || toMoveFrom.isEmpty() || toMoveTo.isEmpty()) {
      return new MoveStats();
    }

    final BalancerStrategy strategy = params.getBalancerStrategy();
    if (params.getCoordinatorDynamicConfig().useBatchedSegmentSampler()) {
      // First try to move loading segments as moving them is faster
      Iterator<BalancerSegmentHolder> pickedLoadingSegments = strategy.pickSegmentsToMove(
          toMoveFrom,
          params.getBroadcastDatasources(),
          maxSegmentsToMove,
          true
      );
      final MoveStats loadingSegmentStats =
          moveSegments(pickedLoadingSegments, toMoveTo, maxSegmentsToMove, loader, params);

      // Then try to move loaded segments
      final int numLoadedSegmentsToMove = maxSegmentsToMove - loadingSegmentStats.movedCount;
      if (numLoadedSegmentsToMove > 0) {
        Iterator<BalancerSegmentHolder> pickedLoadedSegments = strategy.pickSegmentsToMove(
            toMoveFrom,
            params.getBroadcastDatasources(),
            numLoadedSegmentsToMove,
            false
        );
        final MoveStats loadedSegmentStats =
            moveSegments(pickedLoadedSegments, toMoveTo, numLoadedSegmentsToMove, loader, params);
        loadingSegmentStats.movedCount += loadedSegmentStats.movedCount;
        loadingSegmentStats.unmovedCount += loadedSegmentStats.unmovedCount;
      }

      return loadingSegmentStats;
    } else {
      Iterator<BalancerSegmentHolder> segmentsToMove = strategy.pickSegmentsToMove(
          toMoveFrom,
          params.getBroadcastDatasources(),
          params.getCoordinatorDynamicConfig().getPercentOfSegmentsToConsiderPerMove()
      );

      return moveSegments(segmentsToMove, toMoveTo, maxSegmentsToMove, loader, params);
    }
  }

  private MoveStats moveSegments(
      Iterator<BalancerSegmentHolder> segmentsToMove,
      List<ServerHolder> toMoveTo,
      int maxSegmentsToMove,
      SegmentLoader loader,
      DruidCoordinatorRuntimeParams params
  )
  {
    final BalancerStrategy strategy = params.getBalancerStrategy();

    final MoveStats stats = new MoveStats();
    while (segmentsToMove.hasNext() && stats.processed() < maxSegmentsToMove) {
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
          stats.unmovedCount++;
        } else {
          final ServerHolder destination =
              strategy.findNewSegmentHomeBalancer(segmentToMove, eligibleDestinationServers);

          if (destination == null || destination.getServer().equals(source.getServer())) {
            log.debug("Segment [%s] is already 'optimally' placed.", segmentToMove.getId());
            stats.unmovedCount++;
          } else if (moveSegment(segmentToMove, source, destination, loader)) {
            stats.movedCount++;
          } else {
            stats.unmovedCount++;
          }
        }
      } else {
        stats.unmovedCount++;
      }
    }
    return stats;
  }

  private boolean moveSegment(
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
      log.makeAlert(e, "Exception while moving segment [%s]", segment.getId()).emit();
      return false;
    }
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

  private static class MoveStats
  {
    int movedCount;
    int unmovedCount;

    int processed()
    {
      return movedCount + unmovedCount;
    }
  }
}
