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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Balances segments within the servers of a tier using the balancer strategy.
 * Segments are prioritized for move in the following order:
 * <ul>
 *   <li>Segments loaded on decommissioning servers</li>
 *   <li>Segments loading on active servers</li>
 *   <li>Segments loaded on active servers</li>
 * </ul>
 */
public class TierSegmentBalancer
{
  private static final EmittingLogger log = new EmittingLogger(TierSegmentBalancer.class);

  private final String tier;
  private final DruidCoordinatorRuntimeParams params;
  private final StrategicSegmentAssigner segmentAssigner;

  private final CoordinatorRunStats runStats;

  private final List<ServerHolder> activeServers;
  private final List<ServerHolder> decommissioningServers;
  private final int maxSegmentsToMove;
  private final int movingSegmentCount;

  public TierSegmentBalancer(
      String tier,
      Set<ServerHolder> servers,
      int maxSegmentsToMove,
      DruidCoordinatorRuntimeParams params
  )
  {
    this.tier = tier;
    this.params = params;
    this.segmentAssigner = params.getSegmentAssigner();
    this.runStats = segmentAssigner.getStats();

    Map<Boolean, List<ServerHolder>> partitions =
        servers.stream().collect(Collectors.partitioningBy(ServerHolder::isDecommissioning));
    this.decommissioningServers = partitions.get(true);
    this.activeServers = partitions.get(false);

    this.movingSegmentCount = activeServers.stream().mapToInt(ServerHolder::getNumMovingSegments).sum();
    this.maxSegmentsToMove = maxSegmentsToMove;
  }

  public void run()
  {
    int numDecommSegmentsToMove = getNumDecommSegmentsToMove(maxSegmentsToMove);
    moveSegmentsFrom(decommissioningServers, numDecommSegmentsToMove, "decommissioning");

    int numActiveSegmentsToMove = getNumActiveSegmentsToMove(maxSegmentsToMove - numDecommSegmentsToMove);
    moveSegmentsFrom(activeServers, numActiveSegmentsToMove, "active");
  }

  /**
   * Moves segments from the given source servers to the active servers in this tier.
   */
  private void moveSegmentsFrom(
      final List<ServerHolder> sourceServers,
      final int numSegmentsToMove,
      final String sourceServerType
  )
  {
    if (numSegmentsToMove <= 0 || sourceServers.isEmpty() || activeServers.isEmpty()) {
      return;
    }

    final Set<String> broadcastDatasources = params.getBroadcastDatasources();

    // Always move loading segments first as it is a cheaper operation
    List<BalancerSegmentHolder> pickedSegments = ReservoirSegmentSampler.pickMovableSegmentsFrom(
        sourceServers,
        numSegmentsToMove,
        ServerHolder::getLoadingSegments,
        broadcastDatasources
    );
    int movedCount = moveSegmentsTo(activeServers, pickedSegments, numSegmentsToMove);

    // Move loaded segments only if tier is not already busy moving segments
    if (movingSegmentCount <= 0) {
      int numLoadedSegmentsToMove = numSegmentsToMove - movedCount;
      pickedSegments = ReservoirSegmentSampler.pickMovableSegmentsFrom(
          sourceServers,
          numLoadedSegmentsToMove,
          server -> server.getServer().iterateAllSegments(),
          broadcastDatasources
      );
      movedCount += moveSegmentsTo(activeServers, pickedSegments, numLoadedSegmentsToMove);
    } else {
      log.info("There are already [%,d] segments moving in tier[%s].", movingSegmentCount, tier);
    }

    log.info(
        "Moved [%,d of %,d] segments from [%d] [%s] servers in tier [%s].",
        movedCount, numSegmentsToMove, sourceServers.size(), sourceServerType, tier
    );
  }

  private int moveSegmentsTo(
      List<ServerHolder> destinationServers,
      List<BalancerSegmentHolder> movableSegments,
      int maxSegmentsToMove
  )
  {
    int processed = 0;
    int movedCount = 0;

    final Iterator<BalancerSegmentHolder> segmentIterator = movableSegments.iterator();
    while (segmentIterator.hasNext() && processed < maxSegmentsToMove) {
      ++processed;

      final BalancerSegmentHolder segmentHolder = segmentIterator.next();
      DataSegment segmentToMove = getLoadableSegment(segmentHolder.getSegment());
      if (segmentToMove != null &&
          segmentAssigner.moveSegment(segmentToMove, segmentHolder.getServer(), destinationServers)) {
        ++movedCount;
      }
    }
    return movedCount;
  }

  /**
   * Returns a DataSegment with the correct value of loadSpec (as obtained from
   * metadata store). This method may return null if there is no snapshot available
   * for the underlying datasource or if the segment is unused.
   */
  @Nullable
  private DataSegment getLoadableSegment(DataSegment segmentToMove)
  {
    if (!params.getUsedSegments().contains(segmentToMove)) {
      markUnmoved("Segment is unused", segmentToMove);
      return null;
    }

    ImmutableDruidDataSource datasource = params.getDataSourcesSnapshot()
                                                .getDataSource(segmentToMove.getDataSource());
    if (datasource == null) {
      markUnmoved("Invalid datasource", segmentToMove);
      return null;
    }

    DataSegment loadableSegment = datasource.getSegment(segmentToMove.getId());
    if (loadableSegment == null) {
      markUnmoved("Invalid segment ID", segmentToMove);
      return null;
    }

    return loadableSegment;
  }

  private void markUnmoved(String reason, DataSegment segment)
  {
    RowKey key = RowKey.with(Dimension.TIER, tier)
                       .with(Dimension.DATASOURCE, segment.getDataSource())
                       .and(Dimension.DESCRIPTION, reason);
    runStats.add(Stats.Segments.MOVE_SKIPPED, key, 1);
  }

  /**
   * Number of segments to move away from the decommissioning historicals of this tier.
   */
  private int getNumDecommSegmentsToMove(int maxSegmentsToMove)
  {
    if (decommissioningServers.isEmpty() || activeServers.isEmpty()) {
      return 0;
    } else {
      final int decommSegmentsToMove = decommissioningServers.stream().mapToInt(
          server -> server.getProjectedSegments().getTotalSegmentCount()
      ).sum();
      return Math.min(decommSegmentsToMove, maxSegmentsToMove);
    }
  }

  /**
   * Number of segments to move between the active historicals of this tier.
   */
  private int getNumActiveSegmentsToMove(int maxActiveSegmentsToMove)
  {
    final CoordinatorDynamicConfig dynamicConfig = params.getCoordinatorDynamicConfig();
    if (activeServers.size() < 2) {
      return 0;
    } else if (dynamicConfig.isSmartSegmentLoading()) {
      return SegmentToMoveCalculator.computeNumSegmentsToMoveInTier(tier, activeServers, maxActiveSegmentsToMove);
    } else {
      // If smartSegmentLoading is disabled, just use the configured value
      return maxActiveSegmentsToMove;
    }
  }

}
