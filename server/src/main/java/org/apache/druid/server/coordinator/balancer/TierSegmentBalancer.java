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

import com.google.common.collect.Lists;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.StrategicSegmentAssigner;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
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

  private final BalancerStrategy strategy;
  private final CoordinatorDynamicConfig dynamicConfig;
  private final CoordinatorRunStats runStats;

  private final Set<ServerHolder> allServers;
  private final List<ServerHolder> activeServers;
  private final List<ServerHolder> decommissioningServers;
  private final int totalMaxSegmentsToMove;

  private final int movingSegmentCount;

  public TierSegmentBalancer(
      String tier,
      Set<ServerHolder> servers,
      DruidCoordinatorRuntimeParams params
  )
  {
    this.tier = tier;
    this.params = params;
    this.segmentAssigner = params.getSegmentAssigner();

    this.strategy = params.getBalancerStrategy();
    this.dynamicConfig = params.getCoordinatorDynamicConfig();
    this.totalMaxSegmentsToMove = dynamicConfig.getMaxSegmentsToMove();
    this.runStats = segmentAssigner.getStats();

    Map<Boolean, List<ServerHolder>> partitions =
        servers.stream().collect(Collectors.partitioningBy(ServerHolder::isDecommissioning));
    this.decommissioningServers = partitions.get(true);
    this.activeServers = partitions.get(false);
    this.allServers = servers;

    this.movingSegmentCount = activeServers.stream().mapToInt(ServerHolder::getNumMovingSegments).sum();
  }

  public void run()
  {
    if (activeServers.isEmpty() || (activeServers.size() <= 1 && decommissioningServers.isEmpty())) {
      log.warn(
          "Skipping balance for tier [%s] with [%d] active servers and [%d] decomissioning servers.",
          tier, activeServers.size(), decommissioningServers.size()
      );
      return;
    }

    log.info(
        "Moving max [%d] segments in tier [%s] with [%d] active servers and"
        + " [%d] decommissioning servers. There are [%d] segments already in queue.",
        totalMaxSegmentsToMove, tier, activeServers.size(), decommissioningServers.size(), movingSegmentCount
    );

    // Move segments from decommissioning to active servers
    int movedDecommSegments = 0;
    if (!decommissioningServers.isEmpty()) {
      int maxDecommPercentToMove = dynamicConfig.getDecommissioningMaxPercentOfMaxSegmentsToMove();
      int maxDecommSegmentsToMove = (int) Math.ceil(totalMaxSegmentsToMove * (maxDecommPercentToMove / 100.0));
      movedDecommSegments +=
          moveSegmentsFromTo(decommissioningServers, activeServers, maxDecommSegmentsToMove);
      log.info(
          "Moved [%d] segments out of max [%d (%d%%)] from decommissioning to active servers in tier [%s].",
          movedDecommSegments, maxDecommSegmentsToMove, maxDecommPercentToMove, tier
      );
    }

    // Move segments across active servers
    int maxGeneralSegmentsToMove = totalMaxSegmentsToMove - movedDecommSegments;
    int movedGeneralSegments =
        moveSegmentsFromTo(activeServers, activeServers, maxGeneralSegmentsToMove);
    log.info(
        "Moved [%d] segments out of max [%d] between active servers in tier [%s].",
        movedGeneralSegments, maxGeneralSegmentsToMove, tier
    );

    if (dynamicConfig.emitBalancingStats()) {
      strategy.emitStats(tier, runStats, Lists.newArrayList(allServers));
    }
  }

  private int moveSegmentsFromTo(
      List<ServerHolder> sourceServers,
      List<ServerHolder> destServers,
      int maxSegmentsToMove
  )
  {
    if (maxSegmentsToMove <= 0 || sourceServers.isEmpty() || destServers.isEmpty()) {
      return 0;
    }

    // Always move loading segments first as it is a cheaper operation
    Iterator<BalancerSegmentHolder> pickedSegments
        = pickSegmentsFrom(sourceServers, maxSegmentsToMove, true);
    int movedCount = moveSegmentsTo(destServers, pickedSegments, maxSegmentsToMove);

    // Move loaded segments only if tier is not already busy moving segments
    if (movingSegmentCount <= 0) {
      maxSegmentsToMove -= movedCount;
      pickedSegments = pickSegmentsFrom(sourceServers, maxSegmentsToMove, false);
      movedCount += moveSegmentsTo(destServers, pickedSegments, maxSegmentsToMove);
    }

    return movedCount;
  }

  private Iterator<BalancerSegmentHolder> pickSegmentsFrom(
      List<ServerHolder> sourceServers,
      int maxSegmentsToPick,
      boolean pickLoadingSegments
  )
  {
    if (maxSegmentsToPick <= 0 || sourceServers.isEmpty()) {
      return Collections.emptyIterator();
    } else if (dynamicConfig.useBatchedSegmentSampler()) {
      return strategy.pickSegmentsToMove(
          sourceServers,
          params.getBroadcastDatasources(),
          maxSegmentsToPick,
          pickLoadingSegments
      );
    } else if (pickLoadingSegments) {
      // non-batched sampler cannot pick loading segments
      return Collections.emptyIterator();
    } else {
      return strategy.pickSegmentsToMove(
          sourceServers,
          params.getBroadcastDatasources(),
          dynamicConfig.getPercentOfSegmentsToConsiderPerMove()
      );
    }
  }

  private int moveSegmentsTo(
      List<ServerHolder> destinationServers,
      Iterator<BalancerSegmentHolder> segmentsToMove,
      int maxSegmentsToMove
  )
  {
    int processed = 0;
    int movedCount = 0;
    while (segmentsToMove.hasNext() && processed < maxSegmentsToMove) {
      ++processed;

      final BalancerSegmentHolder segmentHolder = segmentsToMove.next();
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
      markUnmoved(Error.SEGMENT_IS_UNUSED, segmentToMove);
      return null;
    }

    ImmutableDruidDataSource datasource = params.getDataSourcesSnapshot().getDataSource(segmentToMove.getDataSource());
    if (datasource == null) {
      markUnmoved(Error.DATASOURCE_NOT_FOUND, segmentToMove);
      return null;
    }

    DataSegment loadableSegment = datasource.getSegment(segmentToMove.getId());
    if (loadableSegment == null) {
      markUnmoved(Error.METADATA_NOT_FOUND, segmentToMove);
      return null;
    }

    return loadableSegment;
  }

  private void markUnmoved(CoordinatorStat stat, DataSegment segment)
  {
    runStats.addToSegmentStat(stat, tier, segment.getDataSource(), 1);
  }

  private static class Error
  {
    static final CoordinatorStat SEGMENT_IS_UNUSED =
        new CoordinatorStat("move skipped (unused segment)", CoordinatorStat.Level.INFO);
    static final CoordinatorStat DATASOURCE_NOT_FOUND =
        new CoordinatorStat("move skipped (invalid datasource)", CoordinatorStat.Level.INFO);
    static final CoordinatorStat METADATA_NOT_FOUND =
        new CoordinatorStat("move skipped (invalid segment)", CoordinatorStat.Level.INFO);
  }

}
