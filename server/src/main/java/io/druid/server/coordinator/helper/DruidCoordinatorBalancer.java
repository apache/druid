/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.ImmutableDruidServer;
import io.druid.java.util.common.guava.Comparators;
import io.druid.server.coordinator.BalancerSegmentHolder;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class DruidCoordinatorBalancer implements DruidCoordinatorHelper
{
  public static final Comparator<ServerHolder> percentUsedComparator = Comparators.inverse(
      new Comparator<ServerHolder>()
      {
        @Override
        public int compare(ServerHolder lhs, ServerHolder rhs)
        {
          return lhs.getPercentUsed().compareTo(rhs.getPercentUsed());
        }
      }
  );
  protected static final EmittingLogger log = new EmittingLogger(DruidCoordinatorBalancer.class);

  protected final DruidCoordinator coordinator;

  protected final Map<String, ConcurrentHashMap<String, BalancerSegmentHolder>> currentlyMovingSegments =
      new ConcurrentHashMap<>();

  public DruidCoordinatorBalancer(
      DruidCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  protected void reduceLifetimes(String tier)
  {
    for (BalancerSegmentHolder holder : currentlyMovingSegments.get(tier).values()) {
      holder.reduceLifetime();
      if (holder.getLifetime() <= 0) {
        log.makeAlert("[%s]: Balancer move segments queue has a segment stuck", tier)
           .addData("segment", holder.getSegment().getIdentifier())
           .addData("server", holder.getFromServer().getMetadata())
           .emit();
      }
    }
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    final BalancerStrategy strategy = params.getBalancerStrategy();
    final int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();

    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry :
        params.getDruidCluster().getHistoricals().entrySet()) {
      String tier = entry.getKey();

      Map<String, BalancerSegmentHolder> tierMovingSegments =
          currentlyMovingSegments.computeIfAbsent(tier, t -> new ConcurrentHashMap<>());

      final int numberOfMovingSegments = tierMovingSegments.size();

      if (numberOfMovingSegments > 0) {
        reduceLifetimes(tier);
        log.info("[%s]: Still waiting on %,d segments to be moved", tier, numberOfMovingSegments);
      }

      final int segmentsToMove = maxSegmentsToMove - numberOfMovingSegments;

      if (segmentsToMove <= 0) {
        log.info("[%s]: Too many segments are currently moving. Waiting.", tier);
        continue;
      }

      final List<ServerHolder> serverHolderList = Lists.newArrayList(entry.getValue());

      if (serverHolderList.size() <= 1) {
        log.info("[%s]: One or fewer servers found.  Cannot balance.", tier);
        continue;
      }

      int numSegments = 0;
      for (ServerHolder server : serverHolderList) {
        numSegments += server.getServer().getSegments().size();
      }

      if (numSegments == 0) {
        log.info("No segments found.  Cannot balance.");
        continue;
      }
      long unmoved = 0L;
      long moved = 0L;
      for (int iter = 0; iter < segmentsToMove; iter++) {
        final BalancerSegmentHolder segmentToMove = strategy.pickSegmentToMove(serverHolderList);

        if (segmentToMove != null && params.getAvailableSegments().contains(segmentToMove.getSegment())) {
          final ServerHolder holder = strategy.findNewSegmentHomeBalancer(segmentToMove.getSegment(), serverHolderList);

          if (holder != null && moveSegment(segmentToMove, holder.getServer(), params)) {
            ++moved;
          } else {
            ++unmoved;
          }
        }
      }
      if (unmoved == segmentsToMove) {
        // Cluster should be alive and constantly adjusting
        log.info("No good moves found in tier [%s]", tier);
      }
      stats.addToTieredStat("unmovedCount", tier, unmoved);
      stats.addToTieredStat("movedCount", tier, moved);
      if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
        strategy.emitStats(tier, stats, serverHolderList);
      }
      log.info(
          "[%s]: Segments Moved: [%d] Segments Let Alone: [%d]",
          tier,
          moved,
          unmoved
      );

    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  protected boolean moveSegment(
      final BalancerSegmentHolder segment,
      final ImmutableDruidServer toServer,
      final DruidCoordinatorRuntimeParams params
  )
  {
    final LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServer.getName());

    final ImmutableDruidServer fromServer = segment.getFromServer();
    final DataSegment segmentToMove = segment.getSegment();
    final String segmentName = segmentToMove.getIdentifier();

    if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
        (toServer.getSegment(segmentName) == null) &&
        new ServerHolder(toServer, toPeon).getAvailableSize() > segmentToMove.getSize()) {
      log.info("Moving [%s] from [%s] to [%s]", segmentName, fromServer.getName(), toServer.getName());

      LoadPeonCallback callback = null;
      try {
        currentlyMovingSegments.get(toServer.getTier()).put(segmentName, segment);
        callback = new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
            Map<String, BalancerSegmentHolder> movingSegments = currentlyMovingSegments.get(toServer.getTier());
            if (movingSegments != null) {
              movingSegments.remove(segmentName);
            }
          }
        };
        coordinator.moveSegment(
            fromServer,
            toServer,
            segmentToMove,
            callback
        );
        return true;
      }
      catch (Exception e) {
        log.makeAlert(e, String.format("[%s] : Moving exception", segmentName)).emit();
        if (callback != null) {
          callback.execute();
        }
      }
    }
    return false;
  }
}
