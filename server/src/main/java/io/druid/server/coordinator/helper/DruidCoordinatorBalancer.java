/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator.helper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.guava.Comparators;
import com.metamx.emitter.EmittingLogger;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.BalancerSegmentHolder;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

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

  protected final Map<String, ConcurrentHashMap<String, BalancerSegmentHolder>> currentlyMovingSegments = Maps.newHashMap();

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
    final DateTime referenceTimestamp = params.getBalancerReferenceTimestamp();
    final BalancerStrategy strategy = params.getBalancerStrategyFactory().createBalancerStrategy(referenceTimestamp);
    final int maxSegmentsToMove = params.getCoordinatorDynamicConfig().getMaxSegmentsToMove();

    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry :
        params.getDruidCluster().getCluster().entrySet()) {
      String tier = entry.getKey();

      if (currentlyMovingSegments.get(tier) == null) {
        currentlyMovingSegments.put(tier, new ConcurrentHashMap<String, BalancerSegmentHolder>());
      }

      if (!currentlyMovingSegments.get(tier).isEmpty()) {
        reduceLifetimes(tier);
        log.info("[%s]: Still waiting on %,d segments to be moved", tier, currentlyMovingSegments.size());
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

      for (int iter = 0; iter < maxSegmentsToMove; iter++) {
        final BalancerSegmentHolder segmentToMove = strategy.pickSegmentToMove(serverHolderList);

        if (segmentToMove != null && params.getAvailableSegments().contains(segmentToMove.getSegment())) {
          final ServerHolder holder = strategy.findNewSegmentHomeBalancer(segmentToMove.getSegment(), serverHolderList);

          if (holder != null) {
            moveSegment(segmentToMove, holder.getServer(), params);
          }
        }
      }
      stats.addToTieredStat("movedCount", tier, currentlyMovingSegments.get(tier).size());
      if (params.getCoordinatorDynamicConfig().emitBalancingStats()) {
        strategy.emitStats(tier, stats, serverHolderList);

      }
      log.info(
          "[%s]: Segments Moved: [%d]", tier, currentlyMovingSegments.get(tier).size()
      );

    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  protected void moveSegment(
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
            segmentToMove.getIdentifier(),
            callback
        );
      }
      catch (Exception e) {
        log.makeAlert(e, String.format("[%s] : Moving exception", segmentName)).emit();
        if (callback != null) {
          callback.execute();
        }
      }
    } else {
      currentlyMovingSegments.get(toServer.getTier()).remove(segmentName);
    }

  }
}
