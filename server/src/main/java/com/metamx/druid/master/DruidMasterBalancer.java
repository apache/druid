/*
* Druid - a distributed column store.
* Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.emitter.EmittingLogger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class DruidMasterBalancer implements DruidMasterHelper
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
  private static final EmittingLogger log = new EmittingLogger(DruidMasterBalancer.class);

  private final BalancerCostAnalyzer analyzer;
  private final DruidMaster master;

  private final Map<String, ConcurrentHashMap<String, BalancerSegmentHolder>> currentlyMovingSegments = Maps.newHashMap();

  public DruidMasterBalancer(
      DruidMaster master,
      BalancerCostAnalyzer analyzer
  )
  {
    this.master = master;
    this.analyzer = analyzer;
  }

  private void reduceLifetimes(String tier)
  {
    for (BalancerSegmentHolder holder : currentlyMovingSegments.get(tier).values()) {
      holder.reduceLifetime();
      if (holder.getLifetime() <= 0) {
        log.makeAlert(
            "[%s]: Balancer move segments queue has a segment stuck",
            tier,
            ImmutableMap.<String, Object>builder()
                        .put("segment", holder.getSegment().getIdentifier())
                        .build()
        ).emit();
      }
    }
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    MasterStats stats = new MasterStats();

    for (Map.Entry<String, MinMaxPriorityQueue<ServerHolder>> entry :
        params.getDruidCluster().getCluster().entrySet()) {
      String tier = entry.getKey();

      if (currentlyMovingSegments.get(tier) == null) {
        currentlyMovingSegments.put(tier, new ConcurrentHashMap<String, BalancerSegmentHolder>());
      }

      if (!currentlyMovingSegments.get(tier).isEmpty()) {
        reduceLifetimes(tier);
        log.info(
            "[%s]: Still waiting on %,d segments to be moved",
            tier,
            currentlyMovingSegments.size()
        );
        continue;
      }

      List<ServerHolder> serverHolderList = new ArrayList<ServerHolder>(entry.getValue());

      analyzer.init(serverHolderList);
      log.info(
          "Initial Total Cost: [%s]",
          analyzer.getInitialTotalCost()
      );

      analyzer.init(serverHolderList);
      log.info(
          "Normalization: [%s]",
          analyzer.getNormalization()
      );

      analyzer.init(serverHolderList);
      log.info(
          "Normalized Inital Cost: [%s]",
          analyzer.getNormalizedInitialCost()
      );

      moveSegments(analyzer.findSegmentsToMove(), params);

      stats.addToTieredStat("costChange", tier, (long) analyzer.getTotalCostChange());
      log.info(
          "Cost Change: [%s]",
          analyzer.getTotalCostChange()
      );

      if (serverHolderList.size() <= 1) {
        log.info(
            "[%s]: One or fewer servers found.  Cannot balance.",
            tier
        );
        continue;
      }

      stats.addToTieredStat("movedCount", tier, currentlyMovingSegments.get(tier).size());
    }

    return params.buildFromExisting()
                 .withMasterStats(stats)
                 .build();
  }

  private void moveSegments(
      final Set<BalancerSegmentHolder> segments,
      final DruidMasterRuntimeParams params
  )
  {

    for (final BalancerSegmentHolder segment : Sets.newHashSet(segments)) {
      final DruidServer toServer = segment.getToServer();
      final String toServerName = segment.getToServer().getName();
      LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServerName);

      String fromServer = segment.getFromServer().getName();
      DataSegment segmentToMove = segment.getSegment();
      final String segmentName = segmentToMove.getIdentifier();

      if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
          (toServer.getSegment(segmentName) == null) &&
          new ServerHolder(toServer, toPeon).getAvailableSize() > segmentToMove.getSize()) {
        log.info(
            "Moving [%s] from [%s] to [%s]",
            segmentName,
            fromServer,
            toServerName
        );
        try {
          master.moveSegment(
              fromServer,
              toServerName,
              segmentToMove.getIdentifier(),
              new LoadPeonCallback()
              {
                @Override
                protected void execute()
                {
                  Map<String, BalancerSegmentHolder> movingSegments = currentlyMovingSegments.get(toServer.getTier());
                  if (movingSegments != null) {
                    movingSegments.remove(segmentName);
                  }
                }
              }
          );
          currentlyMovingSegments.get(toServer.getTier()).put(segmentName, segment);
        }
        catch (Exception e) {
          log.makeAlert(e, String.format("[%s] : Moving exception", segmentName)).emit();
        }
      } else {
        currentlyMovingSegments.get(toServer.getTier()).remove(segment);
      }
    }

  }
}