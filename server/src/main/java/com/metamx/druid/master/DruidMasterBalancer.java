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

import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.emitter.EmittingLogger;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
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

  private final BalancerAnalyzer analyzer;
  private final DruidMaster master;

  private final Map<String, ConcurrentHashMap<String, BalancerSegmentHolder>> currentlyMovingSegments = Maps.newHashMap();

  public DruidMasterBalancer(
      DruidMaster master,
      BalancerAnalyzer analyzer
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
        log.makeAlert("[%s]: Balancer move segments queue has a segment stuck", tier)
            .addData("segment", holder.getSegment().getIdentifier())
            .addData("server", holder.getServer().getStringProps())
            .emit();
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
        log.info("[%s]: Still waiting on %,d segments to be moved", tier, currentlyMovingSegments.size());
        continue;
      }

      TreeSet<ServerHolder> serversByPercentUsed = Sets.newTreeSet(percentUsedComparator);
      serversByPercentUsed.addAll(entry.getValue());

      if (serversByPercentUsed.size() <= 1) {
        log.info(
            "[%s]: No unique values found for highest and lowest percent used servers: nothing to balance", tier
        );
        continue;
      }

      ServerHolder highestPercentUsedServer = serversByPercentUsed.first();
      ServerHolder lowestPercentUsedServer = serversByPercentUsed.last();

      analyzer.init(highestPercentUsedServer, lowestPercentUsedServer);

      log.info(
          "[%s]: Percent difference in percent size used between highest/lowest servers: %s%%",
          tier,
          analyzer.getPercentDiff()
      );

      log.info(
          "[%s]: Highest percent used [%s]: size used[%s], percent used[%s%%]",
          tier,
          highestPercentUsedServer.getServer().getName(),
          highestPercentUsedServer.getSizeUsed(),
          highestPercentUsedServer.getPercentUsed()
      );

      log.info(
          "[%s]: Lowest percent used [%s]: size used[%s], percent used[%s%%]",
          tier,
          lowestPercentUsedServer.getServer().getName(),
          lowestPercentUsedServer.getSizeUsed(),
          lowestPercentUsedServer.getPercentUsed()
      );

      // Use the analyzer to find segments to move and then move them
      moveSegments(
          lowestPercentUsedServer.getServer(),
          analyzer.findSegmentsToMove(highestPercentUsedServer.getServer()),
          params
      );

      stats.addToTieredStat("movedCount", tier, currentlyMovingSegments.get(tier).size());
    }

    return params.buildFromExisting()
                 .withMasterStats(stats)
                 .build();
  }

  private void moveSegments(
      final DruidServer server,
      final Set<BalancerSegmentHolder> segments,
      final DruidMasterRuntimeParams params
  )
  {
    String toServer = server.getName();
    LoadQueuePeon toPeon = params.getLoadManagementPeons().get(toServer);

    for (final BalancerSegmentHolder segment : Sets.newHashSet(segments)) {
      String fromServer = segment.getServer().getName();
      DataSegment segmentToMove = segment.getSegment();
      final String segmentName = segmentToMove.getIdentifier();

      if (!toPeon.getSegmentsToLoad().contains(segmentToMove) &&
          (server.getSegment(segmentName) == null) &&
          new ServerHolder(server, toPeon).getAvailableSize() > segmentToMove.getSize()) {
        log.info("Moving [%s] from [%s] to [%s]", segmentName, fromServer, toServer);
        try {
          master.moveSegment(
              fromServer,
              toServer,
              segmentToMove.getIdentifier(),
              new LoadPeonCallback()
              {
                @Override
                protected void execute()
                {
                  Map<String, BalancerSegmentHolder> movingSegments = currentlyMovingSegments.get(server.getTier());
                  if (movingSegments != null) {
                    movingSegments.remove(segmentName);
                  }
                }
              }
          );
          currentlyMovingSegments.get(server.getTier()).put(segmentName, segment);
        }
        catch (Exception e) {
          log.makeAlert(e, String.format("[%s] : Moving exception", segmentName)).emit();
        }
      } else {
        currentlyMovingSegments.get(server.getTier()).remove(segment);
      }
    }
  }
}