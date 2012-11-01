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

import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceEmitter;

/**
 */
public class DruidMasterBalancer implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterBalancer.class);

  private final BalancerAnalyzer analyzer;
  private final DruidMaster master;

  private final ConcurrentHashMap<String, BalancerSegmentHolder> currentlyMovingSegments =
      new ConcurrentHashMap<String, BalancerSegmentHolder>();

  public DruidMasterBalancer(
      DruidMaster master,
      BalancerAnalyzer analyzer
  )
  {
    this.master = master;
    this.analyzer = analyzer;
  }

  private void reduceLifetimes(ServiceEmitter emitter)
  {
    for (BalancerSegmentHolder holder : currentlyMovingSegments.values()) {
      holder.reduceLifetime();
      if (holder.getLifetime() <= 0) {
        emitter.emit(
            new AlertEvent.Builder().build(
                "Balancer move segments queue has a segment stuck",
                ImmutableMap.<String, Object>builder()
                            .put("segment", holder.getSegment().getIdentifier())
                            .build()
            )
        );
      }
    }
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    if (!currentlyMovingSegments.isEmpty()) {
      reduceLifetimes(params.getEmitter());
      return params.buildFromExisting()
                   .withMessage(
                       String.format(
                           "Still waiting on %,d segments to be moved",
                           currentlyMovingSegments.size()
                       )
                   )
                   .build();
    }

    // Sort all servers by percent used
    TreeSet<ServerHolder> servers = Sets.newTreeSet(
        Comparators.inverse(
            new Comparator<ServerHolder>()
            {
              @Override
              public int compare(ServerHolder lhs, ServerHolder rhs)
              {
                return lhs.getPercentUsed().compareTo(rhs.getPercentUsed());
              }
            }
        )
    );

    for (DruidServer server : params.getHistoricalServers()) {
      servers.add(new ServerHolder(server, params.getLoadManagementPeons().get(server.getName())));
    }

    if (servers.size() <= 1) {
      log.info("No unique values found for highest and lowest percent used servers: nothing to balance");
      return params;
    }

    ServerHolder highestPercentUsedServer = servers.first();
    ServerHolder lowestPercentUsedServer = servers.last();

    analyzer.init(highestPercentUsedServer, lowestPercentUsedServer);

    log.info("Percent difference in percent size used between highest/lowest servers: %s%%", analyzer.getPercentDiff());

    log.info(
        "Highest percent used [%s]: size used[%s], percent used[%s%%]",
        highestPercentUsedServer.getServer().getName(),
        highestPercentUsedServer.getSizeUsed(),
        highestPercentUsedServer.getPercentUsed()
    );

    log.info(
        "Lowest percent used [%s]: size used[%s], percent used[%s%%]",
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

    return params.buildFromExisting()
                 .withMessage(
                     String.format("Moved %,d segment(s)", currentlyMovingSegments.size())
                 )
                 .withMovedCount(currentlyMovingSegments.size())
                 .build();
  }

  private void moveSegments(
      DruidServer server,
      final Set<BalancerSegmentHolder> segments,
      DruidMasterRuntimeParams params
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
        log.info(
            "Moving [%s] from [%s] to [%s]",
            segmentName,
            fromServer,
            toServer
        );
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
                  currentlyMovingSegments.remove(segmentName, segment);
                }
              }
          );
          currentlyMovingSegments.put(segmentName, segment);
        }
        catch (Exception e) {
          log.warn("Exception occurred [%s]", e.getMessage());
          continue;
        }
      } else {
        currentlyMovingSegments.remove(segment);
      }
    }
  }
}