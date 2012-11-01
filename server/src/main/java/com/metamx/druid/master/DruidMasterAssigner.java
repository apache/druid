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

import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;

import com.google.common.collect.ImmutableMap;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.emitter.service.AlertEvent;

/**
 */
public class DruidMasterAssigner implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterAssigner.class);

  private final DruidMaster master;

  public DruidMasterAssigner(
      DruidMaster master
  )
  {
    this.master = master;
  }

  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    int unassignedCount = 0;
    int unassignedSize = 0;
    int assignedCount = 0;

    // Assign unserviced segments to servers in order of most available space
    Collection<DruidServer> servers = params.getHistoricalServers();
    Map<String, LoadQueuePeon> loadManagementPeons = params.getLoadManagementPeons();

    if (servers.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    if (loadManagementPeons.isEmpty()) {
      log.warn("I have servers but no peons for them? What is happening?!");
      return params;
    }

    PriorityQueue<ServerHolder> serverQueue = new PriorityQueue<ServerHolder>(
        loadManagementPeons.size(),
        Comparators.inverse(Comparators.<Comparable>comparable())
    );
    for (DruidServer server : servers) {
      serverQueue.add(new ServerHolder(server, loadManagementPeons.get(server.getName())));
    }

    for (DataSegment segment : params.getUnservicedSegments()) {
      if (master.lookupSegmentLifetime(segment) > 0) {
        continue;
      }
      ServerHolder holder = serverQueue.poll();
      if (holder == null) {
        log.warn("Wtf, holder was null?  Do I have no servers[%s]?", serverQueue);
        continue;
      }

      if (holder.getAvailableSize() < segment.getSize()) {
        log.warn(
            "Not enough node capacity, closest is [%s] with %,d available, skipping segment[%s].",
            holder.getServer(),
            holder.getAvailableSize(),
            segment
        );
        params.getEmitter().emit(
            new AlertEvent.Builder().build(
                "Not enough node capacity",
                ImmutableMap.<String, Object>builder()
                            .put("segmentSkipped", segment.toString())
                            .put("closestNode", holder.getServer().toString())
                            .put("availableSize", holder.getAvailableSize())
                            .build()
            )
        );
        serverQueue.add(holder);
        ++unassignedCount;
        unassignedSize += segment.getSize();
        continue;
      }

      holder.getPeon().loadSegment(segment, new LoadPeonCallback()
      {
        @Override
        protected void execute()
        {
          return;
        }
      });
      serverQueue.add(holder);
      ++assignedCount;
    }
    master.decrementRemovedSegmentsLifetime();

    return params.buildFromExisting()
                 .withMessage(
                     String.format(
                         "Assigned %,d segments among %,d servers",
                         assignedCount,
                         servers.size()
                     )
                 )
                 .withAssignedCount(assignedCount)
                 .withUnassignedCount(unassignedCount)
                 .withUnassignedSize(unassignedSize)
                 .build();
  }
}
