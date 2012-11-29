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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.master.rules.LoadRule;
import com.metamx.druid.master.rules.Rule;
import com.metamx.emitter.service.AlertEvent;

import java.util.List;
import java.util.Map;

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
    long unassignedSize = 0;
    Map<String, Integer> assignedCounts = Maps.newHashMap();

    Map<String, MinMaxPriorityQueue<ServerHolder>> servers = params.getHistoricalServers();

    if (servers.isEmpty()) {
      log.warn("Uh... I have no servers. Not assigning anything...");
      return params;
    }

    // Assign unserviced segments to servers in order of most available space
    for (DataSegment segment : params.getAvailableSegments()) {
      Rule rule = params.getSegmentRules().get(segment.getIdentifier());
      if (rule instanceof LoadRule) {
        LoadRule loadRule = (LoadRule) rule;

        int expectedReplicants = loadRule.getReplicationFactor();
        int actualReplicants = (params.getSegmentsInCluster().get(segment.getIdentifier()) == null ||
                                params.getSegmentsInCluster().get(segment.getIdentifier()).get(loadRule.getNodeType())
                                == null)
                               ? 0
                               : params.getSegmentsInCluster()
                                       .get(segment.getIdentifier())
                                       .get(loadRule.getNodeType());

        MinMaxPriorityQueue<ServerHolder> serverQueue = params.getHistoricalServers().get(loadRule.getNodeType());
        if (serverQueue == null) {
          throw new ISE("No holders found for nodeType[%s]", loadRule.getNodeType());
        }

        List<ServerHolder> assignedServers = Lists.newArrayList();
        while (actualReplicants < expectedReplicants) {
          ServerHolder holder = serverQueue.pollFirst();
          if (holder == null) {
            log.warn("Not enough %s servers[%d] to assign segments!!!", loadRule.getNodeType(), serverQueue.size());
            break;
          }

          // Segment already exists on this node
          if (holder.getServer().getSegments().containsKey(segment.getIdentifier()) ||
              holder.getPeon().getSegmentsToLoad().contains(segment)) {
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
            unassignedCount++;
            unassignedSize += segment.getSize();
            break;
          }

          holder.getPeon().loadSegment(
              segment,
              new LoadPeonCallback()
              {
                @Override
                protected void execute()
                {
                }
              }
          );
          assignedServers.add(holder);

          assignedCounts.put(
              loadRule.getNodeType(),
              assignedCounts.get(loadRule.getNodeType()) == null ? 1 :
              assignedCounts.get(loadRule.getNodeType()) + 1
          );

          ++actualReplicants;
        }

        serverQueue.addAll(assignedServers);
      }
    }
    master.decrementRemovedSegmentsLifetime();

    List<String> assignmentMsgs = Lists.newArrayList();
    for (Map.Entry<String, Integer> entry : assignedCounts.entrySet()) {
      assignmentMsgs.add(
          String.format(
              "[%s] : Assigned %,d segments among %,d servers",
              entry.getKey(), assignedCounts.get(entry.getKey()), servers.get(entry.getKey()).size()
          )
      );
    }

    return params.buildFromExisting()
                 .withMessages(assignmentMsgs)
                 .withAssignedCount(assignedCounts)
                 .withUnassignedCount(unassignedCount)
                 .withUnassignedSize(unassignedSize)
                 .build();
  }
}
