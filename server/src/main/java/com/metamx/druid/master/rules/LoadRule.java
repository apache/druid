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

package com.metamx.druid.master.rules;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.common.Pair;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.collect.CountingMap;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import com.metamx.druid.master.LoadPeonCallback;
import com.metamx.druid.master.ServerHolder;
import com.metamx.druid.master.stats.AssignStat;
import com.metamx.druid.master.stats.DropStat;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.AlertEvent;

import java.util.List;
import java.util.Map;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(LoadRule.class);

  @Override
  public AssignStat runAssign(DruidMasterRuntimeParams params, DataSegment segment)
  {
    int assignedCount = 0;
    int unassignedCount = 0;
    long unassignedSize = 0;

    int expectedReplicants = getReplicationFactor();
    int actualReplicants = params.getSegmentReplicantLookup().lookup(segment.getIdentifier(), gettier());

    MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().getServersByTier(gettier());
    if (serverQueue == null) {
      log.makeAlert("No holders found for tier[%s]", gettier()).emit();
      return new AssignStat(new Pair<String, Integer>(gettier(), 0), 0, 0);
    }

    List<ServerHolder> assignedServers = Lists.newArrayList();
    while (actualReplicants < expectedReplicants) {
      ServerHolder holder = serverQueue.pollFirst();
      if (holder == null) {
        log.warn("Not enough %s servers[%d] to assign segments!!!", gettier(), serverQueue.size());
        break;
      }
      if (holder.containsSegment(segment)) {
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

      ++assignedCount;
      ++actualReplicants;
    }
    serverQueue.addAll(assignedServers);

    return new AssignStat(new Pair<String, Integer>(gettier(), assignedCount), unassignedCount, unassignedSize);
  }

  @Override
  public DropStat runDrop(DruidMaster master, DruidMasterRuntimeParams params, DataSegment segment)
  {
    CountingMap<String> droppedCounts = new CountingMap<String>();
    int expectedNumReplicants = getReplicationFactor();
    int actualNumReplicants = params.getSegmentReplicantLookup().lookup(
        segment.getIdentifier(),
        gettier()
    );

    if (actualNumReplicants < expectedNumReplicants) {
      return new DropStat(droppedCounts, 0);
    }

    Map<String, Integer> replicantsByType = params.getSegmentReplicantLookup().gettiers(segment.getIdentifier());

    for (Map.Entry<String, Integer> entry : replicantsByType.entrySet()) {
      String tier = entry.getKey();
      int actualNumReplicantsForType = entry.getValue();
      int expectedNumReplicantsForType = getReplicationFactor(tier);

      MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().get(tier);
      if (serverQueue == null) {
        log.makeAlert("No holders found for tier[%s]", entry.getKey()).emit();
        return new DropStat(droppedCounts, 0);
      }

      List<ServerHolder> droppedServers = Lists.newArrayList();
      while (actualNumReplicantsForType > expectedNumReplicantsForType) {
        ServerHolder holder = serverQueue.pollLast();
        if (holder == null) {
          log.warn("Wtf, holder was null?  Do I have no servers[%s]?", serverQueue);
          continue;
        }

        holder.getPeon().dropSegment(
            segment,
            new LoadPeonCallback()
            {
              @Override
              protected void execute()
              {
              }
            }
        );
        droppedServers.add(holder);
        --actualNumReplicantsForType;
        droppedCounts.add(tier, 1);
      }
      serverQueue.addAll(droppedServers);
    }

    return new DropStat(droppedCounts, 0);
  }

  public abstract int getReplicationFactor();

  public abstract int getReplicationFactor(String tier);

  public abstract String gettier();
}
