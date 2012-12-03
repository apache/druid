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
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import com.metamx.druid.master.LoadPeonCallback;
import com.metamx.druid.master.MasterStats;
import com.metamx.druid.master.ServerHolder;
import com.metamx.emitter.EmittingLogger;

import java.util.List;
import java.util.Map;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(LoadRule.class);

  @Override
  public MasterStats run(DruidMaster master, DruidMasterRuntimeParams params, DataSegment segment)
  {
    MasterStats stats = new MasterStats();

    int expectedReplicants = getReplicants();
    int actualReplicants = params.getSegmentReplicantLookup().lookup(segment.getIdentifier(), getTier());

    MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().getServersByTier(getTier());
    if (serverQueue == null) {
      log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", getTier()).emit();
      throw new ISE("Tier[%s] has no servers! Check your cluster configuration!", getTier());
    }

    stats.accumulate(assign(expectedReplicants, actualReplicants, serverQueue, segment));
    stats.accumulate(drop(expectedReplicants, actualReplicants, segment, params));

    return stats;
  }

  private MasterStats assign(
      int expectedReplicants,
      int actualReplicants,
      MinMaxPriorityQueue<ServerHolder> serverQueue,
      DataSegment segment
  )
  {
    MasterStats stats = new MasterStats();

    List<ServerHolder> assignedServers = Lists.newArrayList();
    while (actualReplicants < expectedReplicants) {
      ServerHolder holder = serverQueue.pollFirst();
      if (holder == null) {
        log.warn("Not enough %s servers[%d] to assign segments!!!", getTier(), serverQueue.size());
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
        log.makeAlert(
            "Not enough node capacity",
            ImmutableMap.<String, Object>builder()
                        .put("segmentSkipped", segment.toString())
                        .put("closestNode", holder.getServer().toString())
                        .put("availableSize", holder.getAvailableSize())
                        .build()
        ).emit();
        serverQueue.add(holder);
        stats.addToTieredStat("unassignedCount", getTier(), 1);
        stats.addToTieredStat("unassignedSize", getTier(), segment.getSize());
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

      stats.addToTieredStat("assignedCount", getTier(), 1);
      ++actualReplicants;
    }
    serverQueue.addAll(assignedServers);

    return stats;
  }

  private MasterStats drop(
      int expectedReplicants,
      int actualReplicants,
      DataSegment segment,
      DruidMasterRuntimeParams params
  )
  {
    MasterStats stats = new MasterStats();

    if (!params.hasDeletionWaitTimeElapsed()) {
      return stats;
    }

    // Make sure we have enough actual replicants in the cluster before doing anything
    if (actualReplicants < expectedReplicants) {
      return stats;
    }

    Map<String, Integer> replicantsByType = params.getSegmentReplicantLookup().getTiers(segment.getIdentifier());

    for (Map.Entry<String, Integer> entry : replicantsByType.entrySet()) {
      String tier = entry.getKey();
      int actualNumReplicantsForType = entry.getValue();
      int expectedNumReplicantsForType = getReplicants(tier);

      MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().get(tier);
      if (serverQueue == null) {
        log.makeAlert("No holders found for tier[%s]", entry.getKey()).emit();
        return stats;
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
        stats.addToTieredStat("droppedCount", tier, 1);
      }
      serverQueue.addAll(droppedServers);
    }

    return stats;
  }

  public abstract int getReplicants();

  public abstract int getReplicants(String tier);

  public abstract String getTier();
}
