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

import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.master.BalancerCostAnalyzer;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import com.metamx.druid.master.LoadPeonCallback;
import com.metamx.druid.master.MasterStats;
import com.metamx.druid.master.ReplicationThrottler;
import com.metamx.druid.master.ServerHolder;
import com.metamx.emitter.EmittingLogger;
import org.joda.time.DateTime;

import java.util.ArrayList;
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
    int totalReplicants = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier(), getTier());
    int clusterReplicants = params.getSegmentReplicantLookup().getClusterReplicants(segment.getIdentifier(), getTier());

    MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().getServersByTier(getTier());
    if (serverQueue == null) {
      log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", getTier()).emit();
      return stats;
    }

    final List<ServerHolder> serverHolderList = new ArrayList<ServerHolder>(serverQueue);
    final DateTime referenceTimestamp = params.getBalancerReferenceTimestamp();
    final BalancerCostAnalyzer analyzer = params.getBalancerCostAnalyzer(referenceTimestamp);

    if (params.getAvailableSegments().contains(segment)) {
      stats.accumulate(
          assign(
              params.getReplicationManager(),
              expectedReplicants,
              totalReplicants,
              analyzer,
              serverHolderList,
              segment
          )
      );
    }

    stats.accumulate(drop(expectedReplicants, clusterReplicants, segment, params));

    return stats;
  }

  private MasterStats assign(
      final ReplicationThrottler replicationManager,
      final int expectedReplicants,
      int totalReplicants,
      final BalancerCostAnalyzer analyzer,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment
  )
  {
    final MasterStats stats = new MasterStats();

    while (totalReplicants < expectedReplicants) {
      boolean replicate = totalReplicants > 0;

      if (replicate && !replicationManager.canCreateReplicant(getTier())) {
        break;
      }

      final ServerHolder holder = analyzer.findNewSegmentHomeAssign(segment, serverHolderList);

      if (holder == null) {
        log.warn(
            "Not enough %s servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
            getTier(),
            segment.getIdentifier(),
            expectedReplicants
        );
        break;
      }

      if (replicate) {
        replicationManager.registerReplicantCreation(
            getTier(), segment.getIdentifier(), holder.getServer().getHost()
        );
      }

      holder.getPeon().loadSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            protected void execute()
            {
              replicationManager.unregisterReplicantCreation(
                  getTier(),
                  segment.getIdentifier(),
                  holder.getServer().getHost()
              );
            }
          }
      );

      stats.addToTieredStat("assignedCount", getTier(), 1);
      ++totalReplicants;
    }

    return stats;
  }

  private MasterStats drop(
      int expectedReplicants,
      int clusterReplicants,
      final DataSegment segment,
      final DruidMasterRuntimeParams params
  )
  {
    MasterStats stats = new MasterStats();
    final ReplicationThrottler replicationManager = params.getReplicationManager();

    if (!params.hasDeletionWaitTimeElapsed()) {
      return stats;
    }

    // Make sure we have enough actual replicants in the cluster before doing anything
    if (clusterReplicants < expectedReplicants) {
      return stats;
    }

    Map<String, Integer> replicantsByType = params.getSegmentReplicantLookup().getClusterTiers(segment.getIdentifier());

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
        final ServerHolder holder = serverQueue.pollLast();
        if (holder == null) {
          log.warn("Wtf, holder was null?  I have no servers serving [%s]?", segment.getIdentifier());
          break;
        }

        if (holder.isServingSegment(segment)) {
          if (expectedNumReplicantsForType > 0) { // don't throttle unless we are removing extra replicants
            if (!replicationManager.canDestroyReplicant(getTier())) {
              serverQueue.add(holder);
              break;
            }

            replicationManager.registerReplicantTermination(
                getTier(),
                segment.getIdentifier(),
                holder.getServer().getHost()
            );
          }

          holder.getPeon().dropSegment(
              segment,
              new LoadPeonCallback()
              {
                @Override
                protected void execute()
                {
                  replicationManager.unregisterReplicantTermination(
                      getTier(),
                      segment.getIdentifier(),
                      holder.getServer().getHost()
                  );
                }
              }
          );
          --actualNumReplicantsForType;
          stats.addToTieredStat("droppedCount", tier, 1);
        }
        droppedServers.add(holder);
      }
      serverQueue.addAll(droppedServers);
    }

    return stats;
  }

  public abstract int getReplicants();

  public abstract int getReplicants(String tier);

  public abstract String getTier();
}
