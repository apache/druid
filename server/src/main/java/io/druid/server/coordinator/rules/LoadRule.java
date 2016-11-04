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

package io.druid.server.coordinator.rules;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.emitter.EmittingLogger;

import io.druid.java.util.common.IAE;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(LoadRule.class);
  private static final String assignedCount = "assignedCount";
  private static final String droppedCount = "droppedCount";

  @Override
  public CoordinatorStats run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    final Set<DataSegment> availableSegments = params.getAvailableSegments();

    final Map<String, Integer> loadStatus = Maps.newHashMap();

    int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());
    for (Map.Entry<String, Integer> entry : getTieredReplicants().entrySet()) {
      final String tier = entry.getKey();
      final int expectedReplicantsInTier = entry.getValue();
      final int totalReplicantsInTier = params.getSegmentReplicantLookup()
                                              .getTotalReplicants(segment.getIdentifier(), tier);
      final int loadedReplicantsInTier = params.getSegmentReplicantLookup()
                                         .getLoadedReplicants(segment.getIdentifier(), tier);

      final MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().getServersByTier(tier);
      if (serverQueue == null) {
        log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", tier).emit();
        continue;
      }

      final List<ServerHolder> serverHolderList = Lists.newArrayList(serverQueue);
      final DateTime referenceTimestamp = params.getBalancerReferenceTimestamp();
      final BalancerStrategy strategy = params.getBalancerStrategyFactory().createBalancerStrategy(referenceTimestamp);
      if (availableSegments.contains(segment)) {
        CoordinatorStats assignStats = assign(
            params.getReplicationManager(),
            tier,
            totalReplicantsInCluster,
            expectedReplicantsInTier,
            totalReplicantsInTier,
            strategy,
            serverHolderList,
            segment
        );
        stats.accumulate(assignStats);
        totalReplicantsInCluster += assignStats.getPerTierStats().get(assignedCount).get(tier).get();
      }

      loadStatus.put(tier, expectedReplicantsInTier - loadedReplicantsInTier);
    }
    // Remove over-replication
    stats.accumulate(drop(loadStatus, segment, params));


    return stats;
  }

  private CoordinatorStats assign(
      final ReplicationThrottler replicationManager,
      final String tier,
      final int totalReplicantsInCluster,
      final int expectedReplicantsInTier,
      final int totalReplicantsInTier,
      final BalancerStrategy strategy,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment
  )
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToTieredStat(assignedCount, tier, 0);

    int currReplicantsInTier = totalReplicantsInTier;
    int currTotalReplicantsInCluster = totalReplicantsInCluster;
    while (currReplicantsInTier < expectedReplicantsInTier) {
      boolean replicate = currTotalReplicantsInCluster > 0;

      if (replicate && !replicationManager.canCreateReplicant(tier)) {
        break;
      }

      final ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);

      if (holder == null) {
        log.warn(
            "Not enough [%s] servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
            tier,
            segment.getIdentifier(),
            expectedReplicantsInTier
        );
        break;
      }

      if (replicate) {
        replicationManager.registerReplicantCreation(
            tier, segment.getIdentifier(), holder.getServer().getHost()
        );
      }

      holder.getPeon().loadSegment(
          segment,
          new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
              replicationManager.unregisterReplicantCreation(
                  tier,
                  segment.getIdentifier(),
                  holder.getServer().getHost()
              );
            }
          }
      );

      stats.addToTieredStat(assignedCount, tier, 1);
      ++currReplicantsInTier;
      ++currTotalReplicantsInCluster;
    }

    return stats;
  }

  private CoordinatorStats drop(
      final Map<String, Integer> loadStatus,
      final DataSegment segment,
      final DruidCoordinatorRuntimeParams params
  )
  {
    CoordinatorStats stats = new CoordinatorStats();

    // Make sure we have enough loaded replicants in the correct tiers in the cluster before doing anything
    for (Integer leftToLoad : loadStatus.values()) {
      if (leftToLoad > 0) {
        return stats;
      }
    }

    final ReplicationThrottler replicationManager = params.getReplicationManager();

    // Find all instances of this segment across tiers
    Map<String, Integer> replicantsByTier = params.getSegmentReplicantLookup().getClusterTiers(segment.getIdentifier());

    for (Map.Entry<String, Integer> entry : replicantsByTier.entrySet()) {
      final String tier = entry.getKey();
      int loadedNumReplicantsForTier = entry.getValue();
      int expectedNumReplicantsForTier = getNumReplicants(tier);

      stats.addToTieredStat(droppedCount, tier, 0);

      MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().get(tier);
      if (serverQueue == null) {
        log.makeAlert("No holders found for tier[%s]", entry.getKey()).emit();
        continue;
      }

      List<ServerHolder> droppedServers = Lists.newArrayList();
      while (loadedNumReplicantsForTier > expectedNumReplicantsForTier) {
        final ServerHolder holder = serverQueue.pollLast();
        if (holder == null) {
          log.warn("Wtf, holder was null?  I have no servers serving [%s]?", segment.getIdentifier());
          break;
        }

        if (holder.isServingSegment(segment)) {
          if (expectedNumReplicantsForTier > 0) { // don't throttle unless we are removing extra replicants
            if (!replicationManager.canDestroyReplicant(tier)) {
              serverQueue.add(holder);
              break;
            }

            replicationManager.registerReplicantTermination(
                tier,
                segment.getIdentifier(),
                holder.getServer().getHost()
            );
          }

          holder.getPeon().dropSegment(
              segment,
              new LoadPeonCallback()
              {
                @Override
                public void execute()
                {
                  replicationManager.unregisterReplicantTermination(
                      tier,
                      segment.getIdentifier(),
                      holder.getServer().getHost()
                  );
                }
              }
          );
          --loadedNumReplicantsForTier;
          stats.addToTieredStat(droppedCount, tier, 1);
        }
        droppedServers.add(holder);
      }
      serverQueue.addAll(droppedServers);
    }

    return stats;
  }

  protected void validateTieredReplicants(Map<String, Integer> tieredReplicants){
    if(tieredReplicants.size() == 0) {
      throw new IAE("A rule with empty tiered replicants is invalid");
    }
    for (Map.Entry<String, Integer> entry: tieredReplicants.entrySet()) {
      if (entry.getValue() == null) {
        throw new IAE("Replicant value cannot be empty");
      }
      if (entry.getValue() < 0) {
        throw new IAE("Replicant value [%d] is less than 0, which is not allowed", entry.getValue());
      }
    }
  }

  public abstract Map<String, Integer> getTieredReplicants();

  public abstract int getNumReplicants(String tier);
}
