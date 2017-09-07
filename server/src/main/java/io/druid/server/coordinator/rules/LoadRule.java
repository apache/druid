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
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  private static final EmittingLogger log = new EmittingLogger(LoadRule.class);
  static final String ASSIGNED_COUNT = "assignedCount";
  static final String DROPPED_COUNT = "droppedCount";

  @Override
  public CoordinatorStats run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    final Set<DataSegment> availableSegments = params.getAvailableSegments();

    final Map<String, Integer> loadStatus = Maps.newHashMap();

    final Map<String, Integer> tieredReplicants = getTieredReplicants();
    for (final String tier : tieredReplicants.keySet()) {
      stats.addToTieredStat(ASSIGNED_COUNT, tier, 0);
    }

    final BalancerStrategy strategy = params.getBalancerStrategy();

    final int maxSegmentsInNodeLoadingQueue = params.getCoordinatorDynamicConfig()
                                                    .getMaxSegmentsInNodeLoadingQueue();

    final Predicate<ServerHolder> serverHolderPredicate;
    if (maxSegmentsInNodeLoadingQueue > 0) {
      serverHolderPredicate = s -> (s != null && s.getNumberOfSegmentsInQueue() < maxSegmentsInNodeLoadingQueue);
    } else {
      serverHolderPredicate = Objects::nonNull;
    }

    int totalReplicantsInCluster = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());

    final ServerHolder primaryHolderToLoad;
    if (totalReplicantsInCluster <= 0) {
      log.debug("No replicants for %s", segment.getIdentifier());
      primaryHolderToLoad = getPrimaryHolder(
          params.getDruidCluster(),
          tieredReplicants,
          strategy,
          segment,
          serverHolderPredicate
      );

      if (primaryHolderToLoad == null) {
        log.trace("No primary holder found for %s", segment.getIdentifier());
        return stats.accumulate(drop(loadStatus, segment, params));
      }

      primaryHolderToLoad.getPeon().loadSegment(segment, null);
      stats.addToTieredStat(ASSIGNED_COUNT, primaryHolderToLoad.getServer().getTier(), 1);
      ++totalReplicantsInCluster;
    } else {
      primaryHolderToLoad = null;
    }

    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      final String tier = entry.getKey();
      final int expectedReplicantsInTier = entry.getValue();

      int totalReplicantsInTier = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier(), tier);
      if (primaryHolderToLoad != null && primaryHolderToLoad.getServer().getTier().equals(tier)) {
        totalReplicantsInTier += 1;
      }

      final int loadedReplicantsInTier = params.getSegmentReplicantLookup()
                                               .getLoadedReplicants(segment.getIdentifier(), tier);

      final MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().getHistoricalsByTier(tier);

      if (serverQueue == null) {
        log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", tier).emit();
        continue;
      }

      final List<ServerHolder> serverHolderList =
          serverQueue
              .stream()
              .filter(serverHolderPredicate)
              .filter(holder -> primaryHolderToLoad != null && !holder.equals(primaryHolderToLoad))
              .collect(Collectors.toList());

      if (availableSegments.contains(segment)) {
        final int assignedCount = assignReplicas(
            params.getReplicationManager(),
            tier,
            expectedReplicantsInTier,
            totalReplicantsInTier,
            strategy,
            serverHolderList,
            segment
        );
        stats.addToTieredStat(ASSIGNED_COUNT, tier, assignedCount);
        totalReplicantsInCluster += assignedCount;
      }

      loadStatus.put(tier, expectedReplicantsInTier - loadedReplicantsInTier);
    }
    // Remove over-replication
    stats.accumulate(drop(loadStatus, segment, params));

    return stats;
  }

  @Nullable
  private static ServerHolder getPrimaryHolder(
      final DruidCluster cluster,
      final Map<String, Integer> tieredReplicants,
      final BalancerStrategy strategy,
      final DataSegment segment,
      final Predicate<ServerHolder> serverHolderPredicate
  )
  {
    ServerHolder topCandidate = null;

    for (final Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
      final int expectedReplicantsInTier = entry.getValue();
      if (expectedReplicantsInTier <= 0) {
        continue;
      }

      final String tier = entry.getKey();

      final MinMaxPriorityQueue<ServerHolder> serverQueue = cluster.getHistoricalsByTier(tier);
      if (serverQueue == null) {
        log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", tier).emit();
        continue;
      }

      final ServerHolder candidate = strategy.findNewSegmentHomeReplicator(
          segment,
          serverQueue.stream().filter(serverHolderPredicate).collect(Collectors.toList())
      );
      if (candidate == null) {
        log.warn(
            "Not enough [%s] servers or node capacity to assign primary segment[%s]! Expected Replicants[%d]",
            tier,
            segment.getIdentifier(),
            expectedReplicantsInTier
        );
      } else {
        if (topCandidate == null) {
          topCandidate = candidate;
        } else {
          if (candidate.getServer().getPriority() > topCandidate.getServer().getPriority()) {
            topCandidate = candidate;
          }
        }
      }
    }

    return topCandidate;
  }

  private int assignReplicas(
      final ReplicationThrottler replicationManager,
      final String tier,
      final int expectedReplicantsInTier,
      final int totalReplicantsInTier,
      final BalancerStrategy strategy,
      final List<ServerHolder> serverHolderList,
      final DataSegment segment
  )
  {
    int assignedCount = 0;
    int currReplicantsInTier = totalReplicantsInTier;
    while (currReplicantsInTier < expectedReplicantsInTier) {
      if (!replicationManager.canCreateReplicant(tier)) {
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
      serverHolderList.remove(holder);

      replicationManager.registerReplicantCreation(
          tier,
          segment.getIdentifier(),
          holder.getServer().getHost()
      );

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

      ++assignedCount;
      ++currReplicantsInTier;
    }

    return assignedCount;
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

    // Find all instances of this segment across tiers
    Map<String, Integer> replicantsByTier = params.getSegmentReplicantLookup().getClusterTiers(segment.getIdentifier());

    for (Map.Entry<String, Integer> entry : replicantsByTier.entrySet()) {
      final String tier = entry.getKey();
      int loadedNumReplicantsForTier = entry.getValue();
      int expectedNumReplicantsForTier = getNumReplicants(tier);

      stats.addToTieredStat(DROPPED_COUNT, tier, 0);

      MinMaxPriorityQueue<ServerHolder> serverQueue = params.getDruidCluster().getHistoricalsByTier(tier);
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
          holder.getPeon().dropSegment(
              segment,
              null
          );
          --loadedNumReplicantsForTier;
          stats.addToTieredStat(DROPPED_COUNT, tier, 1);
        }
        droppedServers.add(holder);
      }
      serverQueue.addAll(droppedServers);
    }

    return stats;
  }

  protected void validateTieredReplicants(Map<String, Integer> tieredReplicants)
  {
    if (tieredReplicants.size() == 0) {
      throw new IAE("A rule with empty tiered replicants is invalid");
    }
    for (Map.Entry<String, Integer> entry : tieredReplicants.entrySet()) {
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
