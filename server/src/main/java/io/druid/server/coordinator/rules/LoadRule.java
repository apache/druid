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

import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.IAE;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  public CoordinatorStats run(
      final DruidCoordinator coordinator,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    final Object2IntMap<String> targetReplicants = new Object2IntOpenHashMap<>(getTieredReplicants());

    final Object2IntMap<String> currentReplicants = new Object2IntOpenHashMap<>(
        params.getSegmentReplicantLookup().getClusterTiers(segment.getIdentifier())
    );

    final CoordinatorStats stats = new CoordinatorStats();
    if (params.getAvailableSegments().contains(segment)) {
      assign(targetReplicants, currentReplicants, params, segment, stats);
    }

    drop(targetReplicants, currentReplicants, params, segment, stats);

    return stats;
  }

  private static void assign(
      final Object2IntMap<String> targetReplicants,
      final Object2IntMap<String> currentReplicants,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats
  )
  {
    // if primary replica already exists
    if (!currentReplicants.isEmpty()) {
      assignReplicas(targetReplicants, currentReplicants, params, segment, stats);
    } else {
      final ServerHolder primaryHolderToLoad = assignPrimary(targetReplicants, params, segment);
      if (primaryHolderToLoad == null) {
        // cluster does not have any replicants and cannot identify primary holder
        // this implies that no assignment could be done
        return;
      }

      final String tier = primaryHolderToLoad.getServer().getTier();
      // assign replicas for the rest of the tier
      final int numAssigned = 1 /* primary */ + assignReplicasForTier(
          tier,
          targetReplicants.getOrDefault(tier, 0),
          currentReplicants.getOrDefault(tier, 0) + 1,
          params,
          getHolderList(
              tier,
              params.getDruidCluster(),
              createLoadQueueSizeLimitingPredicate(params),
              holder -> !holder.equals(primaryHolderToLoad)
          ),
          segment
      );
      stats.addToTieredStat(ASSIGNED_COUNT, tier, numAssigned);

      // tier with primary replica
      final int targetReplicantsInTier = targetReplicants.removeInt(tier);
      // assign replicas for the other tiers
      assignReplicas(targetReplicants, currentReplicants, params, segment, stats);
      // add back tier (this is for processing drop)
      targetReplicants.put(tier, targetReplicantsInTier);
    }
  }

  private static Predicate<ServerHolder> createLoadQueueSizeLimitingPredicate(
      final DruidCoordinatorRuntimeParams params
  )
  {
    final int maxSegmentsInNodeLoadingQueue = params.getCoordinatorDynamicConfig().getMaxSegmentsInNodeLoadingQueue();
    if (maxSegmentsInNodeLoadingQueue <= 0) {
      return Objects::nonNull;
    } else {
      return s -> (s != null && s.getNumberOfSegmentsInQueue() < maxSegmentsInNodeLoadingQueue);
    }
  }

  @SafeVarargs
  private static List<ServerHolder> getHolderList(
      final String tier,
      final DruidCluster druidCluster,
      final Predicate<ServerHolder> firstPredicate,
      final Predicate<ServerHolder>... otherPredicates
  )
  {
    final MinMaxPriorityQueue<ServerHolder> queue = druidCluster.getHistoricalsByTier(tier);
    if (queue == null) {
      log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", tier).emit();
      return Collections.emptyList();
    }

    final Predicate<ServerHolder> predicate = Arrays.stream(otherPredicates).reduce(firstPredicate, Predicate::and);
    return queue.stream().filter(predicate).collect(Collectors.toList());
  }

  @Nullable
  private static ServerHolder assignPrimary(
      final Object2IntMap<String> targetReplicants,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    ServerHolder topCandidate = null;
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      final int targetReplicantsInTier = entry.getIntValue();
      if (targetReplicantsInTier <= 0) {
        continue;
      }

      final String tier = entry.getKey();

      final List<ServerHolder> holders = getHolderList(tier, params.getDruidCluster(), createLoadQueueSizeLimitingPredicate(params));
      if (holders.isEmpty()) {
        continue;
      }

      final ServerHolder candidate = params.getBalancerStrategy().findNewSegmentHomeReplicator(segment, holders);
      if (candidate == null) {
        log.warn(
            "Not enough [%s] servers or node capacity to assign primary segment[%s]! Expected Replicants[%d]",
            tier, segment.getIdentifier(), targetReplicantsInTier
        );
      } else if (topCandidate == null || candidate.getServer().getPriority() > topCandidate.getServer().getPriority()) {
        topCandidate = candidate;
      }
    }

    if (topCandidate != null) {
      topCandidate.getPeon().loadSegment(segment, null);
    }

    return topCandidate;
  }

  private static void assignReplicas(
      final Object2IntMap<String> targetReplicants,
      final Object2IntMap<String> currentReplicants,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats
  )
  {
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      final String tier = entry.getKey();
      final int numAssigned = assignReplicasForTier(
          tier,
          entry.getIntValue(),
          currentReplicants.getOrDefault(tier, 0),
          params,
          getHolderList(tier, params.getDruidCluster(), createLoadQueueSizeLimitingPredicate(params)),
          segment
      );
      stats.addToTieredStat(ASSIGNED_COUNT, tier, numAssigned);
    }
  }

  private static int assignReplicasForTier(
      final String tier,
      final int targetReplicantsInTier,
      final int currentReplicantsInTier,
      final DruidCoordinatorRuntimeParams params,
      final List<ServerHolder> holders,
      final DataSegment segment
  )
  {
    final int numToAssign = targetReplicantsInTier - currentReplicantsInTier;
    if (numToAssign <= 0 || holders.isEmpty()) {
      return 0;
    }

    final ReplicationThrottler throttler = params.getReplicationManager();
    for (int numAssigned = 0; numAssigned < numToAssign; numAssigned++) {
      if (!throttler.canCreateReplicant(tier)) {
        return numAssigned;
      }

      final ServerHolder holder = params.getBalancerStrategy().findNewSegmentHomeReplicator(segment, holders);
      if (holder == null) {
        log.warn(
            "Not enough [%s] servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
            tier, segment.getIdentifier(), targetReplicantsInTier
        );
        return numAssigned;
      }
      holders.remove(holder);

      final String segmentId = segment.getIdentifier();
      final String holderHost = holder.getServer().getHost();
      throttler.registerReplicantCreation(tier, segmentId, holderHost);
      holder.getPeon().loadSegment(segment, () -> throttler.unregisterReplicantCreation(tier, segmentId, holderHost));
    }

    return numToAssign;
  }

  private static void drop(
      final Object2IntMap<String> targetReplicants,
      final Object2IntMap<String> currentReplicants,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats
  )
  {
    final DruidCluster druidCluster = params.getDruidCluster();

    // Make sure we have enough loaded replicants in the correct tiers in the cluster before doing anything
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      final String tier = entry.getKey();
      // if there are replicants loading in cluster
      if (druidCluster.hasTier(tier) && entry.getIntValue() > currentReplicants.getOrDefault(tier, 0)) {
        return;
      }
    }

    for (final Object2IntMap.Entry<String> entry : currentReplicants.object2IntEntrySet()) {
      final String tier = entry.getKey();

      final MinMaxPriorityQueue<ServerHolder> holders = druidCluster.getHistoricalsByTier(tier);

      final int numDropped;
      if (holders == null) {
        log.makeAlert("No holders found for tier[%s]", tier).emit();
        numDropped = 0;
      } else {
        final int numToDrop = entry.getIntValue() - targetReplicants.getOrDefault(tier, 0);
        numDropped = dropForTier(numToDrop, holders, segment);
      }

      stats.addToTieredStat(DROPPED_COUNT, tier, numDropped);
    }
  }

  private static int dropForTier(
      final int numToDrop,
      final MinMaxPriorityQueue<ServerHolder> holders,
      final DataSegment segment
  )
  {
    int numDropped = 0;

    final List<ServerHolder> droppedHolders = new LinkedList<>();
    while (numDropped < numToDrop) {
      final ServerHolder holder = holders.pollLast();
      if (holder == null) {
        log.warn("Wtf, holder was null?  I have no servers serving [%s]?", segment.getIdentifier());
        break;
      }

      if (holder.isServingSegment(segment)) {
        holder.getPeon().dropSegment(segment, null);
        ++numDropped;
      }
      droppedHolders.add(holder);
    }
    // add back the holders
    holders.addAll(droppedHolders);

    return numDropped;
  }

  protected static void validateTieredReplicants(final Map<String, Integer> tieredReplicants)
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
