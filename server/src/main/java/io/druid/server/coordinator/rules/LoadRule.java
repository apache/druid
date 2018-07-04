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

import io.druid.java.util.common.IAE;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordinator.BalancerStrategy;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
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

  private final Object2IntMap<String> targetReplicants = new Object2IntOpenHashMap<>();
  private final Object2IntMap<String> currentReplicants = new Object2IntOpenHashMap<>();

  // Cache to hold unused results from strategy call in assignPrimary
  private final Map<String, ServerHolder> strategyCache = new HashMap<>();

  @Override
  public CoordinatorStats run(
      final DruidCoordinator coordinator,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    try {
      // get the "snapshots" of targetReplicants and currentReplicants for assignments.
      targetReplicants.putAll(getTieredReplicants());
      currentReplicants.putAll(params.getSegmentReplicantLookup().getClusterTiers(segment.getIdentifier()));

      final CoordinatorStats stats = new CoordinatorStats();

      if (params.getAvailableSegments().contains(segment)) {
        assign(params, segment, stats);
      }

      drop(params, segment, stats);

      return stats;
    }
    finally {
      targetReplicants.clear();
      currentReplicants.clear();
      strategyCache.clear();
    }
  }

  /**
   * @param stats {@link CoordinatorStats} to accumulate assignment statistics.
   */
  private void assign(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats
  )
  {
    // if primary replica already exists or is loading
    final int loading = params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier());
    if (!currentReplicants.isEmpty() || loading > 0) {
      assignReplicas(params, segment, stats, null);
    } else {
      final ServerHolder primaryHolderToLoad = assignPrimary(params, segment);
      if (primaryHolderToLoad == null) {
        // cluster does not have any replicants and cannot identify primary holder
        // this implies that no assignment could be done
        return;
      }

      int numAssigned = 1; // 1 replica (i.e., primary replica) already assigned

      final String tier = primaryHolderToLoad.getServer().getTier();
      // assign replicas for the rest of the tier
      numAssigned += assignReplicasForTier(
          tier,
          targetReplicants.getOrDefault(tier, 0),
          numAssigned, // note that the currentReplicantsInTier is the just-assigned primary replica.
          params,
          createLoadQueueSizeLimitingPredicate(params).and(holder -> !holder.equals(primaryHolderToLoad)),
          segment
      );
      stats.addToTieredStat(ASSIGNED_COUNT, tier, numAssigned);

      // do assign replicas for the other tiers.
      assignReplicas(params, segment, stats, tier /* to skip */);
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

  private static List<ServerHolder> getFilteredHolders(
      final String tier,
      final DruidCluster druidCluster,
      final Predicate<ServerHolder> predicate
  )
  {
    final NavigableSet<ServerHolder> queue = druidCluster.getHistoricalsByTier(tier);
    if (queue == null) {
      log.makeAlert("Tier[%s] has no servers! Check your cluster configuration!", tier).emit();
      return Collections.emptyList();
    }

    return queue.stream().filter(predicate).collect(Collectors.toList());
  }

  /**
   * Iterates through each tier and find the respective segment homes; with the found segment homes, selects the one
   * with the highest priority to be the holder for the primary replica.
   */
  @Nullable
  private ServerHolder assignPrimary(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    ServerHolder topCandidate = null;
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      final int targetReplicantsInTier = entry.getIntValue();
      // sanity check: target number of replicants should be more than zero.
      if (targetReplicantsInTier <= 0) {
        continue;
      }
      final String tier = entry.getKey();

      final List<ServerHolder> holders = getFilteredHolders(
          tier,
          params.getDruidCluster(),
          createLoadQueueSizeLimitingPredicate(params)
      );
      // no holders satisfy the predicate
      if (holders.isEmpty()) {
        continue;
      }

      final ServerHolder candidate = params.getBalancerStrategy().findNewSegmentHomeReplicator(segment, holders);
      if (candidate == null) {
        log.warn(
            "No available [%s] servers or node capacity to assign primary segment[%s]! " +
            "Expected Replicants[%d]",
            tier, segment.getIdentifier(), targetReplicantsInTier
        );
      } else {
        // cache the result for later use.
        strategyCache.put(tier, candidate);
        if (topCandidate == null ||
            candidate.getServer().getPriority() > topCandidate.getServer().getPriority()) {
          topCandidate = candidate;
        }
      }
    }

    if (topCandidate != null) {
      // remove tier for primary replica
      strategyCache.remove(topCandidate.getServer().getTier());
      topCandidate.getPeon().loadSegment(segment, null);
    }

    return topCandidate;
  }

  /**
   * @param stats      {@link CoordinatorStats} to accumulate assignment statistics.
   * @param tierToSkip if not null, this tier will be skipped from doing assignment, use when primary replica was
   *                   assigned.
   */
  private void assignReplicas(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats,
      @Nullable final String tierToSkip
  )
  {
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      final String tier = entry.getKey();
      if (tier.equals(tierToSkip)) {
        continue;
      }
      final int numAssigned = assignReplicasForTier(
          tier,
          entry.getIntValue(),
          params.getSegmentReplicantLookup().getTotalReplicants(segment.getIdentifier(), tier),
          params,
          createLoadQueueSizeLimitingPredicate(params),
          segment
      );
      stats.addToTieredStat(ASSIGNED_COUNT, tier, numAssigned);
    }
  }

  /**
   * @param predicate {@link Predicate} used to pre-filter {@link ServerHolder}s retrieved from {@link DruidCluster}.
   */
  private int assignReplicasForTier(
      final String tier,
      final int targetReplicantsInTier,
      final int currentReplicantsInTier,
      final DruidCoordinatorRuntimeParams params,
      final Predicate<ServerHolder> predicate,
      final DataSegment segment
  )
  {
    final int numToAssign = targetReplicantsInTier - currentReplicantsInTier;
    // if nothing to assign
    if (numToAssign <= 0) {
      return 0;
    }

    final List<ServerHolder> holders = getFilteredHolders(tier, params.getDruidCluster(), predicate);
    // if no holders available for assignment
    if (holders.isEmpty()) {
      return 0;
    }

    final ReplicationThrottler throttler = params.getReplicationManager();
    for (int numAssigned = 0; numAssigned < numToAssign; numAssigned++) {
      if (!throttler.canCreateReplicant(tier)) {
        return numAssigned;
      }

      // Retrieves from cache if available
      ServerHolder holder = strategyCache.remove(tier);
      // Does strategy call if not in cache
      if (holder == null) {
        holder = params.getBalancerStrategy().findNewSegmentHomeReplicator(segment, holders);
      }

      if (holder == null) {
        log.warn(
            "No available [%s] servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
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

  /**
   * @param stats {@link CoordinatorStats} to accumulate assignment statistics.
   */
  private void drop(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats
  )
  {
    final DruidCluster druidCluster = params.getDruidCluster();

    // This enforces that loading is completed before we attempt to drop stuffs as a safety measure.
    if (loadingInProgress(druidCluster)) {
      return;
    }

    for (final Object2IntMap.Entry<String> entry : currentReplicants.object2IntEntrySet()) {
      final String tier = entry.getKey();

      final NavigableSet<ServerHolder> holders = druidCluster.getHistoricalsByTier(tier);

      final int numDropped;
      if (holders == null) {
        log.makeAlert("No holders found for tier[%s]", tier).emit();
        numDropped = 0;
      } else {
        final int currentReplicantsInTier = entry.getIntValue();
        final int numToDrop = currentReplicantsInTier - targetReplicants.getOrDefault(tier, 0);
        if (numToDrop > 0) {
          numDropped = dropForTier(numToDrop, holders, segment, params.getBalancerStrategy());
        } else {
          numDropped = 0;
        }
      }

      stats.addToTieredStat(DROPPED_COUNT, tier, numDropped);
    }
  }

  /**
   * Returns true if at least one tier in target replica assignment exists in cluster but does not have enough replicas.
   */
  private boolean loadingInProgress(final DruidCluster druidCluster)
  {
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      final String tier = entry.getKey();
      // if there are replicants loading in cluster
      if (druidCluster.hasTier(tier) && entry.getIntValue() > currentReplicants.getOrDefault(tier, 0)) {
        return true;
      }
    }

    return false;
  }

  private static int dropForTier(
      final int numToDrop,
      final NavigableSet<ServerHolder> holdersInTier,
      final DataSegment segment,
      final BalancerStrategy balancerStrategy
  )
  {
    int numDropped = 0;

    final NavigableSet<ServerHolder> isServingSubset =
        holdersInTier.stream().filter(s -> s.isServingSegment(segment)).collect(Collectors.toCollection(TreeSet::new));

    final Iterator<ServerHolder> iterator = balancerStrategy.pickServersToDrop(segment, isServingSubset);

    while (numDropped < numToDrop) {
      if (!iterator.hasNext()) {
        log.warn("Wtf, holder was null?  I have no servers serving [%s]?", segment.getIdentifier());
        break;
      }

      final ServerHolder holder = iterator.next();

      if (holder.isServingSegment(segment)) {
        holder.getPeon().dropSegment(segment, null);
        ++numDropped;
      } else {
        log.warn(
            "Server [%s] is no longer serving segment [%s], skipping drop.",
            holder.getServer().getName(),
            segment.getIdentifier()
        );
      }
    }

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
