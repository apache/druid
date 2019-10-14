/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator.rules;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

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
  public static final String REQUIRED_CAPACITY = "requiredCapacity";

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
      currentReplicants.putAll(params.getSegmentReplicantLookup().getClusterTiers(segment.getId()));

      final CoordinatorStats stats = new CoordinatorStats();
      assign(params, segment, stats);

      drop(params, segment, stats);
      for (String tier : targetReplicants.keySet()) {
        stats.addToTieredStat(REQUIRED_CAPACITY, tier, segment.getSize() * targetReplicants.getInt(tier));
      }
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
    final int loading = params.getSegmentReplicantLookup().getTotalReplicants(segment.getId());
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
    Predicate<ServerHolder> isActive = s -> !s.isDecommissioning();
    return queue.stream().filter(isActive.and(predicate)).collect(Collectors.toList());
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

      String noAvailability = StringUtils.format(
          "No available [%s] servers or node capacity to assign primary segment[%s]! Expected Replicants[%d]",
          tier,
          segment.getId(),
          targetReplicantsInTier
      );

      final List<ServerHolder> holders = getFilteredHolders(
          tier,
          params.getDruidCluster(),
          createLoadQueueSizeLimitingPredicate(params)
      );
      // no holders satisfy the predicate
      if (holders.isEmpty()) {
        log.warn(noAvailability);
        continue;
      }

      final ServerHolder candidate = params.getBalancerStrategy().findNewSegmentHomeReplicator(segment, holders);
      if (candidate == null) {
        log.warn(noAvailability);
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
      log.info(
          "Assigning 'primary' for segment [%s] to server [%s] in tier [%s]",
          segment.getId(),
          topCandidate.getServer().getName(),
          topCandidate.getServer().getTier()
      );
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
        log.info("Skipping replica assignment for tier [%s]", tier);
        continue;
      }
      final int numAssigned = assignReplicasForTier(
          tier,
          entry.getIntValue(),
          params.getSegmentReplicantLookup().getTotalReplicants(segment.getId(), tier),
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

    String noAvailability = StringUtils.format(
        "No available [%s] servers or node capacity to assign segment[%s]! Expected Replicants[%d]",
        tier,
        segment.getId(),
        targetReplicantsInTier
    );

    final List<ServerHolder> holders = getFilteredHolders(tier, params.getDruidCluster(), predicate);
    // if no holders available for assignment
    if (holders.isEmpty()) {
      log.warn(noAvailability);
      return 0;
    }

    final ReplicationThrottler throttler = params.getReplicationManager();
    for (int numAssigned = 0; numAssigned < numToAssign; numAssigned++) {
      if (!throttler.canCreateReplicant(tier)) {
        log.info("Throttling replication for segment [%s] in tier [%s]", segment.getId(), tier);
        return numAssigned;
      }

      // Retrieves from cache if available
      ServerHolder holder = strategyCache.remove(tier);
      // Does strategy call if not in cache
      if (holder == null) {
        holder = params.getBalancerStrategy().findNewSegmentHomeReplicator(segment, holders);
      }

      if (holder == null) {
        log.warn(noAvailability);
        return numAssigned;
      }
      holders.remove(holder);

      final SegmentId segmentId = segment.getId();
      final String holderHost = holder.getServer().getHost();
      throttler.registerReplicantCreation(tier, segmentId, holderHost);
      log.info(
          "Assigning 'replica' for segment [%s] to server [%s] in tier [%s]",
          segment.getId(),
          holder.getServer().getName(),
          holder.getServer().getTier()
      );
      holder.getPeon().loadSegment(segment, () -> throttler.unregisterReplicantCreation(tier, segmentId));
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
      log.info("Loading in progress, skipping drop until loading is complete");
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
    Map<Boolean, TreeSet<ServerHolder>> holders = holdersInTier.stream()
                                                               .filter(s -> s.isServingSegment(segment))
                                                               .collect(Collectors.partitioningBy(
                                                                   ServerHolder::isDecommissioning,
                                                                   Collectors.toCollection(TreeSet::new)
                                                               ));
    TreeSet<ServerHolder> decommissioningServers = holders.get(true);
    TreeSet<ServerHolder> activeServers = holders.get(false);
    int left = dropSegmentFromServers(balancerStrategy, segment, decommissioningServers, numToDrop);
    if (left > 0) {
      left = dropSegmentFromServers(balancerStrategy, segment, activeServers, left);
    }
    if (left != 0) {
      log.warn("Wtf, holder was null?  I have no servers serving [%s]?", segment.getId());
    }
    return numToDrop - left;
  }

  private static int dropSegmentFromServers(
      BalancerStrategy balancerStrategy,
      DataSegment segment,
      NavigableSet<ServerHolder> holders, int numToDrop
  )
  {
    final Iterator<ServerHolder> iterator = balancerStrategy.pickServersToDrop(segment, holders);

    while (numToDrop > 0) {
      if (!iterator.hasNext()) {
        break;
      }

      final ServerHolder holder = iterator.next();
      if (holder.isServingSegment(segment)) {
        log.info(
            "Dropping segment [%s] on server [%s] in tier [%s]",
            segment.getId(),
            holder.getServer().getName(),
            holder.getServer().getTier()
        );
        holder.getPeon().dropSegment(segment, null);
        numToDrop--;
      } else {
        log.warn(
            "Server [%s] is no longer serving segment [%s], skipping drop.",
            holder.getServer().getName(),
            segment.getId()
        );
      }
    }
    return numToDrop;
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
