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

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.BalancerStrategy;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ReplicationThrottler;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
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
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * LoadRules indicate the number of replicants a segment should have in a given tier.
 */
public abstract class LoadRule implements Rule
{
  /**
   * enum to indicate what LoadRule should execute.
   * PRIMARY_ONLY means only load primary replicants.
   * REPLICA_ONLY means only load non-primary replicants
   * ALL means to load any replicants regardless of primary status
   */
  public enum LoadRuleMode
  {
    PRIMARY_ONLY,
    REPLICA_ONLY,
    ALL
  }

  public static final String REQUIRED_CAPACITY = "requiredCapacity";
  public static final String NON_PRIMARY_ASSIGNED_COUNT = "totalNonPrimaryReplicantsLoaded";
  static final String ASSIGNED_COUNT = "assignedCount";
  static final String DROPPED_COUNT = "droppedCount";
  private static final EmittingLogger log = new EmittingLogger(LoadRule.class);

  @Override
  public CoordinatorStats run(
      final DruidCoordinator coordinator,
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    final Object2IntMap<String> targetReplicants = new Object2IntOpenHashMap<>();
    final Object2IntMap<String> currentReplicants = new Object2IntOpenHashMap<>();
    // Cache to hold unused results from strategy call in assignPrimary
    final Map<String, ServerHolder> strategyCache = new HashMap<>();

    // get the "snapshots" of targetReplicants and currentReplicants for assignments.
    targetReplicants.putAll(getTieredReplicants());
    currentReplicants.putAll(params.getSegmentReplicantLookup().getClusterTiers(segment.getId()));


    final CoordinatorStats stats = new CoordinatorStats();
    // Populate the tiered coordinator stats to ensure returned stats are in predictable state
    populateTieredStats(stats, targetReplicants);
    assign(params, segment, stats, targetReplicants, currentReplicants, strategyCache);

    // We don't do any drop calls if it is PRIMARY_ONLY execution.
    if (!params.getLoadRuleMode().equals(LoadRuleMode.PRIMARY_ONLY)) {
      drop(params, segment, stats, targetReplicants, currentReplicants);
    }

    for (String tier : targetReplicants.keySet()) {
      stats.addToTieredStat(REQUIRED_CAPACITY, tier, segment.getSize() * targetReplicants.getInt(tier));
    }
    return stats;
  }

  @Override
  public boolean canLoadSegments()
  {
    return true;
  }

  @Override
  public void updateUnderReplicated(
      Map<String, Object2LongMap<String>> underReplicatedPerTier,
      SegmentReplicantLookup segmentReplicantLookup,
      DataSegment segment
  )
  {
    getTieredReplicants().forEach((final String tier, final Integer ruleReplicants) -> {
      int currentReplicants = segmentReplicantLookup.getLoadedReplicants(segment.getId(), tier);
      Object2LongMap<String> underReplicationPerDataSource = underReplicatedPerTier.computeIfAbsent(
          tier,
          ignored -> new Object2LongOpenHashMap<>()
      );
      ((Object2LongOpenHashMap<String>) underReplicationPerDataSource).addTo(
          segment.getDataSource(),
          Math.max(ruleReplicants - currentReplicants, 0)
      );
    });
  }

  @Override
  public void updateUnderReplicatedWithClusterView(
      Map<String, Object2LongMap<String>> underReplicatedPerTier,
      SegmentReplicantLookup segmentReplicantLookup,
      DruidCluster cluster,
      DataSegment segment
  )
  {
    getTieredReplicants().forEach((final String tier, final Integer ruleReplicants) -> {
      int currentReplicants = segmentReplicantLookup.getLoadedReplicants(segment.getId(), tier);
      Object2LongMap<String> underReplicationPerDataSource = underReplicatedPerTier.computeIfAbsent(
          tier,
          ignored -> new Object2LongOpenHashMap<>()
      );
      int possibleReplicants = Math.min(ruleReplicants, cluster.getHistoricals().get(tier).size());
      log.debug(
          "ruleReplicants: [%d], possibleReplicants: [%d], currentReplicants: [%d]",
          ruleReplicants,
          possibleReplicants,
          currentReplicants
      );
      ((Object2LongOpenHashMap<String>) underReplicationPerDataSource).addTo(
          segment.getDataSource(),
          Math.max(possibleReplicants - currentReplicants, 0)
      );
    });
  }

  private boolean primaryLoadingOrLoaded(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    return params.getSegmentReplicantLookup().getTotalReplicants(segment.getId()) > 0;
  }

  /**
   * @param stats {@link CoordinatorStats} to accumulate assignment statistics.
   */
  private void assign(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats,
      final Object2IntMap<String> targetReplicants,
      final Object2IntMap<String> currentReplicants,
      final Map<String, ServerHolder> strategyCache
  )
  {
    switch (params.getLoadRuleMode()) {
      case ALL:
        if (primaryLoadingOrLoaded(params, segment)) {
          assignReplicas(params, segment, stats, null, targetReplicants, strategyCache);
        } else {
          final Optional<ServerHolder> primaryServer =
              assignPrimaryAndUpdateStats(params, segment, stats, targetReplicants, strategyCache);
          if (primaryServer.isPresent()) {
            ServerHolder primaryHolderToLoad = primaryServer.get();
            final String tier = primaryHolderToLoad.getServer().getTier();

            // assign replicas for the rest of the tier
            int numAssigned = assignReplicasForTier(
                tier,
                targetReplicants.getOrDefault(tier, 0),
                1, // note that the currentReplicantsInTier is the just-assigned primary replica.
                params,
                createLoadQueueSizeLimitingPredicate(params)
                    .and(holder -> !holder.equals(primaryHolderToLoad)),
                segment,
                strategyCache
            );
            stats.addToGlobalStat(NON_PRIMARY_ASSIGNED_COUNT, numAssigned);
            stats.addToTieredStat(ASSIGNED_COUNT, tier, numAssigned);

            // do assign replicas for the other tiers.
            assignReplicas(params, segment, stats, tier /* to skip */, targetReplicants, strategyCache);
          }
        }
        break;

      case PRIMARY_ONLY:
        if (!primaryLoadingOrLoaded(params, segment)) {
          assignPrimaryAndUpdateStats(params, segment, stats, targetReplicants, strategyCache);
        }
        break;

      case REPLICA_ONLY:
        if (!primaryLoadingOrLoaded(params, segment)) {
          return; // don't load replicas if primary has not been loaded yet
        }
        Map<String, Integer> loadedTierCounts =
            params.getSegmentReplicantLookup().getClusterTiers(segment.getId());
        Map<String, Integer> loadingTierCounts =
            params.getSegmentReplicantLookup().getLoadingTiers(segment.getId());
        Set<String> tiers = Sets.intersection(loadedTierCounts.keySet(), loadingTierCounts.keySet());
        if (tiers.size() == 1) {
          // If there is only one tier with this segment loaded, then it's the tier that contains
          // the primary.  We want to make sure we load replicants for this tier first.
          String primaryTier = tiers.iterator().next();
          int numAssigned = assignReplicasForTier(
              primaryTier,
              targetReplicants.getOrDefault(primaryTier, 0),
              currentReplicants.getOrDefault(primaryTier, 0),
              params,
              createLoadQueueSizeLimitingPredicate(params),
              segment,
              strategyCache
          );
          stats.addToGlobalStat(NON_PRIMARY_ASSIGNED_COUNT, numAssigned);
          stats.addToTieredStat(ASSIGNED_COUNT, primaryTier, numAssigned);

          // now load for the other tiers
          assignReplicas(params, segment, stats, primaryTier /* to skip */, targetReplicants, strategyCache);
        } else {
          // load all tiers
          assignReplicas(params, segment, stats, null, targetReplicants, strategyCache);
        }
        break;

      default:
        throw new ISE("Unexpected load rule mode: %s", params.getLoadRuleMode().name());
    }

  }

  private Optional<ServerHolder> assignPrimaryAndUpdateStats(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats,
      Object2IntMap<String> targetReplicants,
      Map<String, ServerHolder> strategyCache
  )
  {

    final ServerHolder primaryHolderToLoad = assignPrimary(params, segment, targetReplicants, strategyCache);
    if (primaryHolderToLoad == null) {
      // cluster does not have any replicants and cannot identify primary holder
      // this implies that no assignment could be done
      return Optional.empty();
    }
    final String tier = primaryHolderToLoad.getServer().getTier();
    stats.addToTieredStat(ASSIGNED_COUNT, tier, 1);
    return Optional.of(primaryHolderToLoad);
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
      final DataSegment segment,
      final Object2IntMap<String> targetReplicants,
      final Map<String, ServerHolder> strategyCache
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
   */
  private void assignReplicas(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment,
      final CoordinatorStats stats,
      @Nullable final String tierToSkip,
      final Object2IntMap<String> targetReplicants,
      final Map<String, ServerHolder> strategyCache
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
          segment,
          strategyCache
      );
      stats.addToGlobalStat(NON_PRIMARY_ASSIGNED_COUNT, numAssigned);
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
      final DataSegment segment,
      final Map<String, ServerHolder> strategyCache
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
      final CoordinatorStats stats,
      final Object2IntMap<String> targetReplicants,
      final Object2IntMap<String> currentReplicants
  )
  {
    final DruidCluster druidCluster = params.getDruidCluster();

    // This enforces that loading is completed before we attempt to drop stuffs as a safety measure.
    if (loadingInProgress(druidCluster, targetReplicants, currentReplicants)) {
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
  private boolean loadingInProgress(
      final DruidCluster druidCluster,
      final Object2IntMap<String> targetReplicants,
      final Object2IntMap<String> currentReplicants
  )
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
      log.warn("I have no servers serving [%s]?", segment.getId());
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

  /**
   * Ensure that {@link CoordinatorStats} is populated for all Tiers for both
   * DROPPED_COUNT and LOADED_COUNT tiered stats. This utility method helps
   * ensure that stats are fully populated even in the case where LoadRule
   * didn't update all tiered stats before finishing.
   *
   * @param stats {@link CoordinatorStats} object that we want to modify
   */
  private void populateTieredStats(
      final CoordinatorStats stats,
      final Object2IntMap<String> targetReplicants
  )
  {
    for (final Object2IntMap.Entry<String> entry : targetReplicants.object2IntEntrySet()) {
      stats.addToTieredStat(DROPPED_COUNT, entry.getKey(), 0);
      stats.addToTieredStat(ASSIGNED_COUNT, entry.getKey(), 0);
    }
  }

  public abstract Map<String, Integer> getTieredReplicants();

  public abstract int getNumReplicants(String tier);
}
