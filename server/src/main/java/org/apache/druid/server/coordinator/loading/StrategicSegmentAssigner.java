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

package org.apache.druid.server.coordinator.loading;

import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.server.coordinator.rules.SegmentActionHandler;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Used by the coordinator in each run for segment loading, dropping, balancing
 * and broadcasting.
 * <p>
 * An instance of this class is freshly created for each coordinator run.
 */
@NotThreadSafe
public class StrategicSegmentAssigner implements SegmentActionHandler
{
  private final SegmentLoadQueueManager loadQueueManager;
  private final DruidCluster cluster;
  private final CoordinatorRunStats stats;
  private final SegmentReplicaCountMap replicaCountMap;
  private final ReplicationThrottler replicationThrottler;
  private final RoundRobinServerSelector serverSelector;
  private final BalancerStrategy strategy;

  private final boolean useRoundRobinAssignment;
  private final Map<String, Set<String>> historicalTierAliases;
  private final Map<String, String> tierToAliasName;

  private final Map<String, Set<String>> datasourceToInvalidLoadTiers = new HashMap<>();
  private final Map<String, Integer> tierToHistoricalCount = new HashMap<>();
  private final Map<String, Set<SegmentId>> segmentsToDelete = new HashMap<>();
  private final Map<String, Set<DataSegment>> segmentsWithZeroRequiredReplicas = new HashMap<>();
  private final Set<DataSegment> broadcastSegments = new HashSet<>();

  public StrategicSegmentAssigner(
      SegmentLoadQueueManager loadQueueManager,
      DruidCluster cluster,
      BalancerStrategy strategy,
      SegmentLoadingConfig loadingConfig,
      CoordinatorRunStats stats
  )
  {
    this.stats = stats;
    this.cluster = cluster;
    this.strategy = strategy;
    this.loadQueueManager = loadQueueManager;
    this.replicaCountMap = SegmentReplicaCountMap.create(cluster);
    this.replicationThrottler = createReplicationThrottler(cluster, loadingConfig);
    this.useRoundRobinAssignment = loadingConfig.isUseRoundRobinSegmentAssignment();
    this.serverSelector = useRoundRobinAssignment ? new RoundRobinServerSelector(cluster) : null;
    this.historicalTierAliases = loadingConfig.getHistoricalTierAliases();
    this.tierToAliasName = loadingConfig.getTierToAliasName();

    cluster.getManagedHistoricals().forEach(
        (tier, historicals) -> tierToHistoricalCount.put(tier, historicals.size())
    );
  }

  public SegmentReplicationStatus getReplicationStatus()
  {
    return replicaCountMap.toReplicationStatus();
  }

  public Map<String, Set<SegmentId>> getSegmentsToDelete()
  {
    return segmentsToDelete;
  }

  public Map<String, Set<DataSegment>> getSegmentsWithZeroRequiredReplicas()
  {
    return segmentsWithZeroRequiredReplicas;
  }

  public Map<String, Set<String>> getDatasourceToInvalidLoadTiers()
  {
    return datasourceToInvalidLoadTiers;
  }

  /**
   * Moves the given segment from the source server to an eligible destination
   * server.
   * <p>
   * An eligible destination server must:
   * <ul>
   *   <li>be present in the given list of destination servers</li>
   *   <li>belong to the same tier as the source server</li>
   *   <li>not already be serving or loading a replica of the segment</li>
   *   <li>have enough space to load the segment</li>
   * </ul>
   * <p>
   * The segment is not moved if:
   * <ul>
   *   <li>there is no eligible destination server</li>
   *   <li>or segment is already optimally placed</li>
   *   <li>or some other error occurs</li>
   * </ul>
   */
  public boolean moveSegment(
      DataSegment segment,
      ServerHolder sourceServer,
      List<ServerHolder> destinationServers
  )
  {
    final String tier = sourceServer.getServer().getTier();
    final List<ServerHolder> eligibleDestinationServers =
        destinationServers.stream()
                          .filter(s -> s.getServer().getTier().equals(tier))
                          .filter(s -> s.canLoadSegment(segment))
                          .collect(Collectors.toList());

    if (eligibleDestinationServers.isEmpty()) {
      incrementSkipStat(Stats.Segments.MOVE_SKIPPED, "No eligible server", segment, sourceServer);
      return false;
    }

    // If the source server is not decommissioning, move can be skipped if the
    // segment is already optimally placed
    if (!sourceServer.isDecommissioning()) {
      eligibleDestinationServers.add(sourceServer);
    }

    final ServerHolder destination =
        strategy.findDestinationServerToMoveSegment(segment, sourceServer, eligibleDestinationServers);

    if (destination == null || destination.getServer().equals(sourceServer.getServer())) {
      incrementSkipStat(Stats.Segments.MOVE_SKIPPED, "Optimally placed", segment, sourceServer);
      return false;
    } else if (moveSegment(segment, sourceServer, destination)) {
      incrementStat(Stats.Segments.MOVED, segment, tier, 1);
      return true;
    } else {
      incrementSkipStat(Stats.Segments.MOVE_SKIPPED, "Encountered error", segment, sourceServer);
      return false;
    }
  }

  /**
   * Moves the given segment from serverA to serverB.
   */
  private boolean moveSegment(DataSegment segment, ServerHolder serverA, ServerHolder serverB)
  {
    final String tier = serverA.getServer().getTier();
    if (serverA.isLoadingSegment(segment)) {
      // Cancel the load on serverA and load on serverB instead
      if (serverA.cancelOperation(SegmentAction.LOAD, segment)) {
        int loadedCountOnTier = replicaCountMap.get(segment.getId(), tier)
                                               .loadedNotDropping();
        if (loadedCountOnTier >= 1) {
          return replicateSegment(segment, serverB, null);
        } else {
          return loadSegment(segment, serverB, null);
        }
      }

      // Could not cancel load, let the segment load on serverA and count it as unmoved
      return false;
    } else if (serverA.isServingSegment(segment)) {
      return loadQueueManager.moveSegment(segment, serverA, serverB);
    } else {
      return false;
    }
  }

  /**
   * Resolves alias tiers in the given tier-to-replica-count map. For each tier
   * that is a key in {@link #historicalTierAliases}, the entry is replaced by
   * one entry per alias value — each receiving the same replica count. The alias
   * key itself is treated as a virtual tier and is not kept in the result. Tiers
   * not present in {@link #historicalTierAliases} are passed through unchanged.
   * Explicit counts already present in the map are not overwritten by alias expansion.
   */
  private Map<String, Integer> expandWithAliases(Map<String, Integer> tierToReplicaCount)
  {
    if (historicalTierAliases.isEmpty()) {
      return tierToReplicaCount;
    }

    final Map<String, Integer> expanded = new HashMap<>();
    tierToReplicaCount.forEach((tier, replicaCount) -> {
      final Set<String> aliases = historicalTierAliases.get(tier);
      if (aliases != null) {
        // tier is a virtual alias key — replace it with its real tiers
        aliases.forEach(alias -> expanded.putIfAbsent(alias, replicaCount));
      } else {
        expanded.put(tier, replicaCount);
      }
    });
    return expanded;
  }

  @Override
  public void replicateSegment(DataSegment segment, Map<String, Integer> tierToReplicaCount)
  {
    final Map<String, Integer> effectiveTierToReplicaCount = expandWithAliases(tierToReplicaCount);
    final Set<String> allTiersInCluster = Sets.newHashSet(cluster.getTierNames());

    if (effectiveTierToReplicaCount.isEmpty()) {
      // Track the counts for a segment even if it requires 0 replicas on all tiers
      replicaCountMap.computeIfAbsent(segment.getId(), DruidServer.DEFAULT_TIER);
    } else {
      // Identify empty tiers and determine total required replicas
      effectiveTierToReplicaCount.forEach((tier, requiredReplicas) -> {
        reportTierCapacityStats(segment, requiredReplicas, tier);

        SegmentReplicaCount replicaCount = replicaCountMap.computeIfAbsent(segment.getId(), tier);
        replicaCount.setRequired(requiredReplicas, tierToHistoricalCount.getOrDefault(tier, 0));

        if (!allTiersInCluster.contains(tier)) {
          datasourceToInvalidLoadTiers.computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
                                      .add(tier);
        }
      });
    }

    SegmentReplicaCount replicaCountInCluster = replicaCountMap.getTotal(segment.getId());
    if (replicaCountInCluster.required() <= 0) {
      segmentsWithZeroRequiredReplicas
          .computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
          .add(segment);
    }

    final int replicaSurplus = replicaCountInCluster.loadedNotDropping()
                               - replicaCountInCluster.requiredAndLoadable();

    // Update replicas in every tier
    int dropsQueued = 0;
    for (String tier : allTiersInCluster) {
      dropsQueued += updateReplicasInTier(
          segment,
          tier,
          effectiveTierToReplicaCount.getOrDefault(tier, 0),
          replicaSurplus - dropsQueued
      );
    }
  }

  /**
   * Fingerprint-aware partial-load reconciler. Mirrors {@link #replicateSegment} for replica-count and surplus
   * accounting, but per-tier the load/drop decisions are made by classifying replicas as matching or stale relative
   * to the requested {@link PartialLoadProfile#fingerprint()} and applying the load-then-drop swap pattern: stale
   * replicas keep serving until matching replicas have actually loaded.
   */
  @Override
  public void replicateSegmentPartially(
      DataSegment segment,
      PartialLoadProfile profile,
      Map<String, Integer> tierToReplicaCount
  )
  {
    final Map<String, Integer> effectiveTierToReplicaCount = expandWithAliases(tierToReplicaCount);
    final Set<String> allTiersInCluster = Sets.newHashSet(cluster.getTierNames());

    if (effectiveTierToReplicaCount.isEmpty()) {
      replicaCountMap.computeIfAbsent(segment.getId(), DruidServer.DEFAULT_TIER);
    } else {
      effectiveTierToReplicaCount.forEach((tier, requiredReplicas) -> {
        reportTierCapacityStats(segment, requiredReplicas, tier);

        SegmentReplicaCount replicaCount = replicaCountMap.computeIfAbsent(segment.getId(), tier);
        replicaCount.setRequired(requiredReplicas, tierToHistoricalCount.getOrDefault(tier, 0));

        if (!allTiersInCluster.contains(tier)) {
          datasourceToInvalidLoadTiers.computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
                                      .add(tier);
        }
      });
    }

    final SegmentReplicaCount replicaCountInCluster = replicaCountMap.getTotal(segment.getId());
    if (replicaCountInCluster.required() <= 0) {
      segmentsWithZeroRequiredReplicas
          .computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
          .add(segment);
    }

    final int replicaSurplus = replicaCountInCluster.loadedNotDropping()
                               - replicaCountInCluster.requiredAndLoadable();

    int dropsQueued = 0;
    for (String tier : allTiersInCluster) {
      dropsQueued += updateReplicasInTierPartial(
          segment,
          profile,
          tier,
          effectiveTierToReplicaCount.getOrDefault(tier, 0),
          replicaSurplus - dropsQueued
      );
    }
  }

  /**
   * Per-tier reconciliation under the partial-load model; partial load equivalent of
   * {@link #updateReplicasInTier(DataSegment, String, int, int)}
   *
   * <h3>Algorithm</h3>
   * <ol>
   *   <li><b>Classify.</b> Build {@link PartialSegmentStatusInTier} for this tier; every server falls into at most
   *       one of: matching-loaded, stale-loaded (optionally also eligible-for-additive-reload),
   *       matching-in-flight, stale-in-flight, eligible-for-fresh-load, or unclassified (drop/move pending; see
   *       {@link PartialSegmentStatusInTier#classify} for why). Matching means the announced fingerprint equals
   *       this request's fingerprint; stale is anything else, including a non-profile regular full-load replica.</li>
   *   <li><b>Compute matching count:</b> matching-loaded + matching-in-flight − pending-move-drop.</li>
   *   <li><b>If matching count is short of {@code requiredReplicas}</b> (deficit):
   *     <ol type="a">
   *       <li>Cancel stale-in-flight loads to free their slots. Canceled servers become same-run fresh-load
   *           destinations.</li>
   *       <li>Queue fresh partial-load requests up to the deficit. Destination preference order, applied in
   *           {@link #loadPartialReplicas}:
   *           <ol>
   *             <li>Empty servers (clean slate, no in-place mutation needed).</li>
   *             <li>Servers whose stale-in-flight load we just canceled in (a), their slot is now free.</li>
   *             <li>Stale-loaded servers eligible for additive reload (the historical fills in the missing parts in
   *                 place). This is the fallback path that mitigates the "no spare server" stuck state.
   *                 Same-run dedup is enforced by {@link ServerHolder#startOperation}, which rejects a second
   *                 queue attempt on a server whose segment is already queued.</li>
   *           </ol>
   *       </li>
   *     </ol>
   *   </li>
   *   <li><b>If matching count exceeds requirement</b> (surplus): drop the excess like the full-load surplus
   *       path.</li>
   *   <li><b>Drop stale-loaded replicas</b> only when the count of <em>actually-loaded</em> matching replicas
   *       already meets the requirement. This preserves availability across the swap: stale replicas keep serving
   *       until matching replicas have completed loading and announced, then get dropped. The
   *       {@code maxReplicasToDrop} budget caps how many drops we queue per coordinator run to avoid drop
   *       storms.</li>
   * </ol>
   *
   * <h3>Returns</h3>
   * Total number of drop operations queued on this tier (matching surplus + stale), used to budget cross-tier drop
   * pressure across subsequent tier reconciliations in the same run.
   */
  private int updateReplicasInTierPartial(
      DataSegment segment,
      PartialLoadProfile profile,
      String tier,
      int requiredReplicas,
      int maxReplicasToDrop
  )
  {
    final SegmentReplicaCount replicaCountOnTier = replicaCountMap.get(segment.getId(), tier);
    final int movingReplicas = replicaCountOnTier.moving();
    final int moveCompletedPendingDrop = Math.max(0, replicaCountOnTier.moveCompletedPendingDrop());

    final PartialSegmentStatusInTier status = new PartialSegmentStatusInTier(
        segment,
        profile.fingerprint(),
        cluster.getManagedHistoricalsByTier(tier)
    );

    final int matchingProjected = status.getMatchingLoaded().size()
                                  + status.getMatchingInFlight().size()
                                  - moveCompletedPendingDrop;
    final boolean shouldCancelMoves = requiredReplicas == 0 && movingReplicas > 0;

    // If everything's already in shape and no stale work, fast-exit.
    if (matchingProjected == requiredReplicas
        && !shouldCancelMoves
        && status.getStaleInFlight().isEmpty()
        && (status.getStaleLoaded().isEmpty() || status.getMatchingLoaded().size() < requiredReplicas)) {
      return 0;
    }

    if (shouldCancelMoves) {
      // Convert to SegmentStatusInTier for the existing cancelOperations move-cancel helper.
      final SegmentStatusInTier vanillaStatus =
          new SegmentStatusInTier(segment, cluster.getManagedHistoricalsByTier(tier));
      cancelOperations(SegmentAction.MOVE_TO, movingReplicas, segment, vanillaStatus);
      cancelOperations(SegmentAction.MOVE_FROM, movingReplicas, segment, vanillaStatus);
    }

    // Cancel stale in-flight: when there's a matching deficit we want their slots back; when requirement is 0 we
    // want them gone unconditionally so we don't realize a stale fingerprint nobody asked for. Canceled servers
    // become eligible for a fresh matching load later in this same run.
    final int matchingDeficit = requiredReplicas - matchingProjected;
    final List<ServerHolder> canceledStaleServers = new ArrayList<>();
    if (matchingDeficit > 0 || requiredReplicas == 0) {
      final int toCancel = requiredReplicas == 0 ? status.getStaleInFlight().size() : matchingDeficit;
      cancelLoadsOnServers(segment, status.getStaleInFlight(), toCancel, canceledStaleServers);
      if (!canceledStaleServers.isEmpty()) {
        incrementStat(Stats.Segments.PARTIAL_STALE_CANCELLED, segment, tier, canceledStaleServers.size());
      }
    }

    // Queue fresh matching loads to fill the deficit.
    if (matchingDeficit > 0) {
      final int numLoadedReplicas = status.getMatchingLoaded().size() + status.getStaleLoaded().size();
      final int queued = loadPartialReplicas(
          matchingDeficit,
          numLoadedReplicas,
          segment,
          tier,
          status,
          canceledStaleServers,
          profile
      );
      if (queued > 0) {
        incrementStat(Stats.Segments.PARTIAL_ASSIGNED, segment, tier, queued);
      }
    }

    int dropsQueuedOnTier = 0;
    int dropBudget = maxReplicasToDrop;

    // Surplus matching: drop excess matching replicas. Same shape as the full-load surplus path.
    // Note: cancellations of matching in-flight loads here are intentionally not emitted as a separate stat. Unlike
    // the stale-in-flight cancellations above (a distinct "rule churn" event), these are a surplus-absorption
    // mechanism: each canceled in-flight load just reduces how many physical drops we'd otherwise need to queue
    // (see `surplus - canceledMatching.size()` below). The full-load path in `updateReplicasInTier` follows the same
    // convention: canceled surplus loads aren't statted, only the resulting drops are.
    if (matchingProjected > requiredReplicas) {
      final int surplus = matchingProjected - requiredReplicas;
      final List<ServerHolder> canceledMatching = new ArrayList<>();
      cancelLoadsOnServers(segment, status.getMatchingInFlight(), surplus, canceledMatching);
      final int numToDrop = Math.min(surplus - canceledMatching.size(), dropBudget);
      if (numToDrop > 0) {
        final int dropped = dropFromList(numToDrop, segment, status.getMatchingLoaded());
        incrementStat(Stats.Segments.DROPPED, segment, tier, dropped);
        dropsQueuedOnTier += dropped;
        dropBudget -= dropped;
      }
    }

    // Stale drops: only safe once matching-loaded already satisfies the requirement (i.e., truly-serving matching
    // replicas already cover the rule before we touch any stale). This is the "load then drop" half of the swap.
    if (status.getMatchingLoaded().size() >= requiredReplicas
        && !status.getStaleLoaded().isEmpty()
        && dropBudget > 0) {
      final int numToDrop = Math.min(status.getStaleLoaded().size(), dropBudget);
      final int dropped = dropFromList(numToDrop, segment, status.getStaleLoaded());
      if (dropped > 0) {
        incrementStat(Stats.Segments.PARTIAL_STALE_DROPPED, segment, tier, dropped);
        dropsQueuedOnTier += dropped;
      }
    }

    return dropsQueuedOnTier;
  }

  /**
   * Queues fresh partial-load requests on up to {@code numToLoad} eligible servers. Preference order: empty
   * (fresh-load) servers first; then servers whose stale-fingerprint in-flight loads were just canceled (their slot
   * is now free); then stale-loaded servers (additive reload; the historical fills missing parts in place). The last
   * fallback is what mitigates the "tier saturated with stale" stuck state.
   */
  private int loadPartialReplicas(
      int numToLoad,
      int numLoadedReplicas,
      DataSegment segment,
      String tier,
      PartialSegmentStatusInTier status,
      List<ServerHolder> canceledStaleServers,
      PartialLoadProfile profile
  )
  {
    final boolean isAlreadyLoadedOnTier = numLoadedReplicas >= 1;

    if (isAlreadyLoadedOnTier && replicationThrottler.isReplicationThrottledForTier(tier)) {
      return 0;
    }

    final List<ServerHolder> destinations = new ArrayList<>(
        status.getEligibleForFreshLoad().size()
        + canceledStaleServers.size()
        + status.getEligibleForAdditiveReload().size()
    );
    destinations.addAll(status.getEligibleForFreshLoad());
    destinations.addAll(canceledStaleServers);
    destinations.addAll(status.getEligibleForAdditiveReload());

    if (destinations.isEmpty()) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "No eligible server", segment, tier);
      return 0;
    }

    int numLoadsQueued = 0;
    for (ServerHolder server : destinations) {
      if (numLoadsQueued >= numToLoad) {
        break;
      }
      final boolean queuedSuccessfully = isAlreadyLoadedOnTier
                                         ? replicateSegment(segment, server, profile)
                                         : loadSegment(segment, server, profile);
      if (queuedSuccessfully) {
        ++numLoadsQueued;
      }
    }
    return numLoadsQueued;
  }

  /**
   * Cancels up to {@code numToCancel} in-flight load operations across the given list of servers. Successfully
   * canceled servers are appended to {@code canceledOut} so the caller can re-target them as fresh-load
   * destinations within the same run. Used both to release slots taken by stale-fingerprint in-flight loads and to
   * reduce a matching surplus.
   */
  private void cancelLoadsOnServers(
      DataSegment segment,
      List<ServerHolder> servers,
      int numToCancel,
      List<ServerHolder> canceledOut
  )
  {
    if (numToCancel <= 0) {
      return;
    }
    for (ServerHolder server : servers) {
      if (canceledOut.size() >= numToCancel) {
        break;
      }
      // Try LOAD then REPLICATE; the queued action depends on whether this was a primary or a replica.
      if (server.cancelOperation(SegmentAction.LOAD, segment)
          || server.cancelOperation(SegmentAction.REPLICATE, segment)) {
        canceledOut.add(server);
      }
    }
  }

  /**
   * Drops the segment from up to {@code numToDrop} servers in the given list, preferring decommissioning servers
   * first (they're trying to shed load anyway). Returns the number of drop operations queued.
   */
  private int dropFromList(int numToDrop, DataSegment segment, List<ServerHolder> candidates)
  {
    if (numToDrop <= 0 || candidates.isEmpty()) {
      return 0;
    }
    final List<ServerHolder> ordered = new ArrayList<>(candidates);
    ordered.sort((a, b) -> Boolean.compare(b.isDecommissioning(), a.isDecommissioning()));
    int dropped = 0;
    for (ServerHolder server : ordered) {
      if (dropped >= numToDrop) {
        break;
      }
      if (loadQueueManager.dropSegment(segment, server)) {
        ++dropped;
      }
    }
    return dropped;
  }

  /**
   * Queues load or drop operations on this tier based on the required
   * number of replicas and the current state.
   * <p>
   * The {@code maxReplicasToDrop} helps to maintain the required level of
   * replication in the cluster. This ensures that segment read concurrency does
   * not suffer during a tier shift or load rule change.
   * <p>
   * Returns the number of new drop operations queued on this tier.
   */
  private int updateReplicasInTier(
      DataSegment segment,
      String tier,
      int requiredReplicas,
      int maxReplicasToDrop
  )
  {
    final SegmentReplicaCount replicaCountOnTier
        = replicaCountMap.get(segment.getId(), tier);

    final int projectedReplicas = replicaCountOnTier.loadedNotDropping()
                                  + replicaCountOnTier.loading()
                                  - Math.max(0, replicaCountOnTier.moveCompletedPendingDrop());

    final int movingReplicas = replicaCountOnTier.moving();
    final boolean shouldCancelMoves = requiredReplicas == 0 && movingReplicas > 0;

    // Check if there is any action required on this tier
    if (projectedReplicas == requiredReplicas && !shouldCancelMoves) {
      return 0;
    }

    final SegmentStatusInTier segmentStatus =
        new SegmentStatusInTier(segment, cluster.getManagedHistoricalsByTier(tier));

    // Cancel all moves in this tier if it does not need to have replicas
    if (shouldCancelMoves) {
      cancelOperations(SegmentAction.MOVE_TO, movingReplicas, segment, segmentStatus);
      cancelOperations(SegmentAction.MOVE_FROM, movingReplicas, segment, segmentStatus);
    }

    // Cancel drops and queue loads if the projected count is below the requirement
    if (projectedReplicas < requiredReplicas) {
      int replicaDeficit = requiredReplicas - projectedReplicas;
      int canceledDrops =
          cancelOperations(SegmentAction.DROP, replicaDeficit, segment, segmentStatus);

      // Canceled drops can be counted as loaded replicas, thus reducing deficit
      int numReplicasToLoad = replicaDeficit - canceledDrops;
      if (numReplicasToLoad > 0) {
        int numLoadedReplicas = replicaCountOnTier.loadedNotDropping() + canceledDrops;
        int numLoadsQueued = loadReplicas(numReplicasToLoad, numLoadedReplicas, segment, tier, segmentStatus, null);
        incrementStat(Stats.Segments.ASSIGNED, segment, tier, numLoadsQueued);
      }
    }

    // Cancel loads and queue drops if the projected count exceeds the requirement
    if (projectedReplicas > requiredReplicas) {
      int replicaSurplus = projectedReplicas - requiredReplicas;
      int canceledLoads =
          cancelOperations(SegmentAction.LOAD, replicaSurplus, segment, segmentStatus);

      int numReplicasToDrop = Math.min(replicaSurplus - canceledLoads, maxReplicasToDrop);
      if (numReplicasToDrop > 0) {
        int dropsQueuedOnTier = dropReplicas(numReplicasToDrop, segment, tier, segmentStatus);
        incrementStat(Stats.Segments.DROPPED, segment, tier, dropsQueuedOnTier);
        return dropsQueuedOnTier;
      }
    }

    return 0;
  }

  private void reportTierCapacityStats(DataSegment segment, int requiredReplicas, String tier)
  {
    final RowKey rowKey = tierRowKey(tier);
    stats.updateMax(Stats.Tier.REPLICATION_FACTOR, rowKey, requiredReplicas);
    stats.add(Stats.Tier.REQUIRED_CAPACITY, rowKey, segment.getSize() * requiredReplicas);
  }

  /**
   * Builds a {@link RowKey} for the given physical tier, additionally tagging it
   * with {@link Dimension#TIER_ALIAS} when the tier belongs to an alias. This lets
   * metrics for aliased tiers (e.g. blue/green pairs) be aggregated by alias.
   */
  private RowKey tierRowKey(String tier)
  {
    final String alias = tierToAliasName.get(tier);
    if (alias == null) {
      return RowKey.of(Dimension.TIER, tier);
    }
    return RowKey.with(Dimension.TIER, tier).and(Dimension.TIER_ALIAS, alias);
  }

  @Override
  public void broadcastSegment(DataSegment segment)
  {
    final Object2IntOpenHashMap<String> tierToRequiredReplicas = new Object2IntOpenHashMap<>();
    for (ServerHolder server : cluster.getAllManagedServers()) {
      // Ignore servers which are not broadcast targets
      if (!server.getServer().getType().isSegmentBroadcastTarget()) {
        continue;
      }

      final String tier = server.getServer().getTier();

      // Drop from decommissioning servers and load on active servers
      int numDropsQueued = 0;
      int numLoadsQueued = 0;
      if (server.isDecommissioning()) {
        numDropsQueued += dropBroadcastSegment(segment, server) ? 1 : 0;
      } else {
        tierToRequiredReplicas.addTo(tier, 1);
        numLoadsQueued += loadBroadcastSegment(segment, server) ? 1 : 0;
      }

      if (numLoadsQueued > 0) {
        incrementStat(Stats.Segments.ASSIGNED, segment, tier, numLoadsQueued);
      }
      if (numDropsQueued > 0) {
        incrementStat(Stats.Segments.DROPPED, segment, tier, numDropsQueued);
      }
    }

    // Update required replica counts
    tierToRequiredReplicas.object2IntEntrySet().fastForEach(
        entry -> replicaCountMap.computeIfAbsent(segment.getId(), entry.getKey())
                                .setRequired(entry.getIntValue(), entry.getIntValue())
    );

    broadcastSegments.add(segment);
  }

  @Override
  public void deleteSegment(DataSegment segment)
  {
    segmentsToDelete
        .computeIfAbsent(segment.getDataSource(), ds -> new HashSet<>())
        .add(segment.getId());
  }

  /**
   * Loads the broadcast segment if it is not already loaded on the given server.
   * Returns true only if the segment was successfully queued for load on the server.
   */
  private boolean loadBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    if (server.isServingSegment(segment) || server.isLoadingSegment(segment)) {
      return false;
    } else if (server.isDroppingSegment(segment)) {
      return server.cancelOperation(SegmentAction.DROP, segment);
    } else if (server.canLoadSegment(segment)) {
      return loadSegment(segment, server, null);
    }

    final String skipReason;
    if (server.getAvailableSize() < segment.getSize()) {
      skipReason = "Not enough disk space";
    } else if (server.isLoadQueueFull()) {
      skipReason = "Load queue is full";
    } else {
      skipReason = "Unknown error";
    }

    incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, skipReason, segment, server);
    return false;
  }

  public Set<DataSegment> getBroadcastSegments()
  {
    return broadcastSegments;
  }

  /**
   * Drops the broadcast segment if it is loaded on the given server.
   * Returns true only if the segment was successfully queued for drop on the server.
   */
  private boolean dropBroadcastSegment(DataSegment segment, ServerHolder server)
  {
    if (server.isLoadingSegment(segment)) {
      return server.cancelOperation(SegmentAction.LOAD, segment);
    } else if (server.isServingSegment(segment)) {
      return loadQueueManager.dropSegment(segment, server);
    } else {
      return false;
    }
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from a tier.
   * Tries to drop replicas first from decommissioning servers and then from
   * active servers.
   * <p>
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicas(
      final int numToDrop,
      DataSegment segment,
      String tier,
      SegmentStatusInTier segmentStatus
  )
  {
    if (numToDrop <= 0) {
      return 0;
    }

    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToDrop();
    if (eligibleServers.isEmpty()) {
      incrementSkipStat(Stats.Segments.DROP_SKIPPED, "No eligible server", segment, tier);
      return 0;
    }

    // Keep eligible servers sorted by most full first
    final TreeSet<ServerHolder> eligibleLiveServers = new TreeSet<>(Comparator.reverseOrder());
    final TreeSet<ServerHolder> eligibleDyingServers = new TreeSet<>(Comparator.reverseOrder());
    for (ServerHolder server : eligibleServers) {
      if (server.isDecommissioning()) {
        eligibleDyingServers.add(server);
      } else {
        eligibleLiveServers.add(server);
      }
    }

    // Drop as many replicas as possible from decommissioning servers
    int remainingNumToDrop = numToDrop;
    int numDropsQueued =
        dropReplicasFromServers(remainingNumToDrop, segment, eligibleDyingServers.iterator(), tier);

    // Drop replicas from active servers if required
    if (numToDrop > numDropsQueued) {
      remainingNumToDrop = numToDrop - numDropsQueued;
      Iterator<ServerHolder> serverIterator =
          (useRoundRobinAssignment || eligibleLiveServers.size() <= remainingNumToDrop)
          ? eligibleLiveServers.iterator()
          : strategy.findServersToDropSegment(segment, new ArrayList<>(eligibleLiveServers));
      numDropsQueued += dropReplicasFromServers(remainingNumToDrop, segment, serverIterator, tier);
    }

    return numDropsQueued;
  }

  /**
   * Queues drop of {@code numToDrop} replicas of the segment from the servers.
   * Returns the number of successfully queued drop operations.
   */
  private int dropReplicasFromServers(
      int numToDrop,
      DataSegment segment,
      Iterator<ServerHolder> serverIterator,
      String tier
  )
  {
    int numDropsQueued = 0;
    while (numToDrop > numDropsQueued && serverIterator.hasNext()) {
      ServerHolder holder = serverIterator.next();
      boolean dropped = loadQueueManager.dropSegment(segment, holder);

      if (dropped) {
        ++numDropsQueued;
      } else {
        incrementSkipStat(Stats.Segments.DROP_SKIPPED, "Encountered error", segment, holder);
      }
    }

    return numDropsQueued;
  }

  /**
   * Queues load of {@code numToLoad} replicas of the segment on a tier.
   */
  private int loadReplicas(
      int numToLoad,
      int numLoadedReplicas,
      DataSegment segment,
      String tier,
      SegmentStatusInTier segmentStatus,
      @Nullable PartialLoadProfile profile
  )
  {
    final boolean isAlreadyLoadedOnTier = numLoadedReplicas >= 1;

    // Do not assign replicas if tier is already busy loading some
    if (isAlreadyLoadedOnTier && replicationThrottler.isReplicationThrottledForTier(tier)) {
      return 0;
    }

    final List<ServerHolder> eligibleServers = segmentStatus.getServersEligibleToLoad();
    if (eligibleServers.isEmpty()) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "No eligible server", segment, tier);
      return 0;
    }

    final Iterator<ServerHolder> serverIterator =
        useRoundRobinAssignment
        ? serverSelector.getServersInTierToLoadSegment(tier, segment)
        : strategy.findServersToLoadSegment(segment, eligibleServers);
    if (!serverIterator.hasNext()) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "No strategic server", segment, tier);
      return 0;
    }

    // Load the replicas on this tier
    int numLoadsQueued = 0;
    while (numLoadsQueued < numToLoad && serverIterator.hasNext()) {
      ServerHolder server = serverIterator.next();
      boolean queuedSuccessfully = isAlreadyLoadedOnTier ? replicateSegment(segment, server, profile)
                                                         : loadSegment(segment, server, profile);
      numLoadsQueued += queuedSuccessfully ? 1 : 0;
    }

    return numLoadsQueued;
  }

  private boolean loadSegment(DataSegment segment, ServerHolder server, @Nullable PartialLoadProfile profile)
  {
    final boolean assigned = loadQueueManager.loadSegment(segment, server, SegmentAction.LOAD, profile);

    if (!assigned) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "Encountered error", segment, server);
    }

    return assigned;
  }

  private boolean replicateSegment(DataSegment segment, ServerHolder server, @Nullable PartialLoadProfile profile)
  {
    final String tier = server.getServer().getTier();
    if (replicationThrottler.isReplicationThrottledForTier(tier)) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "Throttled replication", segment, server);
      return false;
    }

    final boolean assigned = loadQueueManager.loadSegment(segment, server, SegmentAction.REPLICATE, profile);
    if (!assigned) {
      incrementSkipStat(Stats.Segments.ASSIGN_SKIPPED, "Encountered error", segment, server);
    } else {
      replicationThrottler.incrementAssignedReplicas(tier);
    }

    return assigned;
  }

  private static ReplicationThrottler createReplicationThrottler(
      DruidCluster cluster,
      SegmentLoadingConfig loadingConfig
  )
  {
    final Map<String, Integer> tierToLoadingReplicaCount = new HashMap<>();

    cluster.getManagedHistoricals().forEach(
        (tier, historicals) -> {
          int numLoadingReplicas = historicals.stream().mapToInt(ServerHolder::getNumLoadingReplicas).sum();
          tierToLoadingReplicaCount.put(tier, numLoadingReplicas);
        }
    );
    return new ReplicationThrottler(
        tierToLoadingReplicaCount,
        loadingConfig.getReplicationThrottleLimit()
    );
  }

  private int cancelOperations(
      SegmentAction action,
      int maxNumToCancel,
      DataSegment segment,
      SegmentStatusInTier segmentStatus
  )
  {
    final List<ServerHolder> servers = segmentStatus.getServersPerforming(action);
    if (servers.isEmpty() || maxNumToCancel <= 0) {
      return 0;
    }

    int numCanceled = 0;
    for (int i = 0; i < servers.size() && numCanceled < maxNumToCancel; ++i) {
      numCanceled += servers.get(i).cancelOperation(action, segment) ? 1 : 0;
    }
    return numCanceled;
  }

  private void incrementSkipStat(CoordinatorStat stat, String reason, DataSegment segment, String tier)
  {
    final RowKey key = RowKey.with(Dimension.TIER, tier)
                             .with(Dimension.DATASOURCE, segment.getDataSource())
                             .and(Dimension.DESCRIPTION, reason);
    stats.add(stat, key, 1);
  }

  private void incrementSkipStat(CoordinatorStat stat, String reason, DataSegment segment, ServerHolder server)
  {
    final RowKey key = RowKey.with(Dimension.TIER, server.getServer().getTier())
                             .with(Dimension.DATASOURCE, segment.getDataSource())
                             .with(Dimension.SERVER, server.getServer().getName())
                             .and(Dimension.DESCRIPTION, reason);
    stats.add(stat, key, 1);
  }

  private void incrementStat(CoordinatorStat stat, DataSegment segment, String tier, long value)
  {
    stats.addToSegmentStat(stat, tier, segment.getDataSource(), value);
  }

}
