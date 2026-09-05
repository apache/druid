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

import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Objects;

/**
 * Classifies the servers in a tier by their relationship to a {@link PartialLoadProfile} request for a specific
 * segment. This is a passive snapshot the partial-load reconciler reads back during a coordinator run, see
 * {@link StrategicSegmentAssigner#updateReplicasInTierPartial} for the algorithm that consumes these buckets and
 * decides what to load, drop, or cancel. Partial load variant of {@link SegmentStatusInTier}.
 * <p>
 * A partial-load rule resolves, per segment, to a {@link PartialLoadProfile} carrying a {@code wrappedLoadSpec}
 * (scheme-specific request payload) and a {@code fingerprint} that uniquely identifies the request. The coordinator
 * stamps the wrapped load spec onto the outbound segment; the historical loads (partially, or via full-fallback
 * when it can't honor the scheme) and announces back with the wrapper's fingerprint plus realized {@code loadedBytes}.
 * On the next coordinator run this class reads the announced profile per replica and decides "matching" (announced
 * fingerprint equals the requested fingerprint; rule is satisfied for that replica) vs "stale" (any other state,
 * including a non-profile regular full-load replica).
 * <p>
 * A stale replica is also classified as a target for an in-place reload: a partial-load request arriving at a server
 * that is already (stale-)loaded is honored by swapping the rule on the cache entry it already has, so only the delta
 * the new fingerprint adds has to come off deep storage. That makes {@link #getEligibleForInPlaceReload()} the
 * <em>preferred</em> destination for a matching deficit as it downloads strictly less than a fresh load elsewhere, the
 * replica keeps serving throughout, and no follow-up drop is needed to retire the replica it replaces.
 * <p>
 * What makes this safe is that the historical pins a rule with cache holds rather than accumulating. Applying a rule
 * releases the holds on every bundle the new fingerprint does not select, so a reloaded server ends up pinned by
 * exactly the new rule; whatever the previous rule left on disk is ordinary evictable cache, reclaimed under pressure
 * like any other unheld data, and it is not part of the footprint the server announces back.
 */
public class PartialSegmentStatusInTier
{
  private final List<ServerHolder> matchingLoaded = new ArrayList<>();
  private final List<ServerHolder> staleLoaded = new ArrayList<>();
  private final List<ServerHolder> matchingInFlight = new ArrayList<>();
  private final List<ServerHolder> staleInFlight = new ArrayList<>();
  private final List<ServerHolder> eligibleForFreshLoad = new ArrayList<>();
  private final List<ServerHolder> eligibleForInPlaceReload = new ArrayList<>();

  public PartialSegmentStatusInTier(
      DataSegment segment,
      String requestedFingerprint,
      NavigableSet<ServerHolder> historicals
  )
  {
    for (ServerHolder server : historicals) {
      classify(server, segment, requestedFingerprint);
    }
  }

  /**
   * Servers that have the segment loaded with a profile whose fingerprint matches the request (including full-fallback
   * announcements with the matching fingerprint). Count toward the tier's required matching replica count.
   */
  public List<ServerHolder> getMatchingLoaded()
  {
    return matchingLoaded;
  }

  /**
   * Servers that have the segment loaded but with a non-matching profile (different fingerprint, or no profile at all,
   * i.e. a regular full-load replica seen against a partial rule). Eligible for an in-place reload onto the new
   * fingerprint, and for being dropped once enough matching replicas exist.
   */
  public List<ServerHolder> getStaleLoaded()
  {
    return staleLoaded;
  }

  /**
   * Servers with an in-flight load, or an incoming balancer move, whose profile fingerprint matches the request.
   * Counts toward projected matching replicas, the load is on its way to satisfying the rule.
   */
  public List<ServerHolder> getMatchingInFlight()
  {
    return matchingInFlight;
  }

  /**
   * Servers with an in-flight load, or an incoming balancer move, whose profile fingerprint differs from the request
   * (e.g., the rule changed mid-flight, or an in-flight regular full-load against a partial rule). Cancel-and-replace
   * targets when there is a matching deficit; an incoming move refuses the cancellation and is instead reconciled
   * once it lands and reclassifies as stale-loaded.
   */
  public List<ServerHolder> getStaleInFlight()
  {
    return staleInFlight;
  }

  /**
   * Servers that don't have the segment and can take a fresh load. See the algorithm doc on
   * {@code StrategicSegmentAssigner.updateReplicasInTierPartial} for how this bucket is consumed relative to the
   * other buckets.
   */
  public List<ServerHolder> getEligibleForFreshLoad()
  {
    return eligibleForFreshLoad;
  }

  /**
   * Stale-loaded servers that can take an in-place reload request; a subset of {@link #getStaleLoaded()} filtered by
   * {@link #canReloadInPlace}. The preferred destination for a matching deficit, ahead of
   * {@link #getEligibleForFreshLoad()}. See {@link StrategicSegmentAssigner#updateReplicasInTierPartial}.
   */
  public List<ServerHolder> getEligibleForInPlaceReload()
  {
    return eligibleForInPlaceReload;
  }

  /**
   * Mechanical classification of one server against the request fingerprint. Branches are mutually exclusive in
   * order: <b>loaded</b> ({@link ServerHolder#isServingSegment}: matching / stale, with stale optionally also added
   * to {@link #eligibleForInPlaceReload}), <b>in-flight LOAD/REPLICATE/MOVE_TO</b> (matching / stale based on the
   * peon's queued profile), <b>empty-and-loadable</b> ({@link #eligibleForFreshLoad}).
   * <p>
   * A balancer move is counted at its destination: the {@link SegmentAction#MOVE_TO} carries the profile cloned from
   * the source, so it classifies by fingerprint like any other in-flight load, and once it lands the destination
   * classifies as loaded. The source ({@link SegmentAction#MOVE_FROM}) is deliberately left unclassified, so that the
   * two endpoints of a move count as the single replica they will settle into, in both phases of the move and
   * whether the moving replica is matching or stale. Counting the move at the source instead would require the
   * {@link SegmentReplicaCount#moveCompletedPendingDrop()} correction the full-load path uses, which cannot be
   * applied to a fingerprint-partitioned count: it does not know whether the move it is netting out was of a
   * matching or a stale replica.
   * <p>
   * Servers with a queued {@link SegmentAction#DROP} fall through all branches as well, they're accounted for in
   * {@link SegmentReplicaCount} totals and {@link StrategicSegmentAssigner}'s cross-tier drop budget.
   * The {@code isLoaded} branch is gated by {@link ServerHolder#isServingSegment}, which requires <em>no</em> action
   * queued, so stale-loaded servers added to {@link #eligibleForInPlaceReload} are guaranteed to be action-free at
   * snapshot time.
   */
  private void classify(ServerHolder server, DataSegment segment, String requestedFingerprint)
  {
    final SegmentAction action = server.getActionOnSegment(segment);
    final boolean isLoaded = server.isServingSegment(segment);

    if (isLoaded) {
      final PartialLoadProfile loaded = server.getServer().getPartialLoadProfile(segment.getId());
      if (loaded != null && Objects.equals(loaded.fingerprint(), requestedFingerprint)) {
        matchingLoaded.add(server);
      } else {
        staleLoaded.add(server);
        if (canReloadInPlace(server)) {
          eligibleForInPlaceReload.add(server);
        }
      }
    } else if (action == SegmentAction.LOAD
               || action == SegmentAction.REPLICATE
               || action == SegmentAction.MOVE_TO) {
      final PartialLoadProfile inFlight = server.getInFlightProfile(segment);
      if (inFlight != null && Objects.equals(inFlight.fingerprint(), requestedFingerprint)) {
        matchingInFlight.add(server);
      } else {
        staleInFlight.add(server);
      }
    } else if (action == null && server.canLoadSegment(segment)) {
      eligibleForFreshLoad.add(server);
    }
  }

  /**
   * Whether a server that already serves the segment can take an in-place reload of it: not decommissioning, and not
   * over its per-run load-queue budget. Callers are responsible for establishing that the server actually serves the
   * segment with no other action queued ({@link ServerHolder#isServingSegment}); {@link #classify} gets that from the
   * {@code isLoaded} branch it calls this from. Same-run dedup against subsequent re-queueing on the same server is
   * enforced at {@link ServerHolder#startOperation}, not here.
   * <p>
   * {@link ServerHolder#canLoadSegment} is not usable in its place because it requires the server to <em>not</em>
   * already have the segment, which is precisely the case being handled here.
   * <p>
   * Disk space is not checked: the reload's marginal cost is at most {@code segment.size − alreadyLoadedSize}, and a
   * strict full-size disk check would over-conservatively block reloads on near-full servers that already host the
   * stale replica. If the historical is too full to add the missing parts, the load fails at the historical and
   * reports as failed; the reconciler retries next run.
   */
  public static boolean canReloadInPlace(ServerHolder server)
  {
    return !server.isDecommissioning() && !server.isLoadQueueFull();
  }
}
