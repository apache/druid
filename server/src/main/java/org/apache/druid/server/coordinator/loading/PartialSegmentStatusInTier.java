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
 * As a last resort, when a stale replica has nowhere better to be replaced, this classifies the same server as a
 * target for an "additive-historical" in-place replace: a partial-load request arriving at a server that's already
 * (stale-)loaded fills in the missing parts in place rather than re-downloading from scratch. That's what makes
 * {@link #getEligibleForAdditiveReload()} a safe fallback destination when the tier has no spare capacity. This
 * option is the least preferred because the contract of an additive reload is to load only what is now needed and
 * missing; it does not drop anything that is no longer needed, so the server can end up holding a larger amount of
 * data than the current rule strictly requires.
 */
public class PartialSegmentStatusInTier
{
  private final List<ServerHolder> matchingLoaded = new ArrayList<>();
  private final List<ServerHolder> staleLoaded = new ArrayList<>();
  private final List<ServerHolder> matchingInFlight = new ArrayList<>();
  private final List<ServerHolder> staleInFlight = new ArrayList<>();
  private final List<ServerHolder> eligibleForFreshLoad = new ArrayList<>();
  private final List<ServerHolder> eligibleForAdditiveReload = new ArrayList<>();

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
   * i.e. a regular full-load replica seen against a partial rule). Eligible for additive reload (the historical
   * fills in the missing parts in place) and for being dropped once enough matching replicas exist.
   */
  public List<ServerHolder> getStaleLoaded()
  {
    return staleLoaded;
  }

  /**
   * Servers with an in-flight load whose profile fingerprint matches the request. Counts toward projected matching
   * replicas, the load is on its way to satisfying the rule.
   */
  public List<ServerHolder> getMatchingInFlight()
  {
    return matchingInFlight;
  }

  /**
   * Servers with an in-flight load whose profile fingerprint differs from the request (e.g., the rule changed
   * mid-flight, or an in-flight regular full-load against a partial rule). Cancel-and-replace targets when there is a
   * matching deficit.
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
   * Stale-loaded servers that can take an additive reload request; kept as a subset of {@link #getStaleLoaded()}
   * filtered for decommissioning / load-queue-full, so the algorithm can target them as a fallback destination when
   * no fresh-load slots are available. See {@code StrategicSegmentAssigner.updateReplicasInTierPartial}.
   */
  public List<ServerHolder> getEligibleForAdditiveReload()
  {
    return eligibleForAdditiveReload;
  }

  /**
   * Mechanical classification of one server against the request fingerprint. Branches are mutually exclusive in
   * order: <b>loaded</b> ({@link ServerHolder#isServingSegment}: matching / stale, with stale optionally also added
   * to {@link #eligibleForAdditiveReload}), <b>in-flight LOAD/REPLICATE</b> (matching / stale based on the peon's
   * queued profile), <b>empty-and-loadable</b> ({@link #eligibleForFreshLoad}).
   * <p>
   * Servers with a queued {@link SegmentAction#DROP}, {@link SegmentAction#MOVE_TO}, or
   * {@link SegmentAction#MOVE_FROM} fall through all branches by design, they're already accounted for in
   * {@link SegmentReplicaCount} totals and {@link StrategicSegmentAssigner}'s cross-tier drop budget.
   * The {@code isLoaded} branch is in particular gated by {@link ServerHolder#isServingSegment}, which requires
   * <em>no</em> action queued, so stale-loaded servers added to {@link #eligibleForAdditiveReload} are guaranteed
   * to be action-free at snapshot time.
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
        if (canReloadAdditively(server)) {
          eligibleForAdditiveReload.add(server);
        }
      }
    } else if (action == SegmentAction.LOAD || action == SegmentAction.REPLICATE) {
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
   * Filters a stale-loaded server for additive-reload eligibility: not decommissioning, and not over its per-run
   * load-queue budget. The "no other action queued" requirement that you'd otherwise expect to find here is
   * already satisfied implicitly, this is only called from the {@code isLoaded} branch of {@link #classify}, which
   * requires {@link ServerHolder#isServingSegment} = true (loaded AND no queued action). Same-run dedup against
   * subsequent re-queueing on the same server is enforced at {@link ServerHolder#startOperation}, not here.
   * <p>
   * Disk space is not checked: the additive reload's marginal cost is at most
   * {@code segment.size − alreadyLoadedSize}, and a strict full-size disk check would over-conservatively block
   * reloads on near-full servers that already host the stale replica. If the historical is too full to add the
   * missing parts, the load fails at the historical and reports as failed; the reconciler retries next run.
   */
  private static boolean canReloadAdditively(ServerHolder server)
  {
    return !server.isDecommissioning() && !server.isLoadQueueFull();
  }
}
