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
 * segment. Used by the partial-load reconciler in {@link StrategicSegmentAssigner} to decide what to load, drop, or
 * cancel under the load-then-drop swap pattern.
 * <p>
 * Per the additive-historical model: matching means the announced fingerprint equals the requested fingerprint. A
 * full-fallback profile (where the historical was asked to partial-load but couldn't, announcing
 * {@code wrappedLoadSpec=null} with the requested fingerprint) is also treated as matching, the rule was
 * satisfied even though the historical fell back to full.
 * <p>
 * Replicas without any profile (regular full-load) are always classified as stale relative to a partial-load rule.
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
   * Servers that don't have the segment and can take a fresh load (preferred destinations for new replicas; empty
   * slots, no in-place mutation needed).
   */
  public List<ServerHolder> getEligibleForFreshLoad()
  {
    return eligibleForFreshLoad;
  }

  /**
   * Stale-loaded servers that can take an additive reload request (same as {@link #getStaleLoaded()} once filtered for
   * decommissioning / queue-full / pending-action, kept separate so the reconciler can prefer fresh-load destinations
   * before falling back to in-place additive reload). The historical's additive load semantics make this the
   * mitigation for the "no spare server" stuck state.
   */
  public List<ServerHolder> getEligibleForAdditiveReload()
  {
    return eligibleForAdditiveReload;
  }

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
    // Other actions (DROP, MOVE_TO, MOVE_FROM) are intentionally not classified here, they're handled by the
    // existing replica-counting paths.
  }

  /**
   * A stale-loaded server is reload-eligible iff it's not decommissioning, has no other action queued for the segment,
   * and hasn't already exceeded its load-queue assignment budget for this run. Disk space is not checked here; the
   * additive reload's marginal cost is at most {@code segment.size − alreadyLoadedSize}, and a strict disk check
   * against the full segment size would over-conservatively block reloads on near-full servers that already host the
   * stale replica. If the historical is too full to add the missing parts, the load will fail at the historical and
   * report as failed; the reconciler retries next run.
   */
  private static boolean canReloadAdditively(ServerHolder server)
  {
    return !server.isDecommissioning() && !server.isLoadQueueFull();
  }
}
