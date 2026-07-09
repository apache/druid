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

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import org.apache.druid.error.InvalidInput;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Bundles the partial-load metadata that travels alongside a segment between the coordinator and the historicals.
 * Generic across partial-load schemes: scheme-specific data lives inside the {@code wrappedLoadSpec} map, which
 * matches the shape of the wire-form load-spec wrapper that gets stamped onto outbound load requests via
 * {@code DataSegment.withLoadSpec(...)}.
 * <p>
 * Used in three distinct states:
 * <ul>
 *   <li>{@link #forRequest(Map, String) forRequest}: outbound from coordinator to historical. Carries the wrapped
 *       load-spec map identifying what to load and the fingerprint identifying that request. {@code loadedBytes} is
 *       null because the on-disk footprint is only known after the historical has parsed segment metadata.</li>
 *   <li>{@link #forLoaded(Map, String, long) forLoaded}: inbound on a historical announcement after a successful
 *       partial load. {@code wrappedLoadSpec} echoes the request and {@code loadedBytes} reports the realized
 *       footprint.</li>
 *   <li>{@link #forFullFallback(String, long) forFullFallback}: inbound on a historical announcement when partial
 *       loading was requested but the historical fell back to a full download (zipped-V10 segment, capability
 *       mismatch on a partially-upgraded server, etc.). {@code wrappedLoadSpec} is null to signal the fallback;
 *       {@code loadedBytes} equals the full segment size. The matching fingerprint still satisfies the rule that
 *       requested the load, so no reload-thrash occurs.</li>
 * </ul>
 * The fingerprint is derived from the resolved scheme-specific data (e.g., for projections, the sorted/deduped name
 * list, see {@code ProjectionPartialLoadMatcher}) so that two rule configurations that resolve to the same set on a
 * segment produce the same fingerprint and don't churn replicas across coordinator runs.
 */
public record PartialLoadProfile(
    @Nullable Map<String, Object> wrappedLoadSpec,
    String fingerprint,
    @Nullable Long loadedBytes
)
{
  /**
   * Weak interner so the same partial-load profile is shared by reference. Most of the per-profile heap is in the
   * {@code wrappedLoadSpec} map and its contents; collapsing those across replicas is the main win at scale
   * (large partial-load tiers with multiple replicas per segment). All factory methods route through {@link #intern}.
   */
  private static final Interner<PartialLoadProfile> INTERNER = Interners.newWeakInterner();

  public PartialLoadProfile
  {
    Objects.requireNonNull(fingerprint, "fingerprint");
    if (wrappedLoadSpec != null) {
      wrappedLoadSpec = Map.copyOf(wrappedLoadSpec);
    }
  }

  /**
   * Build the outbound (coordinator → historical) profile for a partial-load request.
   * {@code wrappedLoadSpec} must be non-empty: an empty wrapped load spec is the matcher's "does not apply" signal
   * and should never produce a request profile.
   */
  public static PartialLoadProfile forRequest(Map<String, Object> wrappedLoadSpec, String fingerprint)
  {
    if (wrappedLoadSpec == null || wrappedLoadSpec.isEmpty()) {
      throw InvalidInput.exception("wrappedLoadSpec must not be null or empty for an outbound load request");
    }
    return intern(new PartialLoadProfile(wrappedLoadSpec, fingerprint, null));
  }

  /**
   * Build the inbound profile for a successful partial load announcement.
   */
  public static PartialLoadProfile forLoaded(Map<String, Object> wrappedLoadSpec, String fingerprint, long loadedBytes)
  {
    if (wrappedLoadSpec == null || wrappedLoadSpec.isEmpty()) {
      throw InvalidInput.exception("wrappedLoadSpec must not be null or empty for a loaded announcement");
    }
    return intern(new PartialLoadProfile(wrappedLoadSpec, fingerprint, loadedBytes));
  }

  /**
   * Build the inbound profile for a historical that was asked to partial-load but fell back to a full download.
   * {@code wrappedLoadSpec} is null as a sentinel; {@code loadedBytes} should be the full segment size so that
   * inventory accounting reflects actual on-disk footprint.
   */
  public static PartialLoadProfile forFullFallback(String fingerprint, long fullBytes)
  {
    return intern(new PartialLoadProfile(null, fingerprint, fullBytes));
  }

  private static PartialLoadProfile intern(PartialLoadProfile profile)
  {
    return INTERNER.intern(profile);
  }

  /**
   * Whether this profile represents a historical's full-fallback (i.e., it was asked to partial-load but couldn't).
   */
  public boolean isFullFallback()
  {
    return wrappedLoadSpec == null;
  }
}
