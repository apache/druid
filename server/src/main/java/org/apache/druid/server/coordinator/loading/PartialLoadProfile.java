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
 * Used in two distinct states:
 * <ul>
 *   <li>{@link #forRequest(Map, String) forRequest}: outbound from coordinator to historical. Carries the wrapped
 *       load-spec map identifying what to load and the fingerprint identifying that request. {@code loadedBytes} is
 *       null because the on-disk footprint is only known after the historical has parsed segment metadata.</li>
 *   <li>{@link #forLoaded(Map, String, long) forLoaded}: inbound on a historical announcement after a load completed.
 *       {@code wrappedLoadSpec} echoes the request and {@code loadedBytes} reports the realized footprint. Covers
 *       both real partial loads and historicals that fell back to a full download. The fingerprint match is what
 *       satisfies the coordinator's rule either way; the footprint number rides through for capacity accounting.</li>
 * </ul>
 * The fingerprint is derived from the resolved scheme-specific data (e.g., for projections, the sorted/deduped name
 * list, see {@code ProjectionPartialLoadMatcher}) so that two rule configurations that resolve to the same set on a
 * segment produce the same fingerprint and don't churn replicas across coordinator runs.
 */
public record PartialLoadProfile(
    Map<String, Object> wrappedLoadSpec,
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
    Objects.requireNonNull(wrappedLoadSpec, "wrappedLoadSpec");
    Objects.requireNonNull(fingerprint, "fingerprint");
    wrappedLoadSpec = Map.copyOf(wrappedLoadSpec);
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
   * Build the inbound profile for a completed load announcement. Applies to both real partial loads (whose
   * {@code loadedBytes} reflects the rule-declared on-disk footprint) and full-download fallbacks (whose
   * {@code loadedBytes} equals the full segment size).
   */
  public static PartialLoadProfile forLoaded(Map<String, Object> wrappedLoadSpec, String fingerprint, long loadedBytes)
  {
    if (wrappedLoadSpec == null || wrappedLoadSpec.isEmpty()) {
      throw InvalidInput.exception("wrappedLoadSpec must not be null or empty for a loaded announcement");
    }
    return intern(new PartialLoadProfile(wrappedLoadSpec, fingerprint, loadedBytes));
  }

  private static PartialLoadProfile intern(PartialLoadProfile profile)
  {
    return INTERNER.intern(profile);
  }
}
