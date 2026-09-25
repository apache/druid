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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.segment.loading.PartialBaseTableLoadSpec;
import org.apache.druid.segment.loading.PartialProjectionLoadSpec;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Base for {@link PartialLoadMatcher} implementations that decide which of a segment's V10 projections to load.
 * Subclasses supply the resolution policy via {@link #resolveProjectionNames(DataSegment)}; this base handles
 * fingerprint computation and wraps the result into the {@code partialProjection} load spec consumed by the
 * historical-side {@link PartialProjectionLoadSpec}.
 * <p>
 * The fingerprint is a hash of what projections are partially loaded on a segment by this rule; the data node will
 * include this value in the segment announcement so that it can be used as a lightweight value to compare against
 * to handle things like rule change so that we can ensure that the 'right' partial load is in place from run to run.
 * <p>
 * <b>Projection matchers always apply.</b> When none of the configured projections are present on a segment, the
 * matcher resolves to a {@link PartialBaseTableLoadSpec} (every row, no projections) instead of going opaque. A
 * projection is precomputation that is always recoverable from the base table, so the base table is a correct
 * substitute for one the segment doesn't carry, and it is strictly less data than every bundle on the segment. This
 * is the ordinary state of affairs mid-rollout, when a new projection is being reindexed in and only some segments
 * carry it yet.
 * <p>
 * Because {@link #match} therefore never returns {@code null}, a rule whose only matcher is a projection matcher
 * never consults its {@link CannotMatchBehavior}, and "segments lacking projection p belong to a different rule" is
 * not expressible via {@link CannotMatchBehavior#FALL_THROUGH}. Scope such rules by interval or period instead.
 */
public abstract class ProjectionPartialLoadMatcher implements PartialLoadMatcher
{
  static final String FINGERPRINT_VERSION = "v1";

  /**
   * Returns the sorted, deduped list of projection names from {@link DataSegment#getProjections()} that this matcher
   * selects. Returns an empty list when nothing matches (the segment exposes no projections, or no configured pattern
   * intersects what the segment has), which {@link #match} turns into a base-table load rather than a non-match.
   */
  protected abstract List<String> resolveProjectionNames(DataSegment segment);

  /**
   * Never returns {@code null}; see the class doc. Either the resolved projections, or a base-table load when none of
   * them are present on {@code segment}.
   */
  @Override
  public MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec)
  {
    final List<String> resolved = resolveProjectionNames(segment);
    if (resolved.isEmpty()) {
      return new MatchResult(
          PartialBaseTableLoadSpec.wireForm(baseLoadSpec, PartialBaseTableLoadSpec.FINGERPRINT),
          PartialBaseTableLoadSpec.FINGERPRINT
      );
    }
    final String fingerprint = computeFingerprint(resolved);
    return new MatchResult(PartialProjectionLoadSpec.wireForm(baseLoadSpec, resolved, fingerprint), fingerprint);
  }

  static String computeFingerprint(List<String> sortedDedupedNames)
  {
    final Hasher hasher = Hashing.sha256().newHasher();
    for (String name : sortedDedupedNames) {
      hasher.putUnencodedChars(name);
      hasher.putByte((byte) 0);
    }
    final String hex = BaseEncoding.base16().encode(hasher.hash().asBytes()).toLowerCase(Locale.ROOT);
    // should be good enough without dragging the whole thing around for every segment
    return FINGERPRINT_VERSION + ":" + hex.substring(0, 16);
  }
}
