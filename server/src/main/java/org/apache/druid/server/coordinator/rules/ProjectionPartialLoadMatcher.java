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
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Base for {@link PartialLoadMatcher} implementations that decide which of a segment's V10 projections to load.
 * Subclasses supply the resolution policy via {@link #resolveProjectionNames(DataSegment)}; this base handles
 * fingerprint computation and wraps the result into the {@code partialProjection} load-spec wire form consumed
 * by the historical-side {@code PartialProjectionLoadSpec}.
 * <p>
 * The fingerprint is a hash of what projections are partially loaded on a segment by this rule; the data node will
 * include this value in the segment announcement so that it can be used as a lightweight value to compare against
 * to handle things like rule change so that we can ensure that the 'right' partial load is in place from run to run.
 */
public abstract class ProjectionPartialLoadMatcher implements PartialLoadMatcher
{
  static final String LOAD_SPEC_TYPE = "partialProjection";
  static final String FINGERPRINT_VERSION = "v1";

  /**
   * Returns the sorted, deduped list of projection names from {@link DataSegment#getProjections()} that this matcher
   * selects. Returns an empty list when nothing matches (the segment exposes no projections, or no configured pattern
   * intersects what the segment has).
   */
  protected abstract List<String> resolveProjectionNames(DataSegment segment);

  @Override
  @Nullable
  public MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec)
  {
    final List<String> resolved = resolveProjectionNames(segment);
    if (resolved.isEmpty()) {
      return null;
    }
    final String fingerprint = computeFingerprint(resolved);
    final Map<String, Object> wrapped = Map.of(
        "type", LOAD_SPEC_TYPE,
        "delegate", baseLoadSpec,
        "projections", resolved,
        "fingerprint", fingerprint
    );
    return new MatchResult(wrapped, fingerprint);
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
