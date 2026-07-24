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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Decides whether a {@link PartialLoadRule} should partially load a given segment and, when it should, produces the
 * wire-form load-spec wrapper plus a fingerprint identifying that request. Implementations encapsulate both the
 * configuration that drives the decision and the wire format of their corresponding {@link LoadSpec} wrapper, so the
 * rule layer stays scheme-agnostic.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = UnknownPartialLoadMatcher.class)
@JsonSubTypes({
    @JsonSubTypes.Type(name = ExactProjectionPartialLoadMatcher.TYPE, value = ExactProjectionPartialLoadMatcher.class),
    @JsonSubTypes.Type(name = WildcardProjectionPartialLoadMatcher.TYPE, value = WildcardProjectionPartialLoadMatcher.class),
    @JsonSubTypes.Type(name = WildcardClusterGroupPartialLoadMatcher.TYPE, value = WildcardClusterGroupPartialLoadMatcher.class)
})
public interface PartialLoadMatcher
{
  /**
   * Universal fingerprint sentinel for an "empty match" — a matcher decision to partial-load a segment with no
   * scheme-specific content. A matcher that applies to a segment but resolves to no positive content (e.g., a
   * cluster-group matcher on a clustered segment whose tuples don't intersect any configured pattern) returns a
   * {@link MatchResult} with this fingerprint. The empty load is dispatched like any other partial load onto the
   * rule's tiered replicants, so the segment stays announced (and thus in the broker's timeline); the historical
   * downloads no scheme-specific content for it.
   *
   * <p>All matchers share this fingerprint for empty loads — different matchers' empty wire forms are equivalent
   * from a "what's on the historical" perspective (no scheme-specific extras downloaded), and at most one rule
   * applies per segment per coordinator run, so cross-matcher reconciliation isn't a concern.
   */
  String EMPTY_LOAD_FINGERPRINT = "v1:partial-empty";

  /**
   * Returns the {@link MatchResult} this matcher produces for the given segment, or null if the matcher does not apply
   * to the segment. When null, {@link PartialLoadRule} consults {@link CannotMatchBehavior} to decide whether the rule
   * falls through or full-loads.
   */
  @Nullable
  MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec);

  /**
   * Output of {@link #match(DataSegment, Map)} when the matcher applies. Carries the wrapped load-spec map (ready to
   * be stamped onto an outbound {@link SegmentChangeRequestLoad}) and the fingerprint used by the coordinator to
   * reconcile loaded replicas against the rule that requested them. Scheme-specific data lives inside
   * {@code wrappedLoadSpec}; callers that need a typed view extract from the map themselves.
   */
  record MatchResult(Map<String, Object> wrappedLoadSpec, String fingerprint)
  {
  }
}
