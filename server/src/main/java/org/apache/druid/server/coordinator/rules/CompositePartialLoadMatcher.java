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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.loading.CompositePartialLoadSpec;
import org.apache.druid.segment.loading.PartialLoadSpec;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Combines several {@link PartialLoadMatcher}s so that one {@link PartialLoadRule} can contribute more than one kind
 * of partial load to a segment. The resolved selection is the <em>union</em> of what the members select: partial-load
 * a segment's projections and a subset of its cluster groups together, or express a single scheme's selection that
 * its own matcher cannot, such as a disjoint union of include/exclude pattern pairs over the same clustering columns.
 * <p>
 * Composition is a union, not an ordered fallback. Members are not consulted in priority order and no member can
 * shadow another; every member that resolves contributes, and the historical-side {@link CompositePartialLoadSpec}
 * takes the union of their bundles.
 * <p>
 * <b>A member that does not apply vetoes the whole composite.</b> If any member returns {@code null}, it understands
 * neither the segment's shape nor how to express a selection for it, e.g. a cluster-group matcher facing a segment
 * that isn't clustered, or a matcher type this Druid version doesn't recognize (see {@link UnknownPartialLoadMatcher})
 * this matcher returns {@code null} too and the rule's {@link CannotMatchBehavior} decides for the whole segment.
 * Skipping such a member instead would silently narrow the load: a composite whose cluster-group member went opaque
 * would announce a segment holding only its projections and none of its rows, and queries against it would quietly
 * return nothing.
 */
public class CompositePartialLoadMatcher implements PartialLoadMatcher
{
  public static final String TYPE = "composite";

  static final String FINGERPRINT_VERSION = "v1";

  private final List<PartialLoadMatcher> matchers;

  @JsonCreator
  public CompositePartialLoadMatcher(@JsonProperty("matchers") List<PartialLoadMatcher> matchers)
  {
    if (matchers == null || matchers.isEmpty()) {
      throw InvalidInput.exception("matchers must not be null or empty for composite matcher");
    }
    for (int i = 0; i < matchers.size(); i++) {
      if (matchers.get(i) == null) {
        throw InvalidInput.exception("matchers[%s] must not be null for composite matcher", i);
      }
    }
    this.matchers = List.copyOf(matchers);
  }

  @JsonProperty
  public List<PartialLoadMatcher> getMatchers()
  {
    return matchers;
  }

  @Override
  @Nullable
  public MatchResult match(DataSegment segment, Map<String, Object> baseLoadSpec)
  {
    // Members get the real base load spec, not a stub: a member may legitimately inspect it, and its delegate is
    // stripped afterward (see toMember) rather than withheld up front.
    final List<MatchResult> results = new ArrayList<>(matchers.size());
    for (PartialLoadMatcher matcher : matchers) {
      final MatchResult result = matcher.match(segment, baseLoadSpec);
      if (result == null) {
        return null;
      }
      results.add(result);
    }

    if (results.size() == 1) {
      // A single-member composite is exactly its member. Emitting the member's load spec verbatim keeps a rule that
      // was wrapped in a composite fingerprint-identical to the same rule with the bare matcher, so wrapping does not
      // re-fingerprint (and thus re-apply) every segment the rule covers.
      return results.getFirst();
    }

    final List<Map<String, Object>> members = new ArrayList<>(results.size());
    for (MatchResult result : results) {
      members.add(toMember(result));
    }
    final String fingerprint = computeFingerprint(results);
    return new MatchResult(
        CompositePartialLoadSpec.wireForm(baseLoadSpec, members, fingerprint),
        fingerprint
    );
  }

  /**
   * Converts a member's {@link MatchResult} into the composite's member load spec by dropping the
   * {@link PartialLoadSpec#DELEGATE_FIELD} field. Every member of a composite describes a selection within the same
   * segment, so the composite carries the backend load spec once at the top level and re-injects it when it
   * materializes each member; repeating it per member would bloat every load request and announcement.
   */
  private static Map<String, Object> toMember(MatchResult result)
  {
    final Map<String, Object> member = new LinkedHashMap<>(result.wrappedLoadSpec());
    member.remove(PartialLoadSpec.DELEGATE_FIELD);
    return member;
  }

  /**
   * Fingerprints the composite over its members' {@code (type, fingerprint)} pairs. Each member fingerprint already
   * identifies that member's resolved selection within its own scheme, and the type disambiguates equal fingerprints
   * produced by different schemes.
   * <p>
   * Pairs are sorted, so reordering a rule's {@code matchers} does not change the fingerprint — the resolved selection
   * is a set union and therefore order-independent, and the cascade should not thrash on equivalent rule rewordings.
   * <p>
   * When every member resolved to an empty selection the composite reports {@link #EMPTY_LOAD_FINGERPRINT}, carrying
   * the empty-load contract through composition rather than minting a distinct fingerprint for a load that puts no
   * scheme-specific content on the historical.
   * <p>
   * Note that a composite of two same-scheme members does not fingerprint the same as a single matcher that resolved
   * to the same union. The coordinator only compares a segment's fingerprint against the rule that requested it, so
   * the difference costs at most one cheap rule re-apply on the historical, never a re-download.
   */
  private static String computeFingerprint(List<MatchResult> results)
  {
    boolean allEmpty = true;
    final List<String> pairs = new ArrayList<>(results.size());
    for (MatchResult result : results) {
      allEmpty = allEmpty && EMPTY_LOAD_FINGERPRINT.equals(result.fingerprint());
      pairs.add(memberType(result) + '\0' + result.fingerprint());
    }
    if (allEmpty) {
      return EMPTY_LOAD_FINGERPRINT;
    }
    final Hasher hasher = Hashing.sha256().newHasher();
    for (String pair : pairs.stream().sorted().toList()) {
      hasher.putUnencodedChars(pair);
      hasher.putByte((byte) 0);
    }
    final String hex = BaseEncoding.base16().encode(hasher.hash().asBytes()).toLowerCase(Locale.ROOT);
    // should be good enough without dragging the whole thing around for every segment
    return FINGERPRINT_VERSION + ":" + hex.substring(0, 16);
  }

  private static String memberType(MatchResult result)
  {
    return String.valueOf(result.wrappedLoadSpec().get(PartialLoadSpec.TYPE_FIELD));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositePartialLoadMatcher that = (CompositePartialLoadMatcher) o;
    return Objects.equals(matchers, that.matchers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(matchers);
  }

  @Override
  public String toString()
  {
    return "CompositePartialLoadMatcher{matchers=" + matchers + "}";
  }
}
