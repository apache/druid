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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.loading.PartialBaseTableLoadSpec;
import org.apache.druid.segment.loading.PartialFullSegmentLoadSpec;
import org.apache.druid.server.coordinator.loading.PartialLoadProfile;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for rules that load only a subset of a segment on a tier. Pairs a {@link PartialLoadMatcher} (which
 * produces the wrapped load spec and an accounting fingerprint when it applies to a segment) with a
 * {@link CannotMatchBehavior} that decides what the rule does when the matcher does not apply — fall through to the
 * next rule, or apply anyway and ask for none, some, or all of the segment.
 */
public abstract class PartialLoadRule extends LoadRule
{
  private final PartialLoadMatcher matcher;
  private final CannotMatchBehavior onCannotMatch;

  protected PartialLoadRule(
      Map<String, Integer> tieredReplicants,
      @Nullable Boolean useDefaultTierForNull,
      PartialLoadMatcher matcher,
      @Nullable CannotMatchBehavior onCannotMatch
  )
  {
    super(tieredReplicants, useDefaultTierForNull);
    if (matcher == null) {
      throw InvalidInput.exception("matcher must not be null for a partial load rule");
    }
    this.matcher = matcher;
    this.onCannotMatch = Configs.valueOrDefault(onCannotMatch, CannotMatchBehavior.LOAD_ON_DEMAND);
  }

  @JsonProperty
  public PartialLoadMatcher getMatcher()
  {
    return matcher;
  }

  @JsonProperty
  public CannotMatchBehavior getOnCannotMatch()
  {
    return onCannotMatch;
  }

  @Override
  public boolean isIntervalBased()
  {
    return false;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    if (!appliesTo(segment.getInterval(), referenceTimestamp)) {
      return false;
    }
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    if (result != null) {
      return true;
    }
    // Every behavior other than FALL_THROUGH applies the rule; they differ only in how much of the segment run()
    // then asks the historical to make resident.
    return onCannotMatch != CannotMatchBehavior.FALL_THROUGH;
  }

  @Override
  public void run(DataSegment segment, SegmentActionHandler handler)
  {
    final PartialLoadMatcher.MatchResult result = matcher.match(segment, segment.getLoadSpec());
    if (result != null) {
      // Matcher resolved: route through the partial-load handler. The wrappedLoadSpec map carries scheme-specific
      // data that the historical-side wrapper deserializes.
      handler.replicateSegmentPartially(
          segment,
          PartialLoadProfile.forRequest(result.wrappedLoadSpec(), result.fingerprint()),
          getTieredReplicants()
      );
      return;
    }
    // Matcher does not apply, but the rule still does — FALL_THROUGH would have made appliesTo return false, so run
    // wouldn't have been invoked. How much of the segment to make resident is onCannotMatch's call.
    switch (onCannotMatch) {
      case LOAD_ON_DEMAND -> handler.replicateSegment(segment, getTieredReplicants());
      case BASE_LOAD -> replicateWholly(
          segment,
          handler,
          PartialBaseTableLoadSpec.wireForm(segment.getLoadSpec(), PartialBaseTableLoadSpec.FINGERPRINT),
          PartialBaseTableLoadSpec.FINGERPRINT
      );
      case FULL_LOAD -> replicateWholly(
          segment,
          handler,
          PartialFullSegmentLoadSpec.wireForm(segment.getLoadSpec(), PartialFullSegmentLoadSpec.FINGERPRINT),
          PartialFullSegmentLoadSpec.FINGERPRINT
      );
      default -> throw DruidException.defensive(
          "Unreachable onCannotMatch[%s] in run() for segment[%s]; appliesTo should have returned false",
          onCannotMatch,
          segment.getId()
      );
    }
  }

  /**
   * Dispatches a layout-derived partial load — one whose selection is "the base table" or "everything" rather than
   * anything the matcher resolved. These still go through the partial-load handler so the historical actually makes
   * the bundles resident and announces a fingerprint the coordinator can reconcile.
   */
  private void replicateWholly(
      DataSegment segment,
      SegmentActionHandler handler,
      Map<String, Object> wrappedLoadSpec,
      String fingerprint
  )
  {
    handler.replicateSegmentPartially(
        segment,
        PartialLoadProfile.forRequest(wrappedLoadSpec, fingerprint),
        getTieredReplicants()
    );
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
    if (!super.equals(o)) {
      return false;
    }
    PartialLoadRule that = (PartialLoadRule) o;
    return onCannotMatch == that.onCannotMatch
        && Objects.equals(matcher, that.matcher);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), matcher, onCannotMatch);
  }
}
