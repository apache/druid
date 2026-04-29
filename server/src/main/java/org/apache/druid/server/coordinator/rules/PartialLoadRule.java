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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for rules that load only a subset of a segment on a tier. Pairs a {@link PartialLoadMatcher} (which
 * produces the wrapped load-spec wire form and an accounting fingerprint when it applies to a segment) with a
 * {@link CannotMatchBehavior} that controls whether the rule falls through or full-loads when the matcher does not
 * apply.
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
    this.onCannotMatch = Configs.valueOrDefault(onCannotMatch, CannotMatchBehavior.FULL_LOAD);
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
    return onCannotMatch == CannotMatchBehavior.FULL_LOAD;
  }

  @Override
  public void run(DataSegment segment, SegmentActionHandler handler)
  {
    // Partial plumbing is added in future work. For now, a partial rule that applies to a segment full-loads it,
    // identical behavior to the corresponding non-partial rule
    handler.replicateSegment(segment, getTieredReplicants());
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
