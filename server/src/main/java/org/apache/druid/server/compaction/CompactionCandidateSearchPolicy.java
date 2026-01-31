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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.duty.CompactSegments;

import java.util.Objects;

/**
 * Policy used by {@link CompactSegments} duty to pick segments for compaction.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "newestSegmentFirst", value = NewestSegmentFirstPolicy.class),
    @JsonSubTypes.Type(name = "fixedIntervalOrder", value = FixedIntervalOrderPolicy.class),
    @JsonSubTypes.Type(name = "mostFragmentedFirst", value = MostFragmentedIntervalFirstPolicy.class)
})
public interface CompactionCandidateSearchPolicy
{
  /**
   * Compares between two compaction candidates. Used to determine the
   * order in which segments and intervals should be picked for compaction.
   *
   * @return A negative value if {@code candidateA} should be picked first, a
   * positive value if {@code candidateB} should be picked first or zero if the
   * order does not matter.
   */
  int compareCandidates(CompactionCandidate candidateA, CompactionCandidate candidateB);

  /**
   * Checks if the given {@link CompactionCandidate} is eligible for compaction
   * in the current iteration. A policy may implement this method to skip
   * compacting intervals or segments that do not fulfil some required criteria.
   */
  Eligibility checkEligibilityForCompaction(
      CompactionCandidate candidate,
      CompactionTaskStatus latestTaskStatus
  );

  /**
   * Describes the eligibility of an interval for compaction.
   */
  class Eligibility
  {
    public enum PolicyEligibility
    {
      FULL_COMPACTION,
      INCREMENTAL_COMPACTION,
      NOT_ELIGIBLE
    }

    public static final Eligibility FULL_COMPACTION_OK = new Eligibility(PolicyEligibility.FULL_COMPACTION, null);

    private final PolicyEligibility eligible;
    private final String reason;

    private Eligibility(PolicyEligibility eligible, String reason)
    {
      this.eligible = eligible;
      this.reason = reason;
    }

    public PolicyEligibility getPolicyEligibility()
    {
      return eligible;
    }

    public String getReason()
    {
      return reason;
    }

    public static Eligibility incrementalCompaction(String messageFormat, Object... args)
    {
      return new Eligibility(PolicyEligibility.INCREMENTAL_COMPACTION, StringUtils.format(messageFormat, args));
    }

    public static Eligibility fail(String messageFormat, Object... args)
    {
      return new Eligibility(PolicyEligibility.NOT_ELIGIBLE, StringUtils.format(messageFormat, args));
    }

    @Override
    public boolean equals(Object object)
    {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      Eligibility that = (Eligibility) object;
      return eligible == that.eligible && Objects.equals(reason, that.reason);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(eligible, reason);
    }

    @Override
    public String toString()
    {
      return "Eligibility{" +
             "eligible=" + eligible +
             ", reason='" + reason + '\'' +
             '}';
    }
  }
}
