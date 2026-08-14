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
import org.apache.druid.server.coordinator.duty.CompactSegments;

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
   *
   * @return {@link Eligibility#FULL} only if eligible.
   */
  Eligibility checkEligibilityForCompaction(
      CompactionCandidate candidate,
      CompactionTaskStatus latestTaskStatus
  );

  /**
   * Like {@link #checkEligibilityForCompaction} but for an interval that must be compacted regardless
   * of any implementation-specific eligibility criteria. By default, this method calls
   * {@link #checkEligibilityForCompaction} and by default upgrades a rejection to a {@link Eligibility#FULL full}
   * compaction.
   */
  default Eligibility checkEligibilityForMandatoryCompaction(
      CompactionCandidate candidate,
      CompactionTaskStatus latestTaskStatus
  )
  {
    final Eligibility eligibility = checkEligibilityForCompaction(candidate, latestTaskStatus);
    return eligibility.isEligible() ? eligibility : Eligibility.FULL;
  }

  /**
   * Whether an interval that is below this policy's normal eligibility criteria should still be compacted
   * via {@link #checkEligibilityForMandatoryCompaction} when an external trigger requires it. The caller
   * decides what constitutes such a trigger; today the only one is a cascading reindexing interval with
   * deletion rules not yet applied to all of its segments. Defaults to false, so mandatory compaction is
   * never forced unless a policy opts in.
   */
  default boolean isForceMandatoryCompactionEnabled()
  {
    return false;
  }

}
