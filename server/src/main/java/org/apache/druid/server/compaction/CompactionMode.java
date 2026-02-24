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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * Represents the mode of compaction for segment intervals.
 * <p>
 * This enum defines different compaction modes that can be applied to segments,
 * and provides factory methods for creating {@link CompactionCandidate} instances
 * with the appropriate mode.
 */
public enum CompactionMode
{
  /**
   * Indicates that a full compaction should be performed on the segments.
   * This mode allows creating compaction candidates that will undergo complete compaction.
   */
  FULL_COMPACTION {
    @Override
    public CompactionCandidate createCandidate(
        CompactionCandidate.ProposedCompaction proposedCompaction,
        CompactionStatus eligibility,
        @Nullable String policyNote
    )
    {
      return new CompactionCandidate(proposedCompaction, eligibility, policyNote, this);
    }
  },
  /**
   * Indicates that compaction is not applicable for the segments.
   * This mode is used when segments are not eligible for compaction, have failed policy checks,
   * or have already been compacted.
   */
  NOT_APPLICABLE;

  /**
   * Creates a compaction candidate with the given proposed compaction and eligibility status.
   *
   * @param proposedCompaction the proposed compaction configuration
   * @param eligibility the eligibility status for compaction
   * @return a {@link CompactionCandidate} with this mode
   */
  public CompactionCandidate createCandidate(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      CompactionStatus eligibility
  )
  {
    return createCandidate(proposedCompaction, eligibility, null);
  }

  /**
   * Creates a compaction candidate with the given proposed compaction, eligibility status, and policy note.
   *
   * @param proposedCompaction the proposed compaction configuration
   * @param eligibility the eligibility status for compaction
   * @param policyNote optional note about policy evaluation, may be null
   * @return a {@link CompactionCandidate} with this mode
   * @throws DruidException if this mode does not support creating candidates (e.g., NOT_APPLICABLE)
   */
  public CompactionCandidate createCandidate(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      CompactionStatus eligibility,
      @Nullable String policyNote
  )
  {
    throw DruidException.defensive("Cannot create compaction candidate with mode[%s]", this);
  }

  /**
   * Creates a compaction candidate that has failed a policy check.
   *
   * @param proposedCompaction the proposed compaction configuration
   * @param eligibility the eligibility status for compaction
   * @param reasonFormat format string for the failure reason
   * @param args arguments for the format string
   * @return a {@link CompactionCandidate} with mode {@link #NOT_APPLICABLE} and a formatted policy note
   */
  public static CompactionCandidate failWithPolicyCheck(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      CompactionStatus eligibility,
      String reasonFormat,
      Object... args
  )
  {
    return new CompactionCandidate(
        proposedCompaction,
        eligibility,
        StringUtils.format(reasonFormat, args),
        CompactionMode.NOT_APPLICABLE
    );
  }

  /**
   * Creates a compaction candidate that is not eligible for compaction.
   * <p>
   * This is used when the candidate fails the initial eligibility check,
   * before any policy evaluation occurs.
   *
   * @param proposedCompaction the proposed compaction configuration
   * @param reason the reason for ineligibility
   * @return a {@link CompactionCandidate} with mode {@link #NOT_APPLICABLE} and ineligible status
   */
  public static CompactionCandidate notEligible(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      String reason
  )
  {
    // CompactionStatus returns an ineligible reason, have not even got to policy check yet
    return new CompactionCandidate(
        proposedCompaction,
        CompactionStatus.notEligible(reason),
        null,
        CompactionMode.NOT_APPLICABLE
    );
  }

  /**
   * Creates a compaction candidate that has already been compacted.
   *
   * @param proposedCompaction the proposed compaction configuration
   * @return a {@link CompactionCandidate} with mode {@link #NOT_APPLICABLE} and {@link CompactionStatus#COMPLETE}
   */
  public static CompactionCandidate complete(CompactionCandidate.ProposedCompaction proposedCompaction)
  {
    return new CompactionCandidate(proposedCompaction, CompactionStatus.COMPLETE, null, CompactionMode.NOT_APPLICABLE);
  }
}
