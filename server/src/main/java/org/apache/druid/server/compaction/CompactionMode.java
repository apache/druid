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
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Objects;

public enum CompactionMode
{
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
  INCREMENTAL_COMPACTION {
    @Override
    public CompactionCandidate createCandidate(
        CompactionCandidate.ProposedCompaction proposedCompaction,
        CompactionStatus eligibility,
        @Nullable String policyNote
    )
    {
      CompactionCandidate.ProposedCompaction newProposed = new CompactionCandidate.ProposedCompaction(
          Objects.requireNonNull(eligibility.getUncompactedSegments()),
          proposedCompaction.getUmbrellaInterval(),
          proposedCompaction.getCompactionInterval(),
          Math.toIntExact(eligibility.getUncompactedSegments()
                                     .stream()
                                     .map(DataSegment::getInterval)
                                     .distinct()
                                     .count())
      );
      return new CompactionCandidate(newProposed, eligibility, policyNote, this);
    }
  },
  NOT_APPLICABLE;

  public CompactionCandidate createCandidate(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      CompactionStatus eligibility
  )
  {
    return createCandidate(proposedCompaction, eligibility, null);
  }

  public CompactionCandidate createCandidate(
      CompactionCandidate.ProposedCompaction proposedCompaction,
      CompactionStatus eligibility,
      @Nullable String policyNote
  )
  {
    throw DruidException.defensive("Cannot create compaction candidate with mode[%s]", this);
  }

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

  public static CompactionCandidate complete(CompactionCandidate.ProposedCompaction proposedCompaction)
  {
    return new CompactionCandidate(proposedCompaction, CompactionStatus.COMPLETE, null, CompactionMode.NOT_APPLICABLE);
  }
}
