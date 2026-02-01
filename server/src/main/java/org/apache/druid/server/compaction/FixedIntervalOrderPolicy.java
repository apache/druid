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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import java.util.List;

/**
 * Implementation of {@link CompactionCandidateSearchPolicy} that specifies the
 * datasources and intervals eligible for compaction and their order.
 * <p>
 * This policy is primarily used for integration tests.
 */
public class FixedIntervalOrderPolicy implements CompactionCandidateSearchPolicy
{
  private final List<Candidate> eligibleCandidates;

  @JsonCreator
  public FixedIntervalOrderPolicy(
      @JsonProperty("eligibleCandidates") List<Candidate> eligibleCandidates
  )
  {
    this.eligibleCandidates = eligibleCandidates;
  }

  @JsonProperty
  public List<Candidate> getEligibleCandidates()
  {
    return eligibleCandidates;
  }

  @Override
  public int compareCandidates(CompactionCandidate candidateA, CompactionCandidate candidateB)
  {
    return findIndex(candidateA) - findIndex(candidateB);
  }

  @Override
  public Eligibility checkEligibilityForCompaction(
      CompactionCandidate candidate,
      CompactionTaskStatus latestTaskStatus
  )
  {
    return findIndex(candidate) < Integer.MAX_VALUE
        ? Eligibility.FULL_COMPACTION_ELIGIBLE
        : Eligibility.fail("Datasource/Interval is not in the list of 'eligibleCandidates'");
  }

  private int findIndex(CompactionCandidate candidate)
  {
    int index = 0;
    for (Candidate eligibleCandidate : eligibleCandidates) {
      boolean found = eligibleCandidate.datasource.equals(candidate.getDataSource())
                      && eligibleCandidate.interval.contains(candidate.getUmbrellaInterval());
      if (found) {
        return index;
      } else {
        index++;
      }
    }

    return Integer.MAX_VALUE;
  }

  /**
   * Specifies a datasource-interval eligible for compaction.
   */
  public static class Candidate
  {
    private final String datasource;
    private final Interval interval;

    @JsonCreator
    public Candidate(
        @JsonProperty("datasource") String datasource,
        @JsonProperty("interval") Interval interval
    )
    {
      this.datasource = datasource;
      this.interval = interval;
    }

    @JsonProperty
    public String getDatasource()
    {
      return datasource;
    }

    @JsonProperty
    public Interval getInterval()
    {
      return interval;
    }
  }
}
