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

import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;

public class MostFragmentedIntervalFirstPolicyTest
{
  private static final DataSegment SEGMENT =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI).eachOfSizeInMb(100).get(0);

  @Test
  public void test_thresholdValues_ofDefaultPolicy()
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(null, null, null, null);
    Assertions.assertEquals(100, policy.getMinUncompactedCount());
    Assertions.assertEquals(new HumanReadableBytes("10MiB"), policy.getMinUncompactedBytes());
    Assertions.assertEquals(new HumanReadableBytes("2GiB"), policy.getMaxAverageUncompactedBytesPerSegment());
    Assertions.assertNull(policy.getPriorityDatasource());
  }

  @Test
  public void test_checkEligibilityForCompaction_fails_ifUncompactedCountLessThanCutoff()
  {
    final int minUncompactedCount = 10_000;
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        minUncompactedCount,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null
    );

    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.fail(
            "Uncompacted segments[1] in interval must be at least [10,000]"
        ),
        policy.checkEligibilityForCompaction(createCandidate(1, 100L), null)
    );
    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.OK,
        policy.checkEligibilityForCompaction(createCandidate(10_001, 100L), null)
    );
  }

  @Test
  public void test_checkEligibilityForCompaction_fails_ifUncompactedBytesLessThanCutoff()
  {
    final HumanReadableBytes minUncompactedBytes = HumanReadableBytes.valueOf(10_000);
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        minUncompactedBytes,
        HumanReadableBytes.valueOf(10_000),
        null
    );

    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.fail(
            "Uncompacted bytes[100] in interval must be at least [10,000]"
        ),
        policy.checkEligibilityForCompaction(createCandidate(1, 100L), null)
    );
    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.OK,
        policy.checkEligibilityForCompaction(createCandidate(100, 10_000L), null)
    );
  }

  @Test
  public void test_checkEligibilityForCompaction_fails_ifAvgSegmentSizeGreaterThanCutoff()
  {
    final HumanReadableBytes maxAvgSegmentSize = HumanReadableBytes.valueOf(100);
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(100),
        maxAvgSegmentSize,
        null
    );

    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.fail(
            "Average size[10,000] of uncompacted segments in interval must be at most [100]"
        ),
        policy.checkEligibilityForCompaction(createCandidate(1, 10_000L), null)
    );
    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.OK,
        policy.checkEligibilityForCompaction(createCandidate(1, 100L), null)
    );
  }

  @Test
  public void test_policy_favorsIntervalWithMoreUncompactedSegments_ifTotalBytesIsEqual()
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null
    );

    final CompactionCandidate candidateA = createCandidate(1, 1000L);
    final CompactionCandidate candidateB = createCandidate(2, 500L);

    verifyCandidateIsEligible(candidateA, policy);
    verifyCandidateIsEligible(candidateB, policy);

    Assertions.assertTrue(policy.compareCandidates(candidateA, candidateB) > 0);
    Assertions.assertTrue(policy.compareCandidates(candidateB, candidateA) < 0);
  }

  @Test
  public void test_policy_favorsIntervalWithMoreUncompactedSegments_ifAverageSizeIsEqual()
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null
    );

    final CompactionCandidate candidateA = createCandidate(1, 1000L);
    final CompactionCandidate candidateB = createCandidate(2, 1000L);

    verifyCandidateIsEligible(candidateA, policy);
    verifyCandidateIsEligible(candidateB, policy);

    Assertions.assertTrue(policy.compareCandidates(candidateA, candidateB) > 0);
    Assertions.assertTrue(policy.compareCandidates(candidateB, candidateA) < 0);
  }

  @Test
  public void test_policy_favorsIntervalWithSmallerSegments_ifCountIsEqual()
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null
    );

    final CompactionCandidate candidateA = createCandidate(10, 500L);
    final CompactionCandidate candidateB = createCandidate(10, 1000L);

    verifyCandidateIsEligible(candidateA, policy);
    verifyCandidateIsEligible(candidateB, policy);

    Assertions.assertTrue(policy.compareCandidates(candidateA, candidateB) < 0);
    Assertions.assertTrue(policy.compareCandidates(candidateB, candidateA) > 0);
  }

  @Test
  public void test_compareCandidates_returnsZeroIfSegmentCountAndAvgSizeScaleEquivalently()
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        100,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(100),
        null
    );

    final CompactionCandidate candidateA = createCandidate(100, 25);
    final CompactionCandidate candidateB = createCandidate(400, 100);

    verifyCandidateIsEligible(candidateA, policy);
    verifyCandidateIsEligible(candidateB, policy);

    Assertions.assertEquals(0, policy.compareCandidates(candidateA, candidateB));
    Assertions.assertEquals(0, policy.compareCandidates(candidateB, candidateA));
  }

  private CompactionCandidate createCandidate(int numSegments, long avgSizeBytes)
  {
    final CompactionStatistics dummyCompactedStats = CompactionStatistics.create(1L, 1L, 1L);
    final CompactionStatistics uncompactedStats = CompactionStatistics.create(
        avgSizeBytes * numSegments,
        numSegments,
        1L
    );
    return CompactionCandidate.from(List.of(SEGMENT), null)
        .withCurrentStatus(CompactionStatus.pending(dummyCompactedStats, uncompactedStats, ""));
  }

  private void verifyCandidateIsEligible(CompactionCandidate candidate, MostFragmentedIntervalFirstPolicy policy)
  {
    Assertions.assertEquals(
        CompactionCandidateSearchPolicy.Eligibility.OK,
        policy.checkEligibilityForCompaction(candidate, null)
    );
  }
}
