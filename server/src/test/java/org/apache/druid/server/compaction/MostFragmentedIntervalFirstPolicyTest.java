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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;

public class MostFragmentedIntervalFirstPolicyTest
{
  private static final DataSegment SEGMENT =
      CreateDataSegments.ofDatasource(TestDataSource.WIKI).eachOfSizeInMb(100).get(0);

  @Test
  public void test_thresholdValues_ofDefaultPolicy()
  {
    final MostFragmentedIntervalFirstPolicy policy =
        new MostFragmentedIntervalFirstPolicy(null, null, null, null, null);
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
        null,
        null
    );

    Assertions.assertEquals(
        CompactionEligibility.fail(
            "Uncompacted segments[1] in interval must be at least [10,000]"
        ),
        policy.checkEligibilityForCompaction(createCandidate(1, 100L))
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(createCandidate(10_001, 100L))
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
        null,
        null
    );

    Assertions.assertEquals(
        CompactionEligibility.fail(
            "Uncompacted bytes[100] in interval must be at least [10,000]"
        ),
        policy.checkEligibilityForCompaction(createCandidate(1, 100L))
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(createCandidate(100, 10_000L))
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
        null,
        null
    );

    Assertions.assertEquals(
        CompactionEligibility.fail(
            "Average size[10,000] of uncompacted segments in interval must be at most [100]"
        ),
        policy.checkEligibilityForCompaction(createCandidate(1, 10_000L))
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(createCandidate(1, 100L))
    );
  }

  @Test
  public void test_policy_favorsIntervalWithMoreUncompactedSegments_ifTotalBytesIsEqual()
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null,
        null
    );

    final CompactionCandidate candidateA = createCandidate(1, 1000L);
    final CompactionCandidate candidateB = createCandidate(2, 500L);

    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateA)
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateB)
    );

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
        null,
        null
    );

    final CompactionCandidate candidateA = createCandidate(1, 1000L);
    final CompactionCandidate candidateB = createCandidate(2, 1000L);

    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateA)
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateB)
    );

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
        null,
        null
    );

    final CompactionCandidate candidateA = createCandidate(10, 500L);
    final CompactionCandidate candidateB = createCandidate(10, 1000L);

    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateA)
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateB)
    );

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
        null,
        null
    );

    final CompactionCandidate candidateA = createCandidate(100, 25);
    final CompactionCandidate candidateB = createCandidate(400, 100);

    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateA)
    );
    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidateB)
    );

    Assertions.assertEquals(0, policy.compareCandidates(candidateA, candidateB));
    Assertions.assertEquals(0, policy.compareCandidates(candidateB, candidateA));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(MostFragmentedIntervalFirstPolicy.class)
                  .usingGetClass()
                  .withIgnoredFields("comparator")
                  .verify();
  }

  @Test
  public void test_serde_allFieldsSet() throws IOException
  {
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(2),
        HumanReadableBytes.valueOf(3),
        null,
        "foo"
    );
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final CompactionCandidateSearchPolicy policy2 =
        mapper.readValue(mapper.writeValueAsString(policy), CompactionCandidateSearchPolicy.class);
    Assertions.assertEquals(policy, policy2);
  }

  @Test
  public void test_serde_noFieldsSet() throws IOException
  {
    final MostFragmentedIntervalFirstPolicy policy =
        new MostFragmentedIntervalFirstPolicy(null, null, null, null, null);
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final CompactionCandidateSearchPolicy policy2 =
        mapper.readValue(mapper.writeValueAsString(policy), CompactionCandidateSearchPolicy.class);
    Assertions.assertEquals(policy, policy2);
  }

  @Test
  public void test_checkEligibilityForCompaction_returnsIncrementalCompaction_whenRatioBelowThreshold()
  {
    // Set threshold to 0.5 (50%)
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        0.5,
        null
    );

    final CompactionCandidate candidate = createCandidateWithStats(1200L, 10, 400L, 100);

    Assertions.assertEquals(
        CompactionEligibility.incrementalCompaction(
            "Uncompacted bytes ratio[0.25] is below threshold[0.50]"),
        policy.checkEligibilityForCompaction(candidate)
    );
  }

  @Test
  public void test_checkEligibilityForCompaction_returnsFullCompaction_whenRatioAboveThreshold()
  {
    // Set threshold to 0.5 (50%)
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        0.5,
        null
    );

    final CompactionCandidate candidate = createCandidateWithStats(500L, 5, 600L, 100);

    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidate)
    );
  }

  @Test
  public void test_checkEligibilityForCompaction_returnsFullCompaction_whenThresholdIsDefault()
  {
    // Default threshold is 0.0
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null,
        null
    );

    // With default threshold 0.0, any positive ratio >= 0.0, so always FULL_COMPACTION_ELIGIBLE
    final CompactionCandidate candidate = createCandidateWithStats(1000L, 10, 100L, 100);

    Assertions.assertEquals(
        CompactionEligibility.FULL_COMPACTION_ELIGIBLE,
        policy.checkEligibilityForCompaction(candidate)
    );
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

  private CompactionCandidate createCandidateWithStats(
      long compactedBytes,
      int compactedSegments,
      long uncompactedBytes,
      int uncompactedSegments
  )
  {
    final CompactionStatistics compactedStats = CompactionStatistics.create(compactedBytes, compactedSegments, 1L);
    final CompactionStatistics uncompactedStats = CompactionStatistics.create(
        uncompactedBytes,
        uncompactedSegments,
        1L
    );
    return CompactionCandidate.from(List.of(SEGMENT), null)
                              .withCurrentStatus(CompactionStatus.pending(compactedStats, uncompactedStats, ""));
  }

}
