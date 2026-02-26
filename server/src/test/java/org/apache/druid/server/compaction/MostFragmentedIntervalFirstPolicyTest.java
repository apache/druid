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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.TestSegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;

public class MostFragmentedIntervalFirstPolicyTest
{
  private static final DataSegment SEGMENT =
      TestSegmentUtils.makeSegment("foo", "1", Intervals.ETERNITY);
  private static final DataSegment SEGMENT2 =
      TestSegmentUtils.makeSegment("foo", "2", Intervals.ETERNITY);
  private static final CompactionCandidate PROPOSED_COMPACTION = CompactionCandidate.from(List.of(SEGMENT, SEGMENT2), null);

  private static final CompactionStatistics DUMMY_COMPACTION_STATS = CompactionStatistics.create(1L, 1L, 1L);

  @Test
  public void test_thresholdValues_ofDefaultPolicy()
  {
    final MostFragmentedIntervalFirstPolicy policy =
        new MostFragmentedIntervalFirstPolicy(null, null, null, null);
    Assertions.assertEquals(100, policy.getMinUncompactedCount());
    Assertions.assertEquals(new HumanReadableBytes("10MiB"), policy.getMinUncompactedBytes());
    Assertions.assertEquals(new HumanReadableBytes("2GiB"), policy.getMaxAverageUncompactedBytesPerSegment());
    Assertions.assertNull(policy.getPriorityDatasource());
  }

  @Test
  public void test_createCandidate_fails_ifUncompactedCountLessThanCutoff()
  {
    final int minUncompactedCount = 10_000;
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        minUncompactedCount,
        HumanReadableBytes.valueOf(1),
        HumanReadableBytes.valueOf(10_000),
        null
    );

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(1, 100L)).build();
    final CompactionCandidateSearchPolicy.Eligibility eligibility1 =
        policy.checkEligibilityForCompaction(new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1));
    Assertions.assertEquals(
        "Uncompacted segments[1] in interval must be at least [10,000]",
        eligibility1.getReason()
    );
    Assertions.assertFalse(eligibility1.isEligible());
    Assertions.assertNull(eligibility1.getMode());

    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(10_001, 100L)).build();
    final CompactionCandidateSearchPolicy.Eligibility eligibility2 =
        policy.checkEligibilityForCompaction(new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2));
    Assertions.assertTrue(eligibility2.isEligible());
    Assertions.assertEquals(CompactionMode.FULL_COMPACTION, eligibility2.getMode());
  }

  @Test
  public void test_createCandidate_fails_ifUncompactedBytesLessThanCutoff()
  {
    final HumanReadableBytes minUncompactedBytes = HumanReadableBytes.valueOf(10_000);
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        minUncompactedBytes,
        HumanReadableBytes.valueOf(10_000),
        null
    );

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(1, 100L)).build();
    final CompactionCandidateSearchPolicy.Eligibility eligibility1 = policy.checkEligibilityForCompaction(new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1));
    Assertions.assertEquals("Uncompacted bytes[100] in interval must be at least [10,000]", eligibility1.getReason());
    Assertions.assertFalse(eligibility1.isEligible());

    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(100, 10_000L)).build();
    final CompactionCandidateSearchPolicy.Eligibility eligibility2 = policy.checkEligibilityForCompaction(new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2));
    Assertions.assertTrue(eligibility2.isEligible());
  }

  @Test
  public void test_createCandidate_fails_ifAvgSegmentSizeGreaterThanCutoff()
  {
    final HumanReadableBytes maxAvgSegmentSize = HumanReadableBytes.valueOf(100);
    final MostFragmentedIntervalFirstPolicy policy = new MostFragmentedIntervalFirstPolicy(
        1,
        HumanReadableBytes.valueOf(100),
        maxAvgSegmentSize,
        null
    );

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(1, 10_000L)).build();
    final CompactionCandidateSearchPolicy.Eligibility eligibility1 = policy.checkEligibilityForCompaction(new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1));
    Assertions.assertEquals(
        "Average size[10,000] of uncompacted segments in interval must be at most [100]",
        eligibility1.getReason()
    );
    Assertions.assertFalse(eligibility1.isEligible());
    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(1, 100L)).build();
    final CompactionCandidateSearchPolicy.Eligibility eligibility2 = policy.checkEligibilityForCompaction(new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2));
    Assertions.assertTrue(eligibility2.isEligible());
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

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(1, 1_000L)).build();
    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(2, 500L)).build();

    final CompactionCandidateAndStatus candidateA = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1);
    final CompactionCandidateAndStatus candidateB = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2);
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

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(1, 1000L)).build();
    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(2, 1000L)).build();

    final CompactionCandidateAndStatus candidateA = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1);
    final CompactionCandidateAndStatus candidateB = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2);
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

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(10, 500L)).build();
    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(10, 1000L)).build();

    final CompactionCandidateAndStatus candidateA = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1);
    final CompactionCandidateAndStatus candidateB = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2);
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

    final CompactionStatus status1 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(100, 25)).build();
    final CompactionStatus status2 =
        eligibilityBuilder().compacted(DUMMY_COMPACTION_STATS).uncompacted(createStats(400, 100)).build();

    final CompactionCandidateAndStatus candidateA = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status1);
    final CompactionCandidateAndStatus candidateB = new CompactionCandidateAndStatus(PROPOSED_COMPACTION, status2);
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
        new MostFragmentedIntervalFirstPolicy(null, null, null, null);
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final CompactionCandidateSearchPolicy policy2 =
        mapper.readValue(mapper.writeValueAsString(policy), CompactionCandidateSearchPolicy.class);
    Assertions.assertEquals(policy, policy2);
  }

  private CompactionStatistics createStats(int numSegments, long avgSizeBytes)
  {
    return CompactionStatistics.create(avgSizeBytes * numSegments, numSegments, 1L);
  }

  private static CompactionStatus.CompactionStatusBuilder eligibilityBuilder()
  {
    return CompactionStatus.builder(CompactionStatus.State.ELIGIBLE, "approve");
  }
}
