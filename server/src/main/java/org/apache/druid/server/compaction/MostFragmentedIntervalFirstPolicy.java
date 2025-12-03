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
import org.apache.druid.common.config.Configs;
import org.apache.druid.java.util.common.HumanReadableBytes;

import javax.annotation.Nullable;

/**
 * {@link CompactionCandidateSearchPolicy} which prioritizes compaction of the
 * intervals with the largest number of small uncompacted segments.
 * <p>
 * This policy favors cluster stability (by prioritizing reduction of segment
 * count) over performance of queries on newer intervals. For the latter, use
 * {@link NewestSegmentFirstPolicy}.
 */
public class MostFragmentedIntervalFirstPolicy implements CompactionCandidateSearchPolicy
{
  private static final HumanReadableBytes SIZE_2_GB = new HumanReadableBytes("2GiB");
  private static final HumanReadableBytes SIZE_10_MB = new HumanReadableBytes("10MiB");

  private final int minUncompactedCount;
  private final HumanReadableBytes minUncompactedBytes;
  private final HumanReadableBytes maxAverageUncompactedBytesPerSegment;

  @JsonCreator
  public MostFragmentedIntervalFirstPolicy(
      @JsonProperty("minUncompactedCount") @Nullable Integer minUncompactedCount,
      @JsonProperty("minUncompactedBytes") @Nullable HumanReadableBytes minUncompactedBytes,
      @JsonProperty("maxAverageUncompactedBytesPerSegment") @Nullable HumanReadableBytes maxAverageUncompactedBytesPerSegment
  )
  {
    this.minUncompactedCount = Configs.valueOrDefault(minUncompactedCount, 100);
    this.minUncompactedBytes = Configs.valueOrDefault(minUncompactedBytes, SIZE_10_MB);
    this.maxAverageUncompactedBytesPerSegment
        = Configs.valueOrDefault(maxAverageUncompactedBytesPerSegment, SIZE_2_GB);
  }

  /**
   * Minimum number of uncompacted segments that must be present in an interval
   * to make it eligible for compaction.
   */
  @JsonProperty
  public int getMinUncompactedCount()
  {
    return minUncompactedCount;
  }

  /**
   * Minimum total bytes of uncompacted segments that must be present in an
   * interval to make it eligible for compaction. Default value is {@link #SIZE_10_MB}.
   */
  @JsonProperty
  public HumanReadableBytes getMinUncompactedBytes()
  {
    return minUncompactedBytes;
  }

  /**
   * Maximum average size of uncompacted segments in an interval eligible for
   * compaction. Default value is {@link #SIZE_2_GB}.
   */
  @JsonProperty
  public HumanReadableBytes getMaxAverageUncompactedBytesPerSegment()
  {
    return maxAverageUncompactedBytesPerSegment;
  }

  @Override
  public int compareCandidates(CompactionCandidate candidateA, CompactionCandidate candidateB)
  {
    final double fragmentationDiff
        = computeFragmentationIndex(candidateA) - computeFragmentationIndex(candidateB);
    return fragmentationDiff > 0 ? 1 : -1;
  }

  @Override
  public Eligibility checkEligibilityForCompaction(
      CompactionCandidate candidate,
      CompactionTaskStatus latestTaskStatus
  )
  {
    final CompactionStatistics uncompacted = candidate.getUncompactedStats();
    if (uncompacted == null) {
      return Eligibility.OK;
    } else if (uncompacted.getNumSegments() < 1) {
      return Eligibility.fail("No uncompacted segments in interval");
    } else if (uncompacted.getNumSegments() < minUncompactedCount) {
      return Eligibility.fail(
          "Uncompacted segments[%,d] in interval must be at least [%,d].",
          uncompacted.getNumSegments(), minUncompactedCount
      );
    } else if (uncompacted.getTotalBytes() < minUncompactedBytes.getBytes()) {
      return Eligibility.fail(
          "Uncompacted bytes[%,d] in interval must be at least [%,d].",
          minUncompactedBytes.getBytes(), uncompacted.getTotalBytes()
      );
    }

    final long avgSegmentSize = (uncompacted.getTotalBytes() / uncompacted.getNumSegments());
    if (avgSegmentSize > maxAverageUncompactedBytesPerSegment.getBytes()) {
      return Eligibility.fail(
          "Average size[%,d] of uncompacted segments in interval must be at most [%,d].",
          avgSegmentSize, maxAverageUncompactedBytesPerSegment.getBytes()
      );
    } else {
      return Eligibility.OK;
    }
  }

  /**
   * Computes the degree of fragmentation of the given compaction candidate by
   * checking the total number and average size of uncompacted segments.
   * A higher fragmentation index causes the candidate to be higher in priority
   * for compaction.
   */
  private double computeFragmentationIndex(CompactionCandidate candidate)
  {
    final CompactionStatistics compacted = candidate.getCompactedStats();
    final CompactionStatistics uncompacted = candidate.getUncompactedStats();
    if (uncompacted == null || compacted == null) {
      return 0;
    }

    final long avgUncompactedSize = Math.max(1, uncompacted.getTotalBytes() / uncompacted.getNumSegments());

    // Fragmentation index increases as segment count increases and avg size decreases
    return (1f * uncompacted.getNumSegments()) / avgUncompactedSize;
  }
}
