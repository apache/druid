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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.HumanReadableBytes;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Objects;

/**
 * Experimental {@link CompactionCandidateSearchPolicy} which prioritizes compaction
 * of intervals with the largest number of small uncompacted segments.
 * <p>
 * This policy favors cluster stability (by prioritizing reduction of segment
 * count) over performance of queries on newer intervals. For the latter, use
 * {@link NewestSegmentFirstPolicy}.
 */
@UnstableApi
public class MostFragmentedIntervalFirstPolicy extends BaseCandidateSearchPolicy
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
      @JsonProperty("maxAverageUncompactedBytesPerSegment") @Nullable
      HumanReadableBytes maxAverageUncompactedBytesPerSegment,
      @JsonProperty("priorityDatasource") @Nullable String priorityDatasource
  )
  {
    super(priorityDatasource);

    InvalidInput.conditionalException(
        minUncompactedCount == null || minUncompactedCount > 0,
        "'minUncompactedCount'[%s] must be greater than 0",
        minUncompactedCount
    );
    InvalidInput.conditionalException(
        maxAverageUncompactedBytesPerSegment == null || maxAverageUncompactedBytesPerSegment.getBytes() > 0,
        "'minUncompactedCount'[%s] must be greater than 0",
        maxAverageUncompactedBytesPerSegment
    );

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
  protected Comparator<CompactionCandidate> getSegmentComparator()
  {
    return this::compare;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    MostFragmentedIntervalFirstPolicy policy = (MostFragmentedIntervalFirstPolicy) o;
    return minUncompactedCount == policy.minUncompactedCount
           && Objects.equals(minUncompactedBytes, policy.minUncompactedBytes)
           && Objects.equals(maxAverageUncompactedBytesPerSegment, policy.maxAverageUncompactedBytesPerSegment);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        minUncompactedCount,
        minUncompactedBytes,
        maxAverageUncompactedBytesPerSegment
    );
  }

  @Override
  public String toString()
  {
    return "MostFragmentedIntervalFirstPolicy{" +
           "minUncompactedCount=" + minUncompactedCount +
           ", minUncompactedBytes=" + minUncompactedBytes +
           ", maxAverageUncompactedBytesPerSegment=" + maxAverageUncompactedBytesPerSegment +
           ", priorityDataSource='" + getPriorityDatasource() + '\'' +
           '}';
  }

  private int compare(CompactionCandidate candidateA, CompactionCandidate candidateB)
  {
    final double fragmentationDiff
        = computeFragmentationIndex(candidateB) - computeFragmentationIndex(candidateA);
    return (int) fragmentationDiff;
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
          "Uncompacted segments[%,d] in interval must be at least [%,d]",
          uncompacted.getNumSegments(), minUncompactedCount
      );
    } else if (uncompacted.getTotalBytes() < minUncompactedBytes.getBytes()) {
      return Eligibility.fail(
          "Uncompacted bytes[%,d] in interval must be at least [%,d]",
          uncompacted.getTotalBytes(), minUncompactedBytes.getBytes()
      );
    }

    final long avgSegmentSize = (uncompacted.getTotalBytes() / uncompacted.getNumSegments());
    if (avgSegmentSize > maxAverageUncompactedBytesPerSegment.getBytes()) {
      return Eligibility.fail(
          "Average size[%,d] of uncompacted segments in interval must be at most [%,d]",
          avgSegmentSize, maxAverageUncompactedBytesPerSegment.getBytes()
      );
    } else {
      return Eligibility.OK;
    }
  }

  /**
   * Computes the degree of fragmentation in the interval of the given compaction
   * candidate. Calculated as the number of uncompacted segments plus an additional
   * term that captures the "smallness" of segments in that interval.
   * A higher fragmentation index causes the candidate to be higher in priority
   * for compaction.
   */
  private double computeFragmentationIndex(CompactionCandidate candidate)
  {
    final CompactionStatistics uncompacted = candidate.getUncompactedStats();
    if (uncompacted == null || uncompacted.getNumSegments() < 1 || uncompacted.getTotalBytes() < 1) {
      return 0;
    }

    final long avgUncompactedSize = Math.max(1, uncompacted.getTotalBytes() / uncompacted.getNumSegments());

    // Fragmentation index increases as uncompacted segment count increases
    double segmentCountTerm = uncompacted.getNumSegments();

    // Fragmentation index increases as avg uncompacted segment size decreases
    double segmentSizeTerm =
        (1.0f * minUncompactedCount * maxAverageUncompactedBytesPerSegment.getBytes()) / avgUncompactedSize;

    return segmentCountTerm + segmentSizeTerm;
  }
}
