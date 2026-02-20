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
import org.apache.druid.java.util.common.StringUtils;

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
  private final double incrementalCompactionUncompactedRatioThreshold;

  @JsonCreator
  public MostFragmentedIntervalFirstPolicy(
      @JsonProperty("minUncompactedCount") @Nullable Integer minUncompactedCount,
      @JsonProperty("minUncompactedBytes") @Nullable HumanReadableBytes minUncompactedBytes,
      @JsonProperty("maxAverageUncompactedBytesPerSegment") @Nullable
      HumanReadableBytes maxAverageUncompactedBytesPerSegment,
      @JsonProperty("incrementalCompactionUncompactedRatioThreshold") @Nullable
      Double incrementalCompactionUncompactedRatioThreshold,
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
    InvalidInput.conditionalException(
        incrementalCompactionUncompactedRatioThreshold == null
        || (incrementalCompactionUncompactedRatioThreshold >= 0.0d
            && incrementalCompactionUncompactedRatioThreshold < 1.0d),
        "'incrementalCompactionUncompactedRatioThreshold'[%s] must be between 0.0 and 1.0",
        incrementalCompactionUncompactedRatioThreshold
    );

    this.minUncompactedCount = Configs.valueOrDefault(minUncompactedCount, 100);
    this.minUncompactedBytes = Configs.valueOrDefault(minUncompactedBytes, SIZE_10_MB);
    this.maxAverageUncompactedBytesPerSegment
        = Configs.valueOrDefault(maxAverageUncompactedBytesPerSegment, SIZE_2_GB);
    this.incrementalCompactionUncompactedRatioThreshold =
        Configs.valueOrDefault(incrementalCompactionUncompactedRatioThreshold, 0.0d);
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

  /**
   * Threshold ratio of uncompacted bytes to compacted bytes below which
   * incremental compaction is eligible instead of full compaction.
   * Default value is 0.0.
   */
  @JsonProperty
  public double getIncrementalCompactionUncompactedRatioThreshold()
  {
    return incrementalCompactionUncompactedRatioThreshold;
  }

  @Override
  protected Comparator<CompactionCandidate> getSegmentComparator()
  {
    return Comparator.comparing(o -> Objects.requireNonNull(o.getEligibility()), this::compare);
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
           && Objects.equals(maxAverageUncompactedBytesPerSegment, policy.maxAverageUncompactedBytesPerSegment)
           // Use Double.compare instead of == to handle NaN correctly and keep equals() consistent with hashCode() (especially for +0.0 vs -0.0).
           && Double.compare(
        incrementalCompactionUncompactedRatioThreshold,
        policy.incrementalCompactionUncompactedRatioThreshold
    ) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        minUncompactedCount,
        minUncompactedBytes,
        maxAverageUncompactedBytesPerSegment,
        incrementalCompactionUncompactedRatioThreshold
    );
  }

  @Override
  public String toString()
  {
    return
        "MostFragmentedIntervalFirstPolicy{" +
        "minUncompactedCount=" + minUncompactedCount +
        ", minUncompactedBytes=" + minUncompactedBytes +
        ", maxAverageUncompactedBytesPerSegment=" + maxAverageUncompactedBytesPerSegment +
        ", incrementalCompactionUncompactedRatioThreshold=" + incrementalCompactionUncompactedRatioThreshold +
        ", priorityDataSource='" + getPriorityDatasource() + '\'' +
        '}';
  }

  private int compare(CompactionStatus candidateA, CompactionStatus candidateB)
  {
    final double fragmentationDiff
        = computeFragmentationIndex(candidateB) - computeFragmentationIndex(candidateA);
    return (int) fragmentationDiff;
  }

  @Override
  public CompactionCandidate createCandidate(
      CompactionCandidate.ProposedCompaction candidate,
      CompactionStatus eligibility
  )
  {
    final CompactionStatistics uncompacted = Objects.requireNonNull(eligibility.getUncompactedStats());

    if (uncompacted.getNumSegments() < 1) {
      return CompactionMode.failWithPolicyCheck(candidate, eligibility, "No uncompacted segments in interval");
    } else if (uncompacted.getNumSegments() < minUncompactedCount) {
      return CompactionMode.failWithPolicyCheck(
          candidate,
          eligibility,
          "Uncompacted segments[%,d] in interval must be at least [%,d]",
          uncompacted.getNumSegments(),
          minUncompactedCount
      );
    } else if (uncompacted.getTotalBytes() < minUncompactedBytes.getBytes()) {
      return CompactionMode.failWithPolicyCheck(
          candidate,
          eligibility,
          "Uncompacted bytes[%,d] in interval must be at least [%,d]",
          uncompacted.getTotalBytes(),
          minUncompactedBytes.getBytes()
      );
    }

    final long avgSegmentSize = (uncompacted.getTotalBytes() / uncompacted.getNumSegments());
    if (avgSegmentSize > maxAverageUncompactedBytesPerSegment.getBytes()) {
      return CompactionMode.failWithPolicyCheck(
          candidate,
          eligibility,
          "Average size[%,d] of uncompacted segments in interval must be at most [%,d]",
          avgSegmentSize,
          maxAverageUncompactedBytesPerSegment.getBytes()
      );
    }

    final double uncompactedBytesRatio = (double) uncompacted.getTotalBytes() /
                                         (uncompacted.getTotalBytes() + eligibility.getCompactedStats()
                                                                                   .getTotalBytes());
    if (uncompactedBytesRatio < incrementalCompactionUncompactedRatioThreshold) {
      String policyNote = StringUtils.format(
          "Uncompacted bytes ratio[%.2f] is below threshold[%.2f]",
          uncompactedBytesRatio,
          incrementalCompactionUncompactedRatioThreshold
      );
      return CompactionMode.INCREMENTAL_COMPACTION.createCandidate(candidate, eligibility, policyNote);
    } else {
      return CompactionMode.FULL_COMPACTION.createCandidate(candidate, eligibility);
    }
  }

  /**
   * Computes the degree of fragmentation in the interval of the given compaction
   * candidate. Calculated as the number of uncompacted segments plus an additional
   * term that captures the "smallness" of segments in that interval.
   * A higher fragmentation index causes the candidate to be higher in priority
   * for compaction.
   */
  private double computeFragmentationIndex(CompactionStatus eligibility)
  {
    final CompactionStatistics uncompacted = eligibility.getUncompactedStats();
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
