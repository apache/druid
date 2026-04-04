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
import org.apache.druid.java.util.common.logger.Logger;

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
  private static final Logger logger = new Logger(MostFragmentedIntervalFirstPolicy.class);
  private static final HumanReadableBytes SIZE_2_GB = new HumanReadableBytes("2GiB");
  private static final HumanReadableBytes SIZE_10_MB = new HumanReadableBytes("10MiB");

  private final int minUncompactedCount;
  private final HumanReadableBytes minUncompactedBytes;
  private final HumanReadableBytes maxAverageUncompactedBytesPerSegment;
  private final int minUncompactedBytesPercentForFullCompaction;
  private final int minUncompactedRowsPercentForFullCompaction;

  @JsonCreator
  public MostFragmentedIntervalFirstPolicy(
      @JsonProperty("minUncompactedCount") @Nullable Integer minUncompactedCount,
      @JsonProperty("minUncompactedBytes") @Nullable HumanReadableBytes minUncompactedBytes,
      @JsonProperty("maxAverageUncompactedBytesPerSegment") @Nullable
      HumanReadableBytes maxAverageUncompactedBytesPerSegment,
      @JsonProperty("minUncompactedBytesPercentForFullCompaction") @Nullable
      Integer minUncompactedBytesPercentForFullCompaction,
      @JsonProperty("minUncompactedRowsPercentForFullCompaction") @Nullable
      Integer minUncompactedRowsPercentForFullCompaction,
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
        minUncompactedBytesPercentForFullCompaction == null
        || (minUncompactedBytesPercentForFullCompaction >= 0
            && minUncompactedBytesPercentForFullCompaction < 100),
        "'minUncompactedBytesPercentForFullCompaction'[%s] must be between 0 and 100",
        minUncompactedBytesPercentForFullCompaction
    );
    InvalidInput.conditionalException(
        minUncompactedRowsPercentForFullCompaction == null
        || (minUncompactedRowsPercentForFullCompaction >= 0
            && minUncompactedRowsPercentForFullCompaction < 100),
        "'minUncompactedRowsPercentForFullCompaction'[%s] must be between 0 and 100",
        minUncompactedRowsPercentForFullCompaction
    );

    this.minUncompactedCount = Configs.valueOrDefault(minUncompactedCount, 100);
    this.minUncompactedBytes = Configs.valueOrDefault(minUncompactedBytes, SIZE_10_MB);
    this.maxAverageUncompactedBytesPerSegment
        = Configs.valueOrDefault(maxAverageUncompactedBytesPerSegment, SIZE_2_GB);
    this.minUncompactedBytesPercentForFullCompaction =
        Configs.valueOrDefault(minUncompactedBytesPercentForFullCompaction, 0);
    this.minUncompactedRowsPercentForFullCompaction =
        Configs.valueOrDefault(minUncompactedRowsPercentForFullCompaction, 0);
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
   * Threshold percentage of uncompacted bytes to total bytes below which
   * minor compaction is eligible instead of full compaction.
   * Default value is 0.
   */
  @JsonProperty
  public int minUncompactedBytesPercentForFullCompaction()
  {
    return minUncompactedBytesPercentForFullCompaction;
  }

  /**
   * Threshold percentage of uncompacted rows to total rows below which
   * minor compaction is eligible instead of full compaction.
   * Default value is 0.
   */
  @JsonProperty
  public int minUncompactedRowsPercentForFullCompaction()
  {
    return minUncompactedRowsPercentForFullCompaction;
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
           && Objects.equals(maxAverageUncompactedBytesPerSegment, policy.maxAverageUncompactedBytesPerSegment)
           && minUncompactedBytesPercentForFullCompaction == policy.minUncompactedBytesPercentForFullCompaction
           && minUncompactedRowsPercentForFullCompaction == policy.minUncompactedRowsPercentForFullCompaction;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        minUncompactedCount,
        minUncompactedBytes,
        maxAverageUncompactedBytesPerSegment,
        minUncompactedBytesPercentForFullCompaction,
        minUncompactedRowsPercentForFullCompaction
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
        ", minUncompactedBytesPercentForFullCompaction=" + minUncompactedBytesPercentForFullCompaction +
        ", minUncompactedRowsPercentForFullCompaction=" + minUncompactedRowsPercentForFullCompaction +
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
      return Eligibility.FULL;
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
    }

    final double uncompactedBytesRatio = (double) uncompacted.getTotalBytes() /
                                         (uncompacted.getTotalBytes() + candidate.getCompactedStats().getTotalBytes())
                                         * 100;
    if (uncompactedBytesRatio < minUncompactedBytesPercentForFullCompaction) {
      return Eligibility.minor(
          "Uncompacted bytes ratio[%.2f] is below threshold[%d]",
          uncompactedBytesRatio,
          minUncompactedBytesPercentForFullCompaction
      );
    }

    // Check uncompacted rows ratio if total rows are available
    final Long uncompactedRows = uncompacted.getTotalRows();
    final Long compactedRows = candidate.getCompactedStats().getTotalRows();
    if (uncompactedRows != null && compactedRows != null) {
      if (uncompactedRows + compactedRows > 0) {
        final double uncompactedRowsRatio = (double) uncompactedRows / (uncompactedRows + compactedRows) * 100;
        if (uncompactedRowsRatio < minUncompactedRowsPercentForFullCompaction) {
          return Eligibility.minor(
              "Uncompacted rows ratio[%.2f] is below threshold[%d]",
              uncompactedRowsRatio,
              minUncompactedRowsPercentForFullCompaction
          );
        }
      } else {
        logger.error("Zero total rows in compaction candidate, something is wrong");
      }
    }

    return Eligibility.FULL;
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
