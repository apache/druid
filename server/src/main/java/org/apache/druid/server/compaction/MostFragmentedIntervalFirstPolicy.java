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
  private static final long SIZE_2_GB = 2_000_000_000;
  private static final long SIZE_10_MB = 10_000_000;

  private final int minUncompactedCount;
  private final long minUncompactedBytes;
  private final long maxUncompactedSize;

  @JsonCreator
  public MostFragmentedIntervalFirstPolicy(
      @JsonProperty("minUncompactedCount") @Nullable Integer minUncompactedCount,
      @JsonProperty("minUncompactedBytes") @Nullable Long minUncompactedBytes,
      @JsonProperty("maxUncompactedSize") @Nullable Long maxUncompactedSize
  )
  {
    this.minUncompactedCount = Configs.valueOrDefault(minUncompactedCount, 100);
    this.minUncompactedBytes = Configs.valueOrDefault(minUncompactedBytes, SIZE_10_MB);
    this.maxUncompactedSize = Configs.valueOrDefault(maxUncompactedSize, SIZE_2_GB);
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
  public long getMinUncompactedBytes()
  {
    return minUncompactedBytes;
  }

  /**
   * Maximum average size of uncompacted segments in an interval eligible for
   * compaction. Default value is {@link #SIZE_2_GB}.
   */
  @JsonProperty
  public long getMaxUncompactedSize()
  {
    return maxUncompactedSize;
  }

  @Override
  public int compareCandidates(CompactionCandidate candidateA, CompactionCandidate candidateB)
  {
    return computePriority(candidateA) - computePriority(candidateB) > 0
           ? 1 : -1;
  }

  @Override
  public boolean isEligibleForCompaction(
      CompactionCandidate candidate,
      CompactionTaskStatus latestTaskStatus
  )
  {
    final CompactionStatistics uncompacted = candidate.getUncompactedStats();
    if (uncompacted == null) {
      return true;
    } else if (uncompacted.getNumSegments() < 1) {
      return false;
    } else {
      return uncompacted.getNumSegments() >= minUncompactedCount
          && uncompacted.getTotalBytes() >= minUncompactedBytes
          && (uncompacted.getTotalBytes() / uncompacted.getNumSegments()) <= maxUncompactedSize;
    }
  }

  /**
   * Computes the priority of the given compaction candidate by checking the
   * total number and average size of uncompacted segments.
   */
  private double computePriority(CompactionCandidate candidate)
  {
    final CompactionStatistics compacted = candidate.getCompactedStats();
    final CompactionStatistics uncompacted = candidate.getUncompactedStats();
    if (uncompacted == null || compacted == null) {
      return 0;
    }

    final long avgUncompactedSize = Math.max(1, uncompacted.getTotalBytes() / uncompacted.getNumSegments());

    // Priority increases as size decreases and number increases
    final double normalizingFactor = 1000f;
    return (normalizingFactor * uncompacted.getNumSegments()) / avgUncompactedSize;
  }
}
