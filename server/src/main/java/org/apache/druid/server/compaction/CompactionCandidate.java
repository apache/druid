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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Non-empty list of segments of a datasource being considered for compaction.
 * A candidate typically contains all the segments of a single time chunk.
 */
public class CompactionCandidate
{
  private final List<DataSegment> segments;
  private final Interval umbrellaInterval;
  private final Interval compactionInterval;
  private final String dataSource;
  private final long totalBytes;
  private final int numIntervals;

  private final CompactionCandidateSearchPolicy.Eligibility policyEligiblity;
  private final CompactionStatus currentStatus;

  public static CompactionCandidate from(
      List<DataSegment> segments,
      @Nullable Granularity targetSegmentGranularity
  )
  {
    if (segments == null || segments.isEmpty()) {
      throw InvalidInput.exception("Segments to compact must be non-empty");
    }

    final Set<Interval> segmentIntervals =
        segments.stream().map(DataSegment::getInterval).collect(Collectors.toSet());
    final Interval umbrellaInterval = JodaUtils.umbrellaInterval(segmentIntervals);
    final Interval compactionInterval =
        targetSegmentGranularity == null
        ? umbrellaInterval
        : JodaUtils.umbrellaInterval(targetSegmentGranularity.getIterable(umbrellaInterval));

    return new CompactionCandidate(
        segments,
        umbrellaInterval,
        compactionInterval,
        segmentIntervals.size(),
        null,
        null
    );
  }

  private CompactionCandidate(
      List<DataSegment> segments,
      Interval umbrellaInterval,
      Interval compactionInterval,
      int numDistinctSegmentIntervals,
      CompactionCandidateSearchPolicy.Eligibility policyEligiblity,
      @Nullable CompactionStatus currentStatus
  )
  {
    this.segments = segments;
    this.totalBytes = segments.stream().mapToLong(DataSegment::getSize).sum();

    this.umbrellaInterval = umbrellaInterval;
    this.compactionInterval = compactionInterval;

    this.numIntervals = numDistinctSegmentIntervals;
    this.dataSource = segments.get(0).getDataSource();
    this.policyEligiblity = policyEligiblity;
    this.currentStatus = currentStatus;
  }

  /**
   * @return Non-empty list of segments that make up this candidate.
   */
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  public long getTotalBytes()
  {
    return totalBytes;
  }

  public int numSegments()
  {
    return segments.size();
  }

  /**
   * Umbrella interval of all the segments in this candidate. This typically
   * corresponds to a single time chunk in the segment timeline.
   */
  public Interval getUmbrellaInterval()
  {
    return umbrellaInterval;
  }

  /**
   * Interval aligned to the target segment granularity used for the compaction
   * task. This interval completely contains the {@link #umbrellaInterval}.
   */
  public Interval getCompactionInterval()
  {
    return compactionInterval;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public CompactionStatistics getStats()
  {
    return CompactionStatistics.create(totalBytes, numSegments(), numIntervals);
  }

  @Nullable
  public CompactionStatistics getCompactedStats()
  {
    return (currentStatus == null || currentStatus.getCompactedStats() == null)
           ? null : currentStatus.getCompactedStats();
  }

  @Nullable
  public CompactionStatistics getUncompactedStats()
  {
    return (currentStatus == null || currentStatus.getUncompactedStats() == null)
           ? null : currentStatus.getUncompactedStats();
  }

  /**
   * Current compaction status of the time chunk corresponding to this candidate.
   */
  @Nullable
  public CompactionStatus getCurrentStatus()
  {
    return currentStatus;
  }

  @Nullable
  public CompactionCandidateSearchPolicy.Eligibility getPolicyEligibility()
  {
    return policyEligiblity;
  }

  /**
   * Creates a copy of this CompactionCandidate object with the given status.
   */
  public CompactionCandidate withCurrentStatus(CompactionStatus status)
  {
    return new CompactionCandidate(
        segments,
        umbrellaInterval,
        compactionInterval,
        numIntervals,
        policyEligiblity,
        status
    );
  }

  public CompactionCandidate withPolicyEligibility(CompactionCandidateSearchPolicy.Eligibility eligibility)
  {
    return new CompactionCandidate(
        segments,
        umbrellaInterval,
        compactionInterval,
        numIntervals,
        eligibility,
        currentStatus
    );
  }

  /**
   * Evaluates this candidate for compaction eligibility based on the provided
   * compaction configuration and search policy.
   * <p>
   * This method first evaluates the candidate against the compaction configuration
   * using a {@link CompactionStatus.Evaluator} to determine if any segments need
   * compaction. If segments are pending compaction, the search policy is consulted
   * to determine the type of compaction:
   * <ul>
   * <li><b>NOT_ELIGIBLE</b>: Returns a candidate with status SKIPPED, indicating
   *     the policy decided compaction should not occur at this time</li>
   * <li><b>FULL_COMPACTION</b>: Returns this candidate with status PENDING,
   *     indicating all segments should be compacted</li>
   * <li><b>INCREMENTAL_COMPACTION</b>: Returns a new candidate containing only
   *     the uncompacted segments (as determined by the evaluator), with status
   *     PENDING for incremental compaction</li>
   * </ul>
   *
   * @param config       the compaction configuration for the datasource
   * @param searchPolicy the policy used to determine compaction eligibility
   * @return a CompactionCandidate with updated status and potentially filtered segments
   */
  public CompactionCandidate evaluate(
      DataSourceCompactionConfig config,
      CompactionCandidateSearchPolicy searchPolicy,
      IndexingStateFingerprintMapper fingerprintMapper
  )
  {
    CompactionStatus.Evaluator evaluator = new CompactionStatus.Evaluator(this, config, fingerprintMapper);
    Pair<CompactionCandidateSearchPolicy.Eligibility, CompactionStatus> evaluated = evaluator.evaluate();
    switch (Objects.requireNonNull(evaluated.lhs).getPolicyEligibility()) {
      case NOT_APPLICABLE:
      case NOT_ELIGIBLE:
        return this.withPolicyEligibility(evaluated.lhs).withCurrentStatus(evaluated.rhs);
      case FULL_COMPACTION: // evaluator has decided compaction is needed, policy needs to further check
        if (!evaluated.rhs.getState().equals(CompactionStatus.State.PENDING)) {
          throw DruidException.defensive(
              "Evaluated compaction status should be PENDING, got status[%s] instead.",
              evaluated.rhs.getState()
          );
        }
        final CompactionCandidateSearchPolicy.Eligibility searchPolicyEligibility =
            searchPolicy.checkEligibilityForCompaction(this, null);
        switch (searchPolicyEligibility.getPolicyEligibility()) {
          case
              NOT_ELIGIBLE: // although evaluator thinks this interval qualifies for compaction, but policy decided not its turn yet.
            return this.withPolicyEligibility(searchPolicyEligibility)
                       .withCurrentStatus(CompactionStatus.skipped(
                           "Rejected by search policy: %s",
                           searchPolicyEligibility.getReason()
                       ));
          case FULL_COMPACTION:
            return this.withPolicyEligibility(searchPolicyEligibility).withCurrentStatus(evaluated.rhs);
          case
              INCREMENTAL_COMPACTION: // policy decided to perform an incremental compaction, the uncompactedSegments is a subset of the original segments.
            return new CompactionCandidate(
                evaluator.getUncompactedSegments(),
                umbrellaInterval,
                compactionInterval,
                numIntervals,
                searchPolicyEligibility,
                evaluated.rhs
            );
          default:
            throw DruidException.defensive("Unexpected eligibility[%s] from policy", searchPolicyEligibility);
        }
      case INCREMENTAL_COMPACTION: // evaluator cant decide when to perform an incremental compaction
      default:
        throw DruidException.defensive("Unexpected eligibility[%s]", evaluated.rhs);
    }
  }

  @Override
  public String toString()
  {
    return "SegmentsToCompact{" +
           "datasource=" + dataSource +
           ", umbrellaInterval=" + umbrellaInterval +
           ", compactionInterval=" + compactionInterval +
           ", numIntervals=" + numIntervals +
           ", segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", totalSize=" + totalBytes +
           ", policyEligiblity=" + policyEligiblity +
           ", currentStatus=" + currentStatus +
           '}';
  }
}
