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

import com.google.common.base.Preconditions;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.SegmentUtils;
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
  /**
   * Non-empty list of segments of a datasource being proposed for compaction.
   * A proposed compaction typically contains all the segments of a single time chunk.
   */
  public static class ProposedCompaction
  {
    private final List<DataSegment> segments;
    private final Interval umbrellaInterval;
    private final Interval compactionInterval;
    private final String dataSource;
    private final long totalBytes;
    private final int numIntervals;

    public static ProposedCompaction from(
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

      return new ProposedCompaction(
          segments,
          umbrellaInterval,
          compactionInterval,
          segmentIntervals.size()
      );
    }

    ProposedCompaction(
        List<DataSegment> segments,
        Interval umbrellaInterval,
        Interval compactionInterval,
        int numDistinctSegmentIntervals
    )
    {
      this.segments = segments;
      this.totalBytes = segments.stream().mapToLong(DataSegment::getSize).sum();

      this.umbrellaInterval = umbrellaInterval;
      this.compactionInterval = compactionInterval;

      this.numIntervals = numDistinctSegmentIntervals;
      this.dataSource = segments.get(0).getDataSource();
    }

    /**
     * @return Non-empty list of segments that make up this proposed compaction.
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
     * Umbrella interval of all the segments in this proposed compaction. This typically
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

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ProposedCompaction that = (ProposedCompaction) o;
      return totalBytes == that.totalBytes
             && numIntervals == that.numIntervals
             && segments.equals(that.segments)
             && umbrellaInterval.equals(that.umbrellaInterval)
             && compactionInterval.equals(that.compactionInterval)
             && dataSource.equals(that.dataSource);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(segments, umbrellaInterval, compactionInterval, dataSource, totalBytes, numIntervals);
    }

    @Override
    public String toString()
    {
      return "ProposedCompaction{" +
             "datasource=" + dataSource +
             ", umbrellaInterval=" + umbrellaInterval +
             ", compactionInterval=" + compactionInterval +
             ", numIntervals=" + numIntervals +
             ", segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
             ", totalSize=" + totalBytes +
             '}';
    }
  }

  /**
   * Used by {@link CompactionStatusTracker#computeCompactionTaskState(CompactionCandidate)}.
   * The callsite then determines whether to launch compaction task or not.
   */
  public enum TaskState
  {
    // no other compaction candidate is running, we can start a new task
    READY,
    // compaction candidate is already running under a task
    TASK_IN_PROGRESS,
    // compaction candidate has recently been completed, and the segment timeline has not yet updated after that
    RECENTLY_COMPLETED
  }

  private final ProposedCompaction proposedCompaction;

  private final CompactionStatus eligibility;
  @Nullable
  private final String policyNote;
  private final CompactionMode mode;

  CompactionCandidate(
      ProposedCompaction proposedCompaction,
      CompactionStatus eligibility,
      @Nullable String policyNote,
      CompactionMode mode
  )
  {
    this.proposedCompaction = Preconditions.checkNotNull(proposedCompaction, "proposedCompaction");
    this.eligibility = Preconditions.checkNotNull(eligibility, "eligibility");
    this.policyNote = policyNote;
    this.mode = Preconditions.checkNotNull(mode, "mode");
  }

  public ProposedCompaction getProposedCompaction()
  {
    return proposedCompaction;
  }

  /**
   * @return Non-empty list of segments that make up this candidate.
   */
  public List<DataSegment> getSegments()
  {
    return proposedCompaction.getSegments();
  }

  public long getTotalBytes()
  {
    return proposedCompaction.getTotalBytes();
  }

  public int numSegments()
  {
    return proposedCompaction.numSegments();
  }

  /**
   * Umbrella interval of all the segments in this candidate. This typically
   * corresponds to a single time chunk in the segment timeline.
   */
  public Interval getUmbrellaInterval()
  {
    return proposedCompaction.getUmbrellaInterval();
  }

  /**
   * Interval aligned to the target segment granularity used for the compaction
   * task. This interval completely contains the {@link #getUmbrellaInterval()}.
   */
  public Interval getCompactionInterval()
  {
    return proposedCompaction.getCompactionInterval();
  }

  public String getDataSource()
  {
    return proposedCompaction.getDataSource();
  }

  public CompactionStatistics getStats()
  {
    return proposedCompaction.getStats();
  }

  @Nullable
  public String getPolicyNote()
  {
    return policyNote;
  }

  public CompactionMode getMode()
  {
    return mode;
  }

  public CompactionStatus getEligibility()
  {
    return eligibility;
  }

  @Override
  public String toString()
  {
    return "SegmentsToCompact{" +
           ", proposedCompaction=" + proposedCompaction +
           ", eligibility=" + eligibility +
           ", policyNote=" + policyNote +
           ", mode=" + mode +
           '}';
  }
}
