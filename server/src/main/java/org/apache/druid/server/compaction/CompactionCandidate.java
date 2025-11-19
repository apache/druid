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

import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
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
        null
    );
  }

  private CompactionCandidate(
      List<DataSegment> segments,
      Interval umbrellaInterval,
      Interval compactionInterval,
      int numDistinctSegmentIntervals,
      @Nullable CompactionStatus currentStatus
  )
  {
    this.segments = segments;
    this.totalBytes = segments.stream().mapToLong(DataSegment::getSize).sum();

    this.umbrellaInterval = umbrellaInterval;
    this.compactionInterval = compactionInterval;

    this.numIntervals = numDistinctSegmentIntervals;
    this.dataSource = segments.get(0).getDataSource();
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

  /**
   * Current compaction status of the time chunk corresponding to this candidate.
   */
  @Nullable
  public CompactionStatus getCurrentStatus()
  {
    return currentStatus;
  }

  /**
   * Creates a copy of this CompactionCandidate object with the given status.
   */
  public CompactionCandidate withCurrentStatus(CompactionStatus status)
  {
    return new CompactionCandidate(segments, umbrellaInterval, compactionInterval, numIntervals, status);
  }

  @Override
  public String toString()
  {
    return "SegmentsToCompact{" +
           "datasource=" + dataSource +
           ", segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", totalSize=" + totalBytes +
           ", currentStatus=" + currentStatus +
           '}';
  }
}
