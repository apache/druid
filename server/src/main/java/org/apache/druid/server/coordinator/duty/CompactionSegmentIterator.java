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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.server.coordinator.CompactionStatistics;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Segments in the lists which are the elements of this iterator are sorted according to the natural segment order
 * (see {@link DataSegment#compareTo}).
 */
public abstract class CompactionSegmentIterator<T> implements Iterator<T>
{
  private static final Logger log = new Logger(CompactionSegmentIterator.class);
  final Map<String, CompactionStatistics> compactedSegments = new HashMap<>();
  final Map<String, CompactionStatistics> skippedSegments = new HashMap<>();

  private final ObjectMapper objectMapper;

  public CompactionSegmentIterator(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  /**
   * Return a map of dataSourceName to CompactionStatistics.
   * This method returns the aggregated statistics of segments that was already compacted and does not need to be compacted
   * again. Hence, segment that were not returned by the {@link Iterator#next()} becuase it does not needs compaction.
   * Note that the aggregations returned by this method is only up to the current point of the iterator being iterated.
   */
  Map<String, CompactionStatistics> totalCompactedStatistics()
  {
    return compactedSegments;
  }

  /**
   * Return a map of dataSourceName to CompactionStatistics.
   * This method returns the aggregated statistics of segments that was skipped as it cannot be compacted.
   * Hence, segment that were not returned by the {@link Iterator#next()} becuase it cannot be compacted.
   * Note that the aggregations returned by this method is only up to the current point of the iterator being iterated.
   */
  Map<String, CompactionStatistics> totalSkippedStatistics()
  {
    return skippedSegments;
  }

  void collectSegmentStatistics(
      Map<String, CompactionStatistics> statisticsMap,
      String dataSourceName,
      SegmentsToCompact segments
  )
  {
    CompactionStatistics statistics = statisticsMap.computeIfAbsent(
        dataSourceName,
        v -> CompactionStatistics.initializeCompactionStatistics()
    );
    statistics.incrementCompactedByte(segments.getTotalSize());
    statistics.incrementCompactedIntervals(segments.getNumberOfIntervals());
    statistics.incrementCompactedSegments(segments.getNumberOfSegments());
  }

  /**
   * compaction iterator reset
   */
  abstract void reset(Map<String, List<Interval>> skipIntervals, IndexingServiceClient indexingServiceClient);

  /**
   * compaction iterator skip elements,
   * skip latest compaction task failure count
   *
   * @param dataSource
   * @param skipCount
   */
  abstract void skip(String dataSource, int skipCount);

  void setCompactType(boolean isMajor)
  {
  }

  /**
   * Need compaction common conditions
   *
   * @param tuningConfig
   * @param candidates
   *
   * @return
   */
  boolean needsCompaction(ClientCompactionTaskQueryTuningConfig tuningConfig, SegmentsToCompact candidates)
  {
    Preconditions.checkState(!candidates.isEmpty(), "Empty candidates");

    final PartitionsSpec partitionsSpecFromConfig = findPartitinosSpecFromConfig(tuningConfig);
    final CompactionState lastCompactionState = candidates.segments.get(0).getLastCompactionState();
    if (lastCompactionState == null) {
      log.info("Candidate segment[%s] is not compacted yet. Needs compaction.", candidates.segments.get(0).getId());
      return true;
    }

    final boolean allCandidatesHaveSameLastCompactionState = candidates
        .segments
        .stream()
        .allMatch(segment -> lastCompactionState.equals(segment.getLastCompactionState()));

    if (!allCandidatesHaveSameLastCompactionState) {
      log.info(
          "[%s] Candidate segments were compacted with different partitions spec. Needs compaction.",
          candidates.segments.size()
      );
      log.debugSegments(
          candidates.segments,
          "Candidate segments compacted with different partiton spec"
      );

      return true;
    }

    final PartitionsSpec segmentPartitionsSpec = lastCompactionState.getPartitionsSpec();
    final IndexSpec segmentIndexSpec = objectMapper.convertValue(lastCompactionState.getIndexSpec(), IndexSpec.class);
    final IndexSpec configuredIndexSpec;
    if (tuningConfig.getIndexSpec() == null) {
      configuredIndexSpec = new IndexSpec();
    } else {
      configuredIndexSpec = tuningConfig.getIndexSpec();
    }
    boolean needsCompaction = false;
    if (!Objects.equals(partitionsSpecFromConfig, segmentPartitionsSpec)) {
      log.info(
          "Configured partitionsSpec[%s] is differenet from "
          + "the partitionsSpec[%s] of segments. Needs compaction.",
          partitionsSpecFromConfig,
          segmentPartitionsSpec
      );
      needsCompaction = true;
    }
    // segmentIndexSpec cannot be null.
    if (!segmentIndexSpec.equals(configuredIndexSpec)) {
      log.info(
          "Configured indexSpec[%s] is different from the one[%s] of segments. Needs compaction",
          configuredIndexSpec,
          segmentIndexSpec
      );
      needsCompaction = true;
    }

    return needsCompaction;
  }

  @VisibleForTesting
  static PartitionsSpec findPartitinosSpecFromConfig(ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    final PartitionsSpec partitionsSpecFromTuningConfig = tuningConfig.getPartitionsSpec();
    if (partitionsSpecFromTuningConfig instanceof DynamicPartitionsSpec) {
      return new DynamicPartitionsSpec(
          partitionsSpecFromTuningConfig.getMaxRowsPerSegment(),
          ((DynamicPartitionsSpec) partitionsSpecFromTuningConfig).getMaxTotalRowsOr(Long.MAX_VALUE)
      );
    } else {
      final long maxTotalRows = tuningConfig.getMaxTotalRows() != null
                                ? tuningConfig.getMaxTotalRows()
                                : Long.MAX_VALUE;
      return partitionsSpecFromTuningConfig == null
             ? new DynamicPartitionsSpec(tuningConfig.getMaxRowsPerSegment(), maxTotalRows)
             : partitionsSpecFromTuningConfig;
    }
  }

  static class SegmentsToCompact
  {
    final List<DataSegment> segments;
    final long totalSize;

    public SegmentsToCompact()
    {
      this(Collections.emptyList());
    }

    public SegmentsToCompact(List<DataSegment> segments)
    {
      this.segments = segments;
      this.totalSize = segments.stream().mapToLong(DataSegment::getSize).sum();
    }

    public boolean isEmpty()
    {
      return segments.isEmpty();
    }

    public long getTotalSize()
    {
      return totalSize;
    }

    public long getNumberOfSegments()
    {
      return segments.size();
    }

    public long getNumberOfIntervals()
    {
      return segments.stream().map(DataSegment::getInterval).distinct().count();
    }

    @Override
    public String toString()
    {
      return "SegmentsToCompact{" +
             "segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
