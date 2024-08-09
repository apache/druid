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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.NumberedPartitionChunk;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.utils.CollectionUtils;
import org.apache.druid.utils.Streams;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Iterator over compactible segments of a datasource in order of specified priority.
 */
public class DataSourceCompactibleSegmentIterator implements CompactionSegmentIterator
{
  private static final Logger log = new Logger(DataSourceCompactibleSegmentIterator.class);

  private final String dataSource;
  private final DataSourceCompactionConfig config;
  private final CompactionStatusTracker statusTracker;
  private final PriorityBasedSegmentSearchPolicy searchPolicy;

  private final Granularity configuredSegmentGranularity;

  private final List<SegmentsToCompact> compactedSegments = new ArrayList<>();
  private final List<SegmentsToCompact> skippedSegments = new ArrayList<>();

  // This is needed for datasource that has segmentGranularity configured
  // If configured segmentGranularity in config is finer than current segmentGranularity, the same set of segments
  // can belong to multiple intervals in the timeline. We keep track of the compacted intervals between each
  // run of the compaction job and skip any interval that was already previously compacted.
  private final Set<Interval> queuedIntervals = new HashSet<>();

  private final PriorityQueue<SegmentsToCompact> queue;

  public DataSourceCompactibleSegmentIterator(
      DataSourceCompactionConfig config,
      SegmentTimeline timeline,
      List<Interval> skipIntervals,
      PriorityBasedSegmentSearchPolicy searchPolicy,
      CompactionStatusTracker statusTracker
  )
  {
    this.statusTracker = statusTracker;
    this.config = config;
    this.dataSource = config.getDataSource();
    this.searchPolicy = searchPolicy;
    this.queue = new PriorityQueue<>(searchPolicy.getSegmentComparator());

    if (config.getGranularitySpec() == null || config.getGranularitySpec().getSegmentGranularity() == null) {
      this.configuredSegmentGranularity = null;
    } else {
      this.configuredSegmentGranularity = config.getGranularitySpec().getSegmentGranularity();
    }

    populateQueue(timeline, skipIntervals);
  }

  private void populateQueue(SegmentTimeline timeline, List<Interval> skipIntervals)
  {
    if (timeline != null) {
      if (!timeline.isEmpty()) {
        SegmentTimeline originalTimeline = null;
        if (configuredSegmentGranularity != null) {
          final Set<DataSegment> segments = timeline.findNonOvershadowedObjectsInInterval(
              Intervals.ETERNITY,
              Partitions.ONLY_COMPLETE
          );

          // Skip compaction if any segment has partial-eternity interval
          // See https://github.com/apache/druid/issues/13208
          final List<DataSegment> partialEternitySegments = new ArrayList<>();
          for (DataSegment segment : segments) {
            if (Intervals.ETERNITY.getStart().equals(segment.getInterval().getStart())
                || Intervals.ETERNITY.getEnd().equals(segment.getInterval().getEnd())) {
              partialEternitySegments.add(segment);
            }
          }
          if (!partialEternitySegments.isEmpty()) {
            SegmentsToCompact candidatesWithStatus = SegmentsToCompact.from(partialEternitySegments).withStatus(
                CompactionStatus.skipped("Segments have partial-eternity intervals")
            );
            skippedSegments.add(candidatesWithStatus);
            statusTracker.onCompactionStatusComputed(candidatesWithStatus, config);
            return;
          }

          // Convert original segmentGranularity to new granularities bucket by configuredSegmentGranularity
          // For example, if the original is interval of 2020-01-28/2020-02-03 with WEEK granularity
          // and the configuredSegmentGranularity is MONTH, the segment will be split to two segments
          // of 2020-01/2020-02 and 2020-02/2020-03.
          final SegmentTimeline timelineWithConfiguredSegmentGranularity = new SegmentTimeline();
          final Map<Interval, Set<DataSegment>> intervalToPartitionMap = new HashMap<>();
          for (DataSegment segment : segments) {
            for (Interval interval : configuredSegmentGranularity.getIterable(segment.getInterval())) {
              intervalToPartitionMap.computeIfAbsent(interval, k -> new HashSet<>())
                                    .add(segment);
            }
          }

          final String temporaryVersion = DateTimes.nowUtc().toString();
          for (Map.Entry<Interval, Set<DataSegment>> partitionsPerInterval : intervalToPartitionMap.entrySet()) {
            Interval interval = partitionsPerInterval.getKey();
            int partitionNum = 0;
            Set<DataSegment> segmentSet = partitionsPerInterval.getValue();
            int partitions = segmentSet.size();
            for (DataSegment segment : segmentSet) {
              DataSegment segmentsForCompact = segment.withShardSpec(new NumberedShardSpec(partitionNum, partitions));
              timelineWithConfiguredSegmentGranularity.add(
                  interval,
                  temporaryVersion,
                  NumberedPartitionChunk.make(partitionNum, partitions, segmentsForCompact)
              );
              partitionNum += 1;
            }
          }
          // PartitionHolder can only holds chunks of one partition space
          // However, partition in the new timeline (timelineWithConfiguredSegmentGranularity) can be hold multiple
          // partitions of the original timeline (when the new segmentGranularity is larger than the original
          // segmentGranularity). Hence, we group all the segments of the original timeline into intervals bucket
          // by the new configuredSegmentGranularity. We then convert each segment into a new partition space so that
          // there is no duplicate partitionNum across all segments of each new Interval.
          // Similarly, segment versions may be mixed in the same time chunk based on new segment granularity
          // Hence we create the new timeline with a temporary version, setting the fake version to all be the same
          // for the same new time bucket.
          // We need to save and store the originalTimeline so that we can use it
          // to get the original ShardSpec and original version back (when converting the segment back to return from this iterator).
          originalTimeline = timeline;
          timeline = timelineWithConfiguredSegmentGranularity;
        }
        final List<Interval> searchIntervals = findInitialSearchInterval(timeline, skipIntervals);
        if (!searchIntervals.isEmpty()) {
          findAndEnqueueSegmentsToCompact(
              new CompactibleSegmentIterator(timeline, searchIntervals, originalTimeline)
          );
        } else {
          log.warn("Skipping compaction for datasource[%s] as it has no compactible segments.", dataSource);
        }
      }
    }
  }

  @Override
  public List<SegmentsToCompact> getCompactedSegments()
  {
    return compactedSegments;
  }

  @Override
  public List<SegmentsToCompact> getSkippedSegments()
  {
    return skippedSegments;
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public SegmentsToCompact next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final SegmentsToCompact entry = queue.poll();
    if (entry == null) {
      throw new NoSuchElementException();
    }

    final List<DataSegment> resultSegments = entry.getSegments();
    Preconditions.checkState(!resultSegments.isEmpty(), "Queue entry must not be empty");

    return entry;
  }

  /**
   * Iterates compactible segments in a {@link SegmentTimeline}.
   */
  private static class CompactibleSegmentIterator implements Iterator<List<DataSegment>>
  {
    private final List<TimelineObjectHolder<String, DataSegment>> holders;
    @Nullable
    private final SegmentTimeline originalTimeline;

    CompactibleSegmentIterator(
        SegmentTimeline timeline,
        List<Interval> totalIntervalsToSearch,
        // originalTimeline can be null if timeline was not modified
        @Nullable SegmentTimeline originalTimeline
    )
    {
      this.holders = totalIntervalsToSearch.stream().flatMap(
          interval -> timeline
              .lookup(interval)
              .stream()
              .filter(holder -> isCompactibleHolder(interval, holder))
      ).collect(Collectors.toList());
      this.originalTimeline = originalTimeline;
    }

    /**
     * Checks if the {@link TimelineObjectHolder} satisfies the following:
     * <ul>
     * <li>It has atleast one segment.</li>
     * <li>The interval of the segments is contained in the searchInterval.</li>
     * <li>The total bytes across all the segments is positive.</li>
     * </ul>
     */
    private boolean isCompactibleHolder(Interval searchInterval, TimelineObjectHolder<String, DataSegment> holder)
    {
      final Iterator<PartitionChunk<DataSegment>> chunks = holder.getObject().iterator();
      if (!chunks.hasNext()) {
        return false;
      }
      PartitionChunk<DataSegment> firstChunk = chunks.next();
      if (!searchInterval.contains(firstChunk.getObject().getInterval())) {
        return false;
      }
      long partitionBytes = firstChunk.getObject().getSize();
      while (partitionBytes == 0 && chunks.hasNext()) {
        partitionBytes += chunks.next().getObject().getSize();
      }
      return partitionBytes > 0;
    }

    @Override
    public boolean hasNext()
    {
      return !holders.isEmpty();
    }

    /**
     * Returns the next list of compactible segments in the datasource timeline.
     * The returned list satisfies the following conditions:
     * <ul>
     * <li>The list is non-null and non-empty.</li>
     * <li>The segments are present in the search interval.</li>
     * <li>Total bytes of segments in the list is greater than zero.</li>
     * </ul>
     */
    @Override
    public List<DataSegment> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      TimelineObjectHolder<String, DataSegment> timelineObjectHolder = holders.remove(holders.size() - 1);
      List<DataSegment> candidates = Streams.sequentialStreamFrom(timelineObjectHolder.getObject())
                                            .map(PartitionChunk::getObject)
                                            .collect(Collectors.toList());

      if (originalTimeline == null) {
        return candidates;
      } else {
        Interval umbrellaInterval = JodaUtils.umbrellaInterval(
            candidates.stream().map(DataSegment::getInterval).collect(Collectors.toList())
        );
        return Lists.newArrayList(
            originalTimeline.findNonOvershadowedObjectsInInterval(umbrellaInterval, Partitions.ONLY_COMPLETE)
        );
      }
    }
  }

  /**
   * Finds segments to compact together for the given datasource and adds them to
   * the priority queue.
   */
  private void findAndEnqueueSegmentsToCompact(CompactibleSegmentIterator compactibleSegmentIterator)
  {
    while (compactibleSegmentIterator.hasNext()) {
      List<DataSegment> segments = compactibleSegmentIterator.next();
      if (CollectionUtils.isNullOrEmpty(segments)) {
        continue;
      }

      // Do not compact an interval which comprises of a single tombstone
      // If there are multiple tombstones in the interval, we may still want to compact them
      if (segments.size() == 1 && segments.get(0).isTombstone()) {
        continue;
      }

      final SegmentsToCompact candidates = SegmentsToCompact.from(segments);
      final CompactionStatus compactionStatus = computeCompactionStatus(candidates);
      final SegmentsToCompact candidatesWithStatus = candidates.withStatus(compactionStatus);
      statusTracker.onCompactionStatusComputed(candidatesWithStatus, config);

      if (compactionStatus.isComplete()) {
        compactedSegments.add(candidatesWithStatus);
      } else if (compactionStatus.isSkipped()) {
        skippedSegments.add(candidatesWithStatus);
      } else {
        queue.add(candidatesWithStatus);
        if (configuredSegmentGranularity != null) {
          queuedIntervals.add(candidates.getUmbrellaInterval());
        }
      }
    }
  }

  private CompactionStatus computeCompactionStatus(SegmentsToCompact candidate)
  {
    final CompactionStatus compactionStatus
        = CompactionStatus.compute(candidate, config, statusTracker.getObjectMapper());
    if (compactionStatus.isComplete()) {
      return compactionStatus;
    }

    // Skip intervals that violate max allowed input segment size
    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    if (candidate.getTotalBytes() > inputSegmentSize) {
      return CompactionStatus.skipped(
          "'inputSegmentSize' exceeded: Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
          candidate.getTotalBytes(), inputSegmentSize
      );
    }

    // Skip intervals that are already queued
    if (configuredSegmentGranularity != null
        && queuedIntervals.contains(candidate.getUmbrellaInterval())) {
      return CompactionStatus.skipped("Interval is already queued");
    }

    // Skip intervals that already have a running task
    final CompactionTaskStatus lastTaskStatus = statusTracker.getLatestTaskStatus(candidate);
    if (lastTaskStatus != null && lastTaskStatus.getState() == TaskState.RUNNING) {
      return CompactionStatus.skipped("Task for interval is already running");
    }

    // Skip intervals that have been filtered out by the policy
    if (searchPolicy.shouldSkipCompaction(candidate, compactionStatus, lastTaskStatus)) {
      return CompactionStatus.skipped("Skipped by policy");
    }

    return compactionStatus;
  }

  /**
   * Returns the initial searchInterval which is {@code (timeline.first().start, timeline.last().end - skipOffset)}.
   */
  private List<Interval> findInitialSearchInterval(
      SegmentTimeline timeline,
      @Nullable List<Interval> skipIntervals
  )
  {
    final Period skipOffset = config.getSkipOffsetFromLatest();
    Preconditions.checkArgument(timeline != null && !timeline.isEmpty(), "timeline should not be null or empty");
    Preconditions.checkNotNull(skipOffset, "skipOffset");

    final TimelineObjectHolder<String, DataSegment> first = Preconditions.checkNotNull(timeline.first(), "first");
    final TimelineObjectHolder<String, DataSegment> last = Preconditions.checkNotNull(timeline.last(), "last");
    final Interval latestSkipInterval = computeLatestSkipInterval(
        configuredSegmentGranularity,
        last.getInterval().getEnd(),
        skipOffset
    );
    final List<Interval> allSkipIntervals
        = sortAndAddSkipIntervalFromLatest(latestSkipInterval, skipIntervals);

    // Collect stats for all skipped segments
    for (Interval skipInterval : allSkipIntervals) {
      final List<DataSegment> segments = new ArrayList<>(
          timeline.findNonOvershadowedObjectsInInterval(skipInterval, Partitions.ONLY_COMPLETE)
      );
      if (!CollectionUtils.isNullOrEmpty(segments)) {
        final SegmentsToCompact candidates = SegmentsToCompact.from(segments);

        final CompactionStatus reason;
        if (candidates.getUmbrellaInterval().overlaps(latestSkipInterval)) {
          reason = CompactionStatus.skipped("skip offset from latest[%s]", skipOffset);
        } else {
          reason = CompactionStatus.skipped("interval locked by another task");
        }

        final SegmentsToCompact candidatesWithStatus = candidates.withStatus(reason);
        skippedSegments.add(candidatesWithStatus);
        statusTracker.onCompactionStatusComputed(candidatesWithStatus, config);
      }
    }

    final Interval totalInterval = new Interval(first.getInterval().getStart(), last.getInterval().getEnd());
    final List<Interval> filteredInterval = filterSkipIntervals(totalInterval, allSkipIntervals);
    final List<Interval> searchIntervals = new ArrayList<>();

    for (Interval lookupInterval : filteredInterval) {
      if (Intervals.ETERNITY.getStart().equals(lookupInterval.getStart())
          || Intervals.ETERNITY.getEnd().equals(lookupInterval.getEnd())) {
        log.warn(
            "Cannot compact datasource[%s] since interval[%s] coincides with ETERNITY.",
            dataSource, lookupInterval
        );
        return Collections.emptyList();
      }
      final List<DataSegment> segments = timeline
          .findNonOvershadowedObjectsInInterval(lookupInterval, Partitions.ONLY_COMPLETE)
          .stream()
          // findNonOvershadowedObjectsInInterval() may return segments merely intersecting with lookupInterval, while
          // we are interested only in segments fully lying within lookupInterval here.
          .filter(segment -> lookupInterval.contains(segment.getInterval()))
          .collect(Collectors.toList());

      if (segments.isEmpty()) {
        continue;
      }

      DateTime searchStart = segments
          .stream()
          .map(segment -> segment.getId().getIntervalStart())
          .min(Comparator.naturalOrder())
          .orElseThrow(AssertionError::new);
      DateTime searchEnd = segments
          .stream()
          .map(segment -> segment.getId().getIntervalEnd())
          .max(Comparator.naturalOrder())
          .orElseThrow(AssertionError::new);
      searchIntervals.add(new Interval(searchStart, searchEnd));
    }

    return searchIntervals;
  }

  static Interval computeLatestSkipInterval(
      @Nullable Granularity configuredSegmentGranularity,
      DateTime latestDataTimestamp,
      Period skipOffsetFromLatest
  )
  {
    if (configuredSegmentGranularity == null) {
      return new Interval(skipOffsetFromLatest, latestDataTimestamp);
    } else {
      DateTime skipFromLastest = new DateTime(latestDataTimestamp, latestDataTimestamp.getZone()).minus(skipOffsetFromLatest);
      DateTime skipOffsetBucketToSegmentGranularity = configuredSegmentGranularity.bucketStart(skipFromLastest);
      return new Interval(skipOffsetBucketToSegmentGranularity, latestDataTimestamp);
    }
  }

  @VisibleForTesting
  static List<Interval> sortAndAddSkipIntervalFromLatest(
      Interval skipFromLatest,
      @Nullable List<Interval> skipIntervals
  )
  {
    final List<Interval> nonNullSkipIntervals = skipIntervals == null
                                                ? new ArrayList<>(1)
                                                : new ArrayList<>(skipIntervals.size());

    if (skipIntervals != null) {
      final List<Interval> sortedSkipIntervals = new ArrayList<>(skipIntervals);
      sortedSkipIntervals.sort(Comparators.intervalsByStartThenEnd());

      final List<Interval> overlapIntervals = new ArrayList<>();

      for (Interval interval : sortedSkipIntervals) {
        if (interval.overlaps(skipFromLatest)) {
          overlapIntervals.add(interval);
        } else {
          nonNullSkipIntervals.add(interval);
        }
      }

      if (!overlapIntervals.isEmpty()) {
        overlapIntervals.add(skipFromLatest);
        nonNullSkipIntervals.add(JodaUtils.umbrellaInterval(overlapIntervals));
      } else {
        nonNullSkipIntervals.add(skipFromLatest);
      }
    } else {
      nonNullSkipIntervals.add(skipFromLatest);
    }

    return nonNullSkipIntervals;
  }

  /**
   * Returns a list of intervals which are contained by totalInterval but don't ovarlap with skipIntervals.
   *
   * @param totalInterval total interval
   * @param skipIntervals intervals to skip. This should be sorted by {@link Comparators#intervalsByStartThenEnd()}.
   */
  @VisibleForTesting
  static List<Interval> filterSkipIntervals(Interval totalInterval, List<Interval> skipIntervals)
  {
    final List<Interval> filteredIntervals = new ArrayList<>(skipIntervals.size() + 1);

    DateTime remainingStart = totalInterval.getStart();
    DateTime remainingEnd = totalInterval.getEnd();
    for (Interval skipInterval : skipIntervals) {
      if (skipInterval.getStart().isBefore(remainingStart) && skipInterval.getEnd().isAfter(remainingStart)) {
        remainingStart = skipInterval.getEnd();
      } else if (skipInterval.getStart().isBefore(remainingEnd) && skipInterval.getEnd().isAfter(remainingEnd)) {
        remainingEnd = skipInterval.getStart();
      } else if (!remainingStart.isAfter(skipInterval.getStart()) && !remainingEnd.isBefore(skipInterval.getEnd())) {
        filteredIntervals.add(new Interval(remainingStart, skipInterval.getStart()));
        remainingStart = skipInterval.getEnd();
      } else {
        // Ignore this skipInterval
        log.warn(
            "skipInterval[%s] is not contained in remainingInterval[%s]",
            skipInterval,
            new Interval(remainingStart, remainingEnd)
        );
      }
    }

    if (!remainingStart.equals(remainingEnd)) {
      filteredIntervals.add(new Interval(remainingStart, remainingEnd));
    }

    return filteredIntervals;
  }

}
