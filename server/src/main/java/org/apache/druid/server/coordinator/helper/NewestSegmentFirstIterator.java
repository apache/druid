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

package org.apache.druid.server.coordinator.helper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class iterates all segments of the dataSources configured for compaction from the newest to the oldest.
 */
public class NewestSegmentFirstIterator implements CompactionSegmentIterator
{
  private static final Logger log = new Logger(NewestSegmentFirstIterator.class);

  private final Map<String, DataSourceCompactionConfig> compactionConfigs;
  private final Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;

  // dataSource -> intervalToFind
  // searchIntervals keeps track of the current state of which interval should be considered to search segments to
  // compact.
  private final Map<String, CompactibleTimelineObjectHolderCursor> timelineIterators;

  private final PriorityQueue<QueueEntry> queue = new PriorityQueue<>(
      (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o2.interval, o1.interval)
  );

  NewestSegmentFirstIterator(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources,
      Map<String, List<Interval>> skipIntervals
  )
  {
    this.compactionConfigs = compactionConfigs;
    this.dataSources = dataSources;
    this.timelineIterators = new HashMap<>(dataSources.size());

    dataSources.forEach((String dataSource, VersionedIntervalTimeline<String, DataSegment> timeline) -> {
      final DataSourceCompactionConfig config = compactionConfigs.get(dataSource);

      if (config != null && !timeline.isEmpty()) {
        final List<Interval> searchIntervals =
            findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest(), skipIntervals.get(dataSource));
        if (!searchIntervals.isEmpty()) {
          timelineIterators.put(dataSource, new CompactibleTimelineObjectHolderCursor(timeline, searchIntervals));
        }
      }
    });

    compactionConfigs.forEach((String dataSourceName, DataSourceCompactionConfig config) -> {
      if (config == null) {
        throw new ISE("Unknown dataSource[%s]", dataSourceName);
      }
      updateQueue(dataSourceName, config);
    });
  }

  @Override
  public Object2LongOpenHashMap<String> remainingSegmentSizeBytes()
  {
    final Object2LongOpenHashMap<String> resultMap = new Object2LongOpenHashMap<>();
    resultMap.defaultReturnValue(UNKNOWN_REMAINING_SEGMENT_SIZE);
    for (QueueEntry entry : queue) {
      final VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(entry.getDataSource());
      final Interval interval = new Interval(timeline.first().getInterval().getStart(), entry.interval.getEnd());

      final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(interval);

      resultMap.put(
          entry.getDataSource(),
          holders.stream()
                 .flatMap(holder -> StreamSupport.stream(holder.getObject().spliterator(), false))
                 .mapToLong(chunk -> chunk.getObject().getSize())
                 .sum()
      );
    }
    return resultMap;
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public List<DataSegment> next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final QueueEntry entry = queue.poll();

    if (entry == null) {
      throw new NoSuchElementException();
    }

    final List<DataSegment> resultSegments = entry.segments;

    Preconditions.checkState(!resultSegments.isEmpty(), "Queue entry must not be empty");

    final String dataSource = resultSegments.get(0).getDataSource();
    updateQueue(dataSource, compactionConfigs.get(dataSource));

    return resultSegments;
  }

  /**
   * Find the next segments to compact for the given dataSource and add them to the queue.
   * {@link #timelineIterators} is updated according to the found segments. That is, the found segments are removed from
   * the timeline of the given dataSource.
   */
  private void updateQueue(String dataSourceName, DataSourceCompactionConfig config)
  {
    final CompactibleTimelineObjectHolderCursor compactibleTimelineObjectHolderCursor = timelineIterators.get(
        dataSourceName
    );

    if (compactibleTimelineObjectHolderCursor == null) {
      log.warn("Cannot find timeline for dataSource[%s]. Skip this dataSource", dataSourceName);
      return;
    }

    final SegmentsToCompact segmentsToCompact = findSegmentsToCompact(
        compactibleTimelineObjectHolderCursor,
        config
    );

    if (segmentsToCompact.getNumSegments() > 1) {
      queue.add(new QueueEntry(segmentsToCompact.segments));
    }
  }

  /**
   * Iterates the given {@link VersionedIntervalTimeline}. Only compactible {@link TimelineObjectHolder}s are returned,
   * which means the holder always has at least two {@link DataSegment}s.
   */
  private static class CompactibleTimelineObjectHolderCursor implements Iterator<List<DataSegment>>
  {
    private final List<TimelineObjectHolder<String, DataSegment>> holders;

    CompactibleTimelineObjectHolderCursor(
        VersionedIntervalTimeline<String, DataSegment> timeline,
        List<Interval> totalIntervalsToSearch
    )
    {
      this.holders = totalIntervalsToSearch
          .stream()
          .flatMap(interval -> timeline
              .lookup(interval)
              .stream()
              .filter(holder -> isCompactibleHolder(interval, holder))
          )
          .collect(Collectors.toList());
    }

    private boolean isCompactibleHolder(Interval interval, TimelineObjectHolder<String, DataSegment> holder)
    {
      final Iterator<PartitionChunk<DataSegment>> chunks = holder.getObject().iterator();
      if (!chunks.hasNext()) {
        return false;
      }
      PartitionChunk<DataSegment> firstChunk = chunks.next();
      if (!interval.contains(firstChunk.getObject().getInterval())) {
        return false;
      }
      long partitionBytes = firstChunk.getObject().getSize();
      if (!chunks.hasNext()) {
        return false; // There should be at least two chunks for a holder to be compactible.
      }
      while (chunks.hasNext()) {
        partitionBytes += chunks.next().getObject().getSize();
        if (partitionBytes > 0) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean hasNext()
    {
      return !holders.isEmpty();
    }

    @Override
    public List<DataSegment> next()
    {
      if (holders.isEmpty()) {
        throw new NoSuchElementException();
      }
      return holders.remove(holders.size() - 1)
                    .getObject()
                    .stream()
                    .map(PartitionChunk::getObject)
                    .collect(Collectors.toList());
    }
  }

  /**
   * Find segments to compact together for the given intervalToSearch. It progressively searches the given
   * intervalToSearch in time order (latest first). The timeline lookup duration is one day. It means, the timeline is
   * looked up for the last one day of the given intervalToSearch, and the next day is searched again if the size of
   * found segments are not enough to compact. This is repeated until enough amount of segments are found.
   *
   * @param compactibleTimelineObjectHolderCursor timeline iterator
   * @param config           compaction config
   *
   * @return segments to compact
   */
  private static SegmentsToCompact findSegmentsToCompact(
      final CompactibleTimelineObjectHolderCursor compactibleTimelineObjectHolderCursor,
      final DataSourceCompactionConfig config
  )
  {
    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    final @Nullable Long targetCompactionSizeBytes = config.getTargetCompactionSizeBytes();
    final int maxNumSegmentsToCompact = config.getMaxNumSegmentsToCompact();

    // Finds segments to compact together while iterating timeline from latest to oldest
    while (compactibleTimelineObjectHolderCursor.hasNext()) {
      final SegmentsToCompact candidates = new SegmentsToCompact(compactibleTimelineObjectHolderCursor.next());
      final boolean isCompactibleSize = candidates.getTotalSize() <= inputSegmentSize;
      final boolean isCompactibleNum = candidates.getNumSegments() <= maxNumSegmentsToCompact;
      final boolean needsCompaction = SegmentCompactorUtil.needsCompaction(
          targetCompactionSizeBytes,
          candidates.segments
      );

      if (isCompactibleSize && isCompactibleNum && needsCompaction) {
        return candidates;
      } else {
        if (!isCompactibleSize) {
          log.warn(
              "total segment size[%d] for datasource[%s] and interval[%s] is larger than inputSegmentSize[%d]."
              + " Continue to the next interval.",
              candidates.getTotalSize(),
              candidates.segments.get(0).getDataSource(),
              candidates.segments.get(0).getInterval(),
              inputSegmentSize
          );
        }
        if (!isCompactibleNum) {
          log.warn(
              "Number of segments[%d] for datasource[%s] and interval[%s] is larger than "
              + "maxNumSegmentsToCompact[%d]. If you see lots of shards are being skipped due to too many "
              + "segments, consider increasing 'numTargetCompactionSegments' and "
              + "'druid.indexer.runner.maxZnodeBytes'. Continue to the next interval.",
              candidates.getNumSegments(),
              candidates.segments.get(0).getDataSource(),
              candidates.segments.get(0).getInterval(),
              maxNumSegmentsToCompact
          );
        }
        if (!needsCompaction) {
          log.warn(
              "Size of most of segments[%s] is larger than targetCompactionSizeBytes[%s] "
              + "for datasource[%s] and interval[%s]. Skipping compaction for this interval.",
              candidates.segments.stream().map(DataSegment::getSize).collect(Collectors.toList()),
              targetCompactionSizeBytes,
              candidates.segments.get(0).getDataSource(),
              candidates.segments.get(0).getInterval()
          );
        }
      }
    }

    // Return an empty set if nothing is found
    return new SegmentsToCompact();
  }

  /**
   * Returns the initial searchInterval which is {@code (timeline.first().start, timeline.last().end - skipOffset)}.
   *
   * @param timeline      timeline of a dataSource
   * @param skipIntervals intervals to skip
   *
   * @return found interval to search or null if it's not found
   */
  private static List<Interval> findInitialSearchInterval(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Period skipOffset,
      @Nullable List<Interval> skipIntervals
  )
  {
    Preconditions.checkArgument(timeline != null && !timeline.isEmpty(), "timeline should not be null or empty");
    Preconditions.checkNotNull(skipOffset, "skipOffset");

    final TimelineObjectHolder<String, DataSegment> first = Preconditions.checkNotNull(timeline.first(), "first");
    final TimelineObjectHolder<String, DataSegment> last = Preconditions.checkNotNull(timeline.last(), "last");
    final List<Interval> fullSkipIntervals = sortAndAddSkipIntervalFromLatest(
        last.getInterval().getEnd(),
        skipOffset,
        skipIntervals
    );

    final Interval totalInterval = new Interval(first.getInterval().getStart(), last.getInterval().getEnd());
    final List<Interval> filteredInterval = filterSkipIntervals(totalInterval, fullSkipIntervals);
    final List<Interval> searchIntervals = new ArrayList<>();

    for (Interval lookupInterval : filteredInterval) {
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

  @VisibleForTesting
  static List<Interval> sortAndAddSkipIntervalFromLatest(
      DateTime latest,
      Period skipOffset,
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
      final Interval skipFromLatest = new Interval(skipOffset, latest);

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
      final Interval skipFromLatest = new Interval(skipOffset, latest);
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

  private static class QueueEntry
  {
    private final Interval interval; // whole interval for all segments
    private final List<DataSegment> segments;

    private QueueEntry(List<DataSegment> segments)
    {
      Preconditions.checkArgument(segments != null && !segments.isEmpty());
      Collections.sort(segments);
      this.interval = new Interval(
          segments.get(0).getInterval().getStart(),
          segments.get(segments.size() - 1).getInterval().getEnd()
      );
      this.segments = segments;
    }

    private String getDataSource()
    {
      return segments.get(0).getDataSource();
    }
  }

  private static class SegmentsToCompact
  {
    private final List<DataSegment> segments;
    private final long totalSize;

    private SegmentsToCompact()
    {
      this(Collections.emptyList());
    }

    private SegmentsToCompact(List<DataSegment> segments)
    {
      this.segments = segments;
      this.totalSize = segments.stream().mapToLong(DataSegment::getSize).sum();
    }

    private int getNumSegments()
    {
      return segments.size();
    }

    private long getTotalSize()
    {
      return totalSize;
    }

    @Override
    public String toString()
    {
      return "SegmentsToCompact{" +
             "segments=" + segments +
             ", totalSize=" + totalSize +
             '}';
    }
  }
}
