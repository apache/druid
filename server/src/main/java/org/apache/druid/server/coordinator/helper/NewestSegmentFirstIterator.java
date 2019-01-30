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
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

    for (Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      final String dataSource = entry.getKey();
      final VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      final DataSourceCompactionConfig config = compactionConfigs.get(dataSource);

      if (config != null && !timeline.isEmpty()) {
        final List<Interval> searchIntervals = findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest(), skipIntervals.get(dataSource));
        if (!searchIntervals.isEmpty()) {
          timelineIterators.put(dataSource, new CompactibleTimelineObjectHolderCursor(timeline, searchIntervals));
        }
      }
    }

    for (Entry<String, DataSourceCompactionConfig> entry : compactionConfigs.entrySet()) {
      final String dataSourceName = entry.getKey();
      final DataSourceCompactionConfig config = entry.getValue();

      if (config == null) {
        throw new ISE("Unknown dataSource[%s]", dataSourceName);
      }

      updateQueue(dataSourceName, config);
    }
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
   * which means the holder always has at least one {@link DataSegment} and the total size of segments is larger than 0.
   */
  private static class CompactibleTimelineObjectHolderCursor
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
              .filter(holder -> {
                final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holder.getObject().iterator());
                final long partitionBytes = chunks.stream().mapToLong(chunk -> chunk.getObject().getSize()).sum();
                return chunks.size() > 0
                       && partitionBytes > 0
                       && interval.contains(chunks.get(0).getObject().getInterval());
              })
          )
          .collect(Collectors.toList());
    }

    boolean hasNext()
    {
      return !holders.isEmpty();
    }

    /**
     * Returns the latest holder.
     */
    @Nullable
    TimelineObjectHolder<String, DataSegment> get()
    {
      if (holders.isEmpty()) {
        return null;
      } else {
        return holders.get(holders.size() - 1);
      }
    }

    /**
     * Removes the latest holder, so that {@link #get()} returns the next one.
     */
    void next()
    {
      if (!holders.isEmpty()) {
        holders.remove(holders.size() - 1);
      }
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
    final boolean keepSegmentGranularity = config.isKeepSegmentGranularity();
    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    final int maxNumSegmentsToCompact = config.getMaxNumSegmentsToCompact();
    final SegmentsToCompact segmentsToCompact = new SegmentsToCompact();

    // Finds segments to compact together while iterating timeline from latest to oldest
    while (compactibleTimelineObjectHolderCursor.hasNext()
           && segmentsToCompact.getTotalSize() < inputSegmentSize
           && segmentsToCompact.getNumSegments() < maxNumSegmentsToCompact) {
      final TimelineObjectHolder<String, DataSegment> timeChunkHolder = Preconditions.checkNotNull(
          compactibleTimelineObjectHolderCursor.get(),
          "timelineObjectHolder"
      );
      final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(timeChunkHolder.getObject().iterator());
      final long timeChunkSizeBytes = chunks.stream().mapToLong(chunk -> chunk.getObject().getSize()).sum();

      final boolean isSameOrAbuttingInterval;
      final Interval lastInterval = segmentsToCompact.getIntervalOfLastSegment();
      if (lastInterval == null) {
        isSameOrAbuttingInterval = true;
      } else {
        final Interval currentInterval = chunks.get(0).getObject().getInterval();
        isSameOrAbuttingInterval = currentInterval.isEqual(lastInterval) || currentInterval.abuts(lastInterval);
      }

      // The segments in a holder should be added all together or not.
      final boolean isCompactibleSize = SegmentCompactorUtil.isCompactibleSize(
          inputSegmentSize,
          segmentsToCompact.getTotalSize(),
          timeChunkSizeBytes
      );
      final boolean isCompactibleNum = SegmentCompactorUtil.isCompactibleNum(
          maxNumSegmentsToCompact,
          segmentsToCompact.getNumSegments(),
          chunks.size()
      );
      if (isCompactibleSize
          && isCompactibleNum
          && isSameOrAbuttingInterval
          && (!keepSegmentGranularity || segmentsToCompact.isEmpty())) {
        chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
      } else {
        if (segmentsToCompact.getNumSegments() > 1) {
          // We found some segmens to compact and cannot add more. End here.
          return segmentsToCompact;
        } else {
          if (!SegmentCompactorUtil.isCompactibleSize(inputSegmentSize, 0, timeChunkSizeBytes)) {
            final DataSegment segment = chunks.get(0).getObject();
            segmentsToCompact.clear();
            log.warn(
                "shardSize[%d] for dataSource[%s] and interval[%s] is larger than inputSegmentSize[%d]."
                + " Continue to the next shard.",
                timeChunkSizeBytes,
                segment.getDataSource(),
                segment.getInterval(),
                inputSegmentSize
            );
          } else if (maxNumSegmentsToCompact < chunks.size()) {
            final DataSegment segment = chunks.get(0).getObject();
            segmentsToCompact.clear();
            log.warn(
                "The number of segments[%d] for dataSource[%s] and interval[%s] is larger than "
                + "maxNumSegmentsToCompact[%d]. If you see lots of shards are being skipped due to too many "
                + "segments, consider increasing 'numTargetCompactionSegments' and "
                + "'druid.indexer.runner.maxZnodeBytes'. Continue to the next shard.",
                chunks.size(),
                segment.getDataSource(),
                segment.getInterval(),
                maxNumSegmentsToCompact
            );
          } else {
            if (segmentsToCompact.getNumSegments() == 1) {
              // We found a segment which is smaller than targetCompactionSize but too large to compact with other
              // segments. Skip this one.
              segmentsToCompact.clear();
              chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
            } else {
              throw new ISE(
                  "Cannot compact segments[%s]. shardBytes[%s], numSegments[%s] "
                  + "with current segmentsToCompact[%s]",
                  chunks.stream().map(PartitionChunk::getObject).collect(Collectors.toList()),
                  timeChunkSizeBytes,
                  chunks.size(),
                  segmentsToCompact
              );
            }
          }
        }
      }

      compactibleTimelineObjectHolderCursor.next();
    }

    if (segmentsToCompact.getNumSegments() == 1) {
      // Don't compact a single segment
      segmentsToCompact.clear();
    }

    return segmentsToCompact;
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
      final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(
          new Interval(lookupInterval.getStart(), lookupInterval.getEnd())
      );

      final List<DataSegment> segments = holders
          .stream()
          .flatMap(holder -> StreamSupport.stream(holder.getObject().spliterator(), false))
          .map(PartitionChunk::getObject)
          .filter(segment -> lookupInterval.contains(segment.getInterval()))
          .sorted((s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval()))
          .collect(Collectors.toList());

      if (!segments.isEmpty()) {
        searchIntervals.add(
            new Interval(
                segments.get(0).getInterval().getStart(),
                segments.get(segments.size() - 1).getInterval().getEnd()
            )
        );
      }
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
    private final List<DataSegment> segments = new ArrayList<>();
    private long totalSize;

    private void add(DataSegment segment)
    {
      segments.add(segment);
      totalSize += segment.getSize();
    }

    private boolean isEmpty()
    {
      Preconditions.checkState((totalSize == 0) == segments.isEmpty());
      return segments.isEmpty();
    }

    @Nullable
    private Interval getIntervalOfLastSegment()
    {
      if (segments.isEmpty()) {
        return null;
      } else {
        return segments.get(segments.size() - 1).getInterval();
      }
    }

    private int getNumSegments()
    {
      return segments.size();
    }

    private long getTotalSize()
    {
      return totalSize;
    }

    private void clear()
    {
      segments.clear();
      totalSize = 0;
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
