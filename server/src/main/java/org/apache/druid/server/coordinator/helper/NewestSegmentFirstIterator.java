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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
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
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources
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
        final Interval searchInterval = findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest());
        timelineIterators.put(dataSource, new CompactibleTimelineObjectHolderCursor(timeline, searchInterval));
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

    if (segmentsToCompact.size() > 1) {
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
        Interval totalIntervalToSearch
    )
    {
      this.holders = timeline
          .lookup(totalIntervalToSearch)
          .stream()
          .filter(holder -> {
            final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holder.getObject().iterator());
            final long partitionBytes = chunks.stream().mapToLong(chunk -> chunk.getObject().getSize()).sum();
            return chunks.size() > 0
                   && partitionBytes > 0
                   && totalIntervalToSearch.contains(chunks.get(0).getObject().getInterval());
          })
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
    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    long totalSegmentsToCompactBytes = 0;

    // Finds segments to compact together while iterating timeline from latest to oldest
    while (compactibleTimelineObjectHolderCursor.hasNext()
           && totalSegmentsToCompactBytes < inputSegmentSize
           && segmentsToCompact.size() < maxNumSegmentsToCompact) {
      final TimelineObjectHolder<String, DataSegment> timeChunkHolder = Preconditions.checkNotNull(
          compactibleTimelineObjectHolderCursor.get(),
          "timelineObjectHolder"
      );
      final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(timeChunkHolder.getObject().iterator());
      final long timeChunkSizeBytes = chunks.stream().mapToLong(chunk -> chunk.getObject().getSize()).sum();

      // The segments in a holder should be added all together or not.
      if (SegmentCompactorUtil.isCompactibleSize(inputSegmentSize, totalSegmentsToCompactBytes, timeChunkSizeBytes)
          && SegmentCompactorUtil.isCompactibleNum(maxNumSegmentsToCompact, segmentsToCompact.size(), chunks.size())
          && (!keepSegmentGranularity || segmentsToCompact.size() == 0)) {
        chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
        totalSegmentsToCompactBytes += timeChunkSizeBytes;
      } else {
        if (segmentsToCompact.size() > 1) {
          // We found some segmens to compact and cannot add more. End here.
          return new SegmentsToCompact(segmentsToCompact);
        } else {
          // (*) Discard segments found so far because we can't compact them anyway.
          final int numSegmentsToCompact = segmentsToCompact.size();
          segmentsToCompact.clear();

          if (!SegmentCompactorUtil.isCompactibleSize(inputSegmentSize, 0, timeChunkSizeBytes)) {
            final DataSegment segment = chunks.get(0).getObject();
            log.warn(
                "shardSize[%d] for dataSource[%s] and interval[%s] is larger than inputSegmentSize[%d]."
                + " Contitnue to the next shard.",
                timeChunkSizeBytes,
                segment.getDataSource(),
                segment.getInterval(),
                inputSegmentSize
            );
          } else if (maxNumSegmentsToCompact < chunks.size()) {
            final DataSegment segment = chunks.get(0).getObject();
            log.warn(
                "The number of segments[%d] for dataSource[%s] and interval[%s] is larger than "
                + "numTargetCompactSegments[%d]. If you see lots of shards are being skipped due to too many "
                + "segments, consider increasing 'numTargetCompactionSegments' and "
                + "'druid.indexer.runner.maxZnodeBytes'. Contitnue to the next shard.",
                chunks.size(),
                segment.getDataSource(),
                segment.getInterval(),
                maxNumSegmentsToCompact
            );
          } else {
            if (numSegmentsToCompact == 1) {
              // We found a segment which is smaller than targetCompactionSize but too large to compact with other
              // segments. Skip this one.
              // Note that segmentsToCompact is already cleared at (*).
              chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
              totalSegmentsToCompactBytes = timeChunkSizeBytes;
            } else {
              throw new ISE(
                  "Cannot compact segments[%s]. shardBytes[%s], numSegments[%s]",
                  chunks.stream().map(PartitionChunk::getObject).collect(Collectors.toList()),
                  timeChunkSizeBytes,
                  chunks.size()
              );
            }
          }
        }
      }

      compactibleTimelineObjectHolderCursor.next();
    }

    if (segmentsToCompact.size() > 1) {
      return new SegmentsToCompact(segmentsToCompact);
    } else {
      return new SegmentsToCompact(Collections.emptyList());
    }
  }

  /**
   * Returns the initial searchInterval which is {@code (timeline.first().start, timeline.last().end - skipOffset)}.
   *
   * @param timeline   timeline of a dataSource
   * @param skipOffset skipOFfset
   *
   * @return found searchInterval
   */
  private static Interval findInitialSearchInterval(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Period skipOffset
  )
  {
    Preconditions.checkArgument(timeline != null && !timeline.isEmpty(), "timeline should not be null or empty");
    Preconditions.checkNotNull(skipOffset, "skipOffset");

    final TimelineObjectHolder<String, DataSegment> first = Preconditions.checkNotNull(timeline.first(), "first");
    final TimelineObjectHolder<String, DataSegment> last = Preconditions.checkNotNull(timeline.last(), "last");

    final Interval skipInterval = new Interval(skipOffset, last.getInterval().getEnd());

    final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(
        new Interval(first.getInterval().getStart(), last.getInterval().getEnd().minus(skipOffset))
    );

    final List<DataSegment> segments = holders
        .stream()
        .flatMap(holder -> StreamSupport.stream(holder.getObject().spliterator(), false))
        .map(PartitionChunk::getObject)
        .filter(segment -> !segment.getInterval().overlaps(skipInterval))
        .sorted((s1, s2) -> Comparators.intervalsByStartThenEnd().compare(s1.getInterval(), s2.getInterval()))
        .collect(Collectors.toList());

    if (segments.isEmpty()) {
      return new Interval(first.getInterval().getStart(), first.getInterval().getStart());
    } else {
      return new Interval(
          segments.get(0).getInterval().getStart(),
          segments.get(segments.size() - 1).getInterval().getEnd()
      );
    }
  }

  private static class QueueEntry
  {
    private final Interval interval; // whole interval for all segments
    private final List<DataSegment> segments;

    QueueEntry(List<DataSegment> segments)
    {
      Preconditions.checkArgument(segments != null && !segments.isEmpty());
      Collections.sort(segments);
      this.interval = new Interval(
          segments.get(0).getInterval().getStart(),
          segments.get(segments.size() - 1).getInterval().getEnd()
      );
      this.segments = segments;
    }

    String getDataSource()
    {
      return segments.get(0).getDataSource();
    }
  }

  private static class SegmentsToCompact
  {
    private final List<DataSegment> segments;

    SegmentsToCompact(List<DataSegment> segments)
    {
      this.segments = segments;
    }

    List<DataSegment> getSegments()
    {
      return segments;
    }

    int size()
    {
      return segments.size();
    }
  }
}
