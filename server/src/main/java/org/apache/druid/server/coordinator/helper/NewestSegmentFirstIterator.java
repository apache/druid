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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
  private final Map<String, Interval> searchIntervals;

  // dataSource -> end dateTime of the initial searchInterval
  // searchEndDates keeps the endDate of the initial searchInterval (the entire searchInterval). It's immutable and not
  // changed once it's initialized.
  // This is used to determine that we can expect more segments to be added for an interval in the future. If the end of
  // the interval is same with searchEndDate, we can expect more segments to be added and discard the found segments for
  // compaction in this run to further optimize the size of compact segments. See checkCompactableSizeForLastSegmentOrReturn().
  private final Map<String, DateTime> searchEndDates;
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
    this.searchIntervals = new HashMap<>(dataSources.size());
    this.searchEndDates = new HashMap<>(dataSources.size());

    for (Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      final String dataSource = entry.getKey();
      final VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      final DataSourceCompactionConfig config = compactionConfigs.get(dataSource);

      if (config != null && !timeline.isEmpty()) {
        final Interval searchInterval = findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest());
        searchIntervals.put(dataSource, searchInterval);
        searchEndDates.put(dataSource, searchInterval.getEnd());
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
    final Iterator<QueueEntry> iterator = queue.iterator();
    while (iterator.hasNext()) {
      final QueueEntry entry = iterator.next();
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
   * {@link #searchIntervals} is updated according to the found segments. That is, the interval of the found segments
   * are removed from the searchInterval of the given dataSource.
   */
  private void updateQueue(String dataSourceName, DataSourceCompactionConfig config)
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(dataSourceName);

    if (timeline == null || timeline.isEmpty()) {
      log.warn("Cannot find timeline for dataSource[%s]. Continue to the next dataSource", dataSourceName);
      return;
    }

    final Interval searchInterval = Preconditions.checkNotNull(
        searchIntervals.get(dataSourceName),
        "Cannot find intervals to find for dataSource[%s]",
        dataSourceName
    );
    final DateTime searchEnd = Preconditions.checkNotNull(
        searchEndDates.get(dataSourceName),
        "searchEndDate for dataSource[%s]",
        dataSourceName
    );

    final Pair<Interval, SegmentsToCompact> pair = findSegmentsToCompact(
        timeline,
        searchInterval,
        searchEnd,
        config
    );
    final List<DataSegment> segmentsToCompact = pair.rhs.getSegments();
    final Interval remainingSearchInterval = pair.lhs;

    searchIntervals.put(dataSourceName, remainingSearchInterval);
    if (!segmentsToCompact.isEmpty()) {
      queue.add(new QueueEntry(segmentsToCompact));
    }
  }

  /**
   * Find segments to compact together for the given intervalToSearch. It progressively searches the given
   * intervalToSearch in time order (latest first). The timeline lookup duration is one day. It means, the timeline is
   * looked up for the last one day of the given intervalToSearch, and the next day is searched again if the size of
   * found segments are not enough to compact. This is repeated until enough amount of segments are found.
   *
   * @param timeline         timeline of a dataSource
   * @param intervalToSearch interval to search
   * @param searchEnd        the end of the whole searchInterval
   * @param config           compaction config
   *
   * @return a pair of the reduced interval of (intervalToSearch - interval of found segments) and segments to compact
   */
  @VisibleForTesting
  static Pair<Interval, SegmentsToCompact> findSegmentsToCompact(
      final VersionedIntervalTimeline<String, DataSegment> timeline,
      final Interval intervalToSearch,
      final DateTime searchEnd,
      final DataSourceCompactionConfig config
  )
  {
    final long targetCompactionSize = config.getTargetCompactionSizeBytes();
    final int numTargetSegments = config.getNumTargetCompactionSegments();
    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    Interval searchInterval = intervalToSearch;
    long totalSegmentsToCompactBytes = 0;

    // Finds segments to compact together while iterating searchInterval from latest to oldest
    while (!Intervals.isEmpty(searchInterval)
           && totalSegmentsToCompactBytes < targetCompactionSize
           && segmentsToCompact.size() < numTargetSegments) {
      final Interval lookupInterval = SegmentCompactorUtil.getNextLoopupInterval(searchInterval);
      // holders are sorted by their interval
      final List<TimelineObjectHolder<String, DataSegment>> holders = timeline.lookup(lookupInterval);

      if (holders.isEmpty()) {
        // We found nothing. Continue to the next interval.
        searchInterval = SegmentCompactorUtil.removeIntervalFromEnd(searchInterval, lookupInterval);
        continue;
      }

      for (int i = holders.size() - 1; i >= 0; i--) {
        final TimelineObjectHolder<String, DataSegment> holder = holders.get(i);
        final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holder.getObject().iterator());
        final long partitionBytes = chunks.stream().mapToLong(chunk -> chunk.getObject().getSize()).sum();
        if (chunks.size() == 0 || partitionBytes == 0) {
          log.warn("Skip empty shard[%s]", holder);
          continue;
        }

        if (!intervalToSearch.contains(chunks.get(0).getObject().getInterval())) {
          searchInterval = SegmentCompactorUtil.removeIntervalFromEnd(
              searchInterval,
              new Interval(chunks.get(0).getObject().getInterval().getStart(), searchInterval.getEnd())
          );
          continue;
        }

        // Addition of the segments of a partition should be atomic.
        if (SegmentCompactorUtil.isCompactible(targetCompactionSize, totalSegmentsToCompactBytes, partitionBytes) &&
            segmentsToCompact.size() + chunks.size() <= numTargetSegments) {
          chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
          totalSegmentsToCompactBytes += partitionBytes;
        } else {
          if (segmentsToCompact.size() > 1) {
            // We found some segmens to compact and cannot add more. End here.
            return checkCompactableSizeForLastSegmentOrReturn(
                segmentsToCompact,
                totalSegmentsToCompactBytes,
                timeline,
                searchInterval,
                searchEnd,
                config
            );
          } else {
            // (*) Discard segments found so far because we can't compact it anyway.
            final int numSegmentsToCompact = segmentsToCompact.size();
            segmentsToCompact.clear();

            if (!SegmentCompactorUtil.isCompactible(targetCompactionSize, 0, partitionBytes)) {
              // TODO: this should be changed to compact many small segments into a few large segments
              final DataSegment segment = chunks.get(0).getObject();
              log.warn(
                  "shardSize[%d] for dataSource[%s] and interval[%s] is larger than targetCompactionSize[%d]."
                  + " Contitnue to the next shard.",
                  partitionBytes,
                  segment.getDataSource(),
                  segment.getInterval(),
                  targetCompactionSize
              );
            } else if (numTargetSegments < chunks.size()) {
              final DataSegment segment = chunks.get(0).getObject();
              log.warn(
                  "The number of segments[%d] for dataSource[%s] and interval[%s] is larger than "
                  + "numTargetCompactSegments[%d]. If you see lots of shards are being skipped due to too many "
                  + "segments, consider increasing 'numTargetCompactionSegments' and "
                  + "'druid.indexer.runner.maxZnodeBytes'. Contitnue to the next shard.",
                  chunks.size(),
                  segment.getDataSource(),
                  segment.getInterval(),
                  numTargetSegments
              );
            } else {
              if (numSegmentsToCompact == 1) {
                // We found a segment which is smaller than targetCompactionSize but too large to compact with other
                // segments. Skip this one.
                // Note that segmentsToCompact is already cleared at (*).
                chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
                totalSegmentsToCompactBytes = partitionBytes;
              } else {
                throw new ISE(
                    "Cannot compact segments[%s]. shardBytes[%s], numSegments[%s]",
                    chunks.stream().map(PartitionChunk::getObject).collect(Collectors.toList()),
                    partitionBytes,
                    chunks.size()
                );
              }
            }
          }
        }

        // Update searchInterval
        searchInterval = SegmentCompactorUtil.removeIntervalFromEnd(
            searchInterval,
            new Interval(chunks.get(0).getObject().getInterval().getStart(), searchInterval.getEnd())
        );
      }
    }

    if (segmentsToCompact.size() == 0 || segmentsToCompact.size() == 1) {
      if (Intervals.isEmpty(searchInterval)) {
        // We found nothing to compact. End here.
        return Pair.of(intervalToSearch, new SegmentsToCompact(ImmutableList.of()));
      } else {
        // We found only 1 segment. Further find segments for the remaining interval.
        return findSegmentsToCompact(timeline, searchInterval, searchEnd, config);
      }
    }

    return checkCompactableSizeForLastSegmentOrReturn(
        segmentsToCompact,
        totalSegmentsToCompactBytes,
        timeline,
        searchInterval,
        searchEnd,
        config
    );
  }

  /**
   * Check the found segments are enough to compact. If it's expected that more data will be added in the future for the
   * interval of found segments, the found segments are skipped and remained to be considered again in the next
   * coordinator run. Otherwise, simply returns a pair of the given searchInterval and found segments.
   */
  private static Pair<Interval, SegmentsToCompact> checkCompactableSizeForLastSegmentOrReturn(
      final List<DataSegment> segmentsToCompact,
      final long totalSegmentsToCompactBytes,
      final VersionedIntervalTimeline<String, DataSegment> timeline,
      final Interval searchInterval,
      final DateTime searchEnd,
      final DataSourceCompactionConfig config
  )
  {
    if (segmentsToCompact.size() > 0) {
      // Check we have enough segments to compact. For realtime dataSources, we can expect more data to be added in the
      // future, so we skip compaction for segments in this run if their size is not sufficiently large.
      final DataSegment lastSegment = segmentsToCompact.get(segmentsToCompact.size() - 1);
      if (lastSegment.getInterval().getEnd().equals(searchEnd) &&
          !SegmentCompactorUtil.isProperCompactionSize(
              config.getTargetCompactionSizeBytes(),
              totalSegmentsToCompactBytes
          ) &&
          config.getNumTargetCompactionSegments() > segmentsToCompact.size()) {
        // Ignore found segments and find again for the remaininig searchInterval.
        return findSegmentsToCompact(timeline, searchInterval, searchEnd, config);
      }
    }

    return Pair.of(searchInterval, new SegmentsToCompact(segmentsToCompact));
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

    public List<DataSegment> getSegments()
    {
      return segments;
    }
  }
}
