/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.CoordinatorCompactionConfig;
import io.druid.server.coordinator.helper.DruidCoordinatorSegmentCompactor.SegmentsToCompact;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
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

public class NewestSegmentFirstIterator implements CompactionSegmentIterator
{
  private static final Logger log = new Logger(NewestSegmentFirstIterator.class);

  private Map<String, CoordinatorCompactionConfig> compactionConfigs;
  private Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;
  private Map<String, Interval> intervalsToFind;
  private Map<String, DateTime> searchEndDates;
  private PriorityQueue<QueueEntry> queue = new PriorityQueue<>(
      (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o2.interval, o1.interval)
  );

  NewestSegmentFirstIterator(
      Map<String, CoordinatorCompactionConfig> compactionConfigs,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources
  )
  {
    this.compactionConfigs = compactionConfigs;
    this.dataSources = dataSources;
    this.intervalsToFind = new HashMap<>(dataSources.size());
    this.searchEndDates = new HashMap<>(dataSources.size());

    for (Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      final String dataSource = entry.getKey();
      final VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      final CoordinatorCompactionConfig config = compactionConfigs.get(dataSource);

      log.info("Initializing dataSource[%s]", dataSource);

      if (config != null && !timeline.isEmpty()) {
        final Interval searchInterval = findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest());
        intervalsToFind.put(dataSource, searchInterval);
        if (!isZero(config.getSkipOffsetFromLatest())) {
          searchEndDates.put(dataSource, searchInterval.getEnd());
        }
      }
    }

    for (Entry<String, CoordinatorCompactionConfig> entry : compactionConfigs.entrySet()) {
      final String dataSourceName = entry.getKey();
      final CoordinatorCompactionConfig config = entry.getValue();

      if (config == null) {
        throw new ISE("Unknown dataSource[%s]", dataSourceName);
      }

      updateQueue(dataSourceName, config);
    }
  }

  @Override
  public Object2LongOpenHashMap<String> remainingSegments()
  {
    final Object2LongOpenHashMap<String> resultMap = new Object2LongOpenHashMap<>();
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
                 .count()
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
      return null;
    }

    final List<DataSegment> resultSegments = entry.segments;

    if (resultSegments.size() > 0) {
      final String dataSource = resultSegments.get(0).getDataSource();
      updateQueue(dataSource, compactionConfigs.get(dataSource));
    }

    return resultSegments;
  }

  private void updateQueue(String dataSourceName, CoordinatorCompactionConfig config)
  {
    VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(dataSourceName);

    if (timeline == null || timeline.isEmpty()) {
      log.warn("Cannot find timeline for dataSource[%s]. Continue to the next dataSource", dataSourceName);
      return;
    }

    Interval intervalToFind = intervalsToFind.get(dataSourceName);
    if (intervalToFind == null) {
      throw new ISE("Cannot find intervals to find for dataSource[%s]", dataSourceName);
    }
    // searchEnd can be null if skipOffsetFromLatest is not set for dataSource
    final DateTime searchEnd = searchEndDates.get(dataSourceName);

    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    long remainingBytesToCompact = config.getTargetCompactionSizeBytes();
    while (remainingBytesToCompact > 0) {
      final Pair<Interval, SegmentsToCompact> pair = findSegmentsToCompact(
          timeline,
          intervalToFind,
          searchEnd,
          remainingBytesToCompact,
          config.getTargetCompactionSizeBytes()
      );
      if (pair.rhs.getSegments().isEmpty()) {
        break;
      }
      segmentsToCompact.addAll(pair.rhs.getSegments());
      remainingBytesToCompact -= pair.rhs.getByteSize();
      intervalToFind = pair.lhs;
    }

    intervalsToFind.put(dataSourceName, intervalToFind);
    if (!segmentsToCompact.isEmpty()) {
      queue.add(new QueueEntry(segmentsToCompact));
    }
  }

  @VisibleForTesting
  static Pair<Interval, SegmentsToCompact> findSegmentsToCompact(
      final VersionedIntervalTimeline<String, DataSegment> timeline,
      final Interval intervalToSearch,
      @Nullable final DateTime searchEnd,
      final long remainingBytes,
      final long targetCompactionSizeBytes
  )
  {
    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    Interval searchInterval = intervalToSearch;
    long totalSegmentsToCompactBytes = 0;

    // Finds segments to compact together while iterating intervalToFind from latest to oldest
    while (!Intervals.isEmpty(searchInterval) && totalSegmentsToCompactBytes < remainingBytes) {
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
        if (SegmentCompactorUtil.isCompactible(remainingBytes, totalSegmentsToCompactBytes, partitionBytes)) {
          chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
          totalSegmentsToCompactBytes += partitionBytes;
        } else {
          if (segmentsToCompact.size() > 1) {
            // We found some segmens to compact and cannot add more. End here.
            return returnIfCompactibleSize(
                segmentsToCompact,
                totalSegmentsToCompactBytes,
                timeline,
                searchInterval,
                searchEnd,
                remainingBytes,
                targetCompactionSizeBytes
            );
          } else if (segmentsToCompact.size() == 1) {
            // We found a segment which is smaller than remainingBytes but too large to compact with other
            // segments. Skip this one.
            segmentsToCompact.clear();
            chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
            totalSegmentsToCompactBytes = partitionBytes;
          } else {
            if (targetCompactionSizeBytes < partitionBytes) {
              // TODO: this should be changed to compact many segments into a few segments
              final DataSegment segment = chunks.get(0).getObject();
              log.warn(
                  "shardSize[%d] for dataSource[%s] and interval [%s] is larger than remainingBytes[%d]."
                  + " Contitnue to the next shard",
                  partitionBytes,
                  segment.getDataSource(),
                  segment.getInterval(),
                  remainingBytes
              );
            } else {
              return Pair.of(intervalToSearch, new SegmentsToCompact(ImmutableList.of(), 0));
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
        return Pair.of(intervalToSearch, new SegmentsToCompact(ImmutableList.of(), 0));
      } else {
        // We found only 1 segment. Further find segments for the remaining interval.
        return findSegmentsToCompact(timeline, searchInterval, searchEnd, remainingBytes, targetCompactionSizeBytes);
      }
    }

    return returnIfCompactibleSize(
        segmentsToCompact,
        totalSegmentsToCompactBytes,
        timeline,
        searchInterval,
        searchEnd,
        remainingBytes,
        targetCompactionSizeBytes
    );
  }

  private static Pair<Interval, SegmentsToCompact> returnIfCompactibleSize(
      final List<DataSegment> segmentsToCompact,
      final long totalSegmentsToCompactBytes,
      final VersionedIntervalTimeline<String, DataSegment> timeline,
      final Interval searchInterval,
      @Nullable final DateTime searchEnd,
      final long remainingBytes,
      final long targetCompactionSizeBytes
  )
  {
    // Check we have enough segments to compact. For realtime dataSources, we can expect more data can be added in the
    // future, so we skip compaction for segments in this run if their size is not sufficiently large.
    // To enable this feature, skipOffsetFromLatest should be set.
    final DataSegment lastSegment = segmentsToCompact.get(segmentsToCompact.size() - 1);
    if (searchEnd != null &&
        lastSegment.getInterval().getEnd().equals(searchEnd) &&
        !SegmentCompactorUtil.isProperCompactionSize(targetCompactionSizeBytes, totalSegmentsToCompactBytes)) {
      return findSegmentsToCompact(timeline, searchInterval, searchEnd, remainingBytes, targetCompactionSizeBytes);
    }

    return Pair.of(searchInterval, new SegmentsToCompact(segmentsToCompact, totalSegmentsToCompactBytes));
  }

  private static boolean isZero(Period period)
  {
    return period.getYears() == 0 &&
           period.getMonths() == 0 &&
           period.getWeeks() == 0 &&
           period.getDays() == 0 &&
           period.getHours() == 0 &&
           period.getMinutes() == 0 &&
           period.getSeconds() == 0 &&
           period.getMillis() == 0;
  }

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
    private final Interval interval;
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
}
