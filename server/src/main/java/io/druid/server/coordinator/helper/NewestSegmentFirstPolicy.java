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
import java.util.PriorityQueue;
import java.util.stream.StreamSupport;

/**
 * This policy searches segments for compaction from the newest one to oldest one.
 */
public class NewestSegmentFirstPolicy implements CompactionSegmentSearchPolicy
{
  private static final Logger log = new Logger(NewestSegmentFirstPolicy.class);

  private Map<String, CoordinatorCompactionConfig> compactionConfigs;
  private Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;
  private Map<String, Interval> intervalsToFind;
  private PriorityQueue<QueueEntry> queue = new PriorityQueue<>(
      (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o2.interval, o1.interval)
  );

  @Override
  public void reset(
      Map<String, CoordinatorCompactionConfig> compactionConfigs,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources
  )
  {
    this.compactionConfigs = compactionConfigs;
    this.dataSources = dataSources;
    this.intervalsToFind = new HashMap<>(dataSources.size());
    this.queue.clear();

    for (Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      final String dataSource = entry.getKey();
      final VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      final CoordinatorCompactionConfig config = compactionConfigs.get(dataSource);

      log.info("Resetting dataSource[%s]", dataSource);

      if (config != null && !timeline.isEmpty()) {
        intervalsToFind.put(dataSource, findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest()));
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

  private static Interval findInitialSearchInterval(
      VersionedIntervalTimeline<String, DataSegment> timeline,
      Period skipOffset
  )
  {
    Preconditions.checkArgument(timeline != null && !timeline.isEmpty(), "timeline should not be null or empty");
    Preconditions.checkNotNull(skipOffset, "skipOffset");
    final TimelineObjectHolder<String, DataSegment> first = Preconditions.checkNotNull(timeline.first(), "first");
    final TimelineObjectHolder<String, DataSegment> last = Preconditions.checkNotNull(timeline.last(), "last");
    final List<TimelineObjectHolder<String, DataSegment>> holdersInSkipOffset = timeline.lookup(
        new Interval(skipOffset, last.getInterval().getEnd())
    );
    if (holdersInSkipOffset.size() > 0) {
      final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holdersInSkipOffset.get(0).getObject());
      if (chunks.size() > 0) {
        return new Interval(
            first.getInterval().getStart(),
            last.getInterval().getEnd().minus(chunks.get(0).getObject().getInterval().toDuration())
        );
      }
    }

    return new Interval(
        first.getInterval().getStart(),
        last.getInterval().getEnd().minus(skipOffset)
    );
  }

  @Nullable
  @Override
  public List<DataSegment> nextSegments()
  {
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

    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    long remainingBytesToCompact = config.getTargetCompactionSizeBytes();
    while (remainingBytesToCompact > 0) {
      final Pair<Interval, SegmentsToCompact> pair = findSegmentsToCompact(
          timeline,
          intervalToFind,
          remainingBytesToCompact
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

  private static Pair<Interval, SegmentsToCompact> findSegmentsToCompact(
      final VersionedIntervalTimeline<String, DataSegment> timeline,
      final Interval intervalToFind,
      final long remainingBytes
  )
  {
    final List<DataSegment> segmentsToCompact = new ArrayList<>();
    Interval searchInterval = intervalToFind;
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

      for (TimelineObjectHolder<String, DataSegment> holder : holders) {
        final List<PartitionChunk<DataSegment>> chunks = Lists.newArrayList(holder.getObject().iterator());
        final long partitionBytes = chunks.stream().mapToLong(chunk -> chunk.getObject().getSize()).sum();
        if (chunks.size() == 0 || partitionBytes == 0) {
          continue;
        }

        // Addition of the segments of a partition should be atomic.
        if (SegmentCompactorUtil.isCompactable(remainingBytes, totalSegmentsToCompactBytes, partitionBytes)) {
          chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
          totalSegmentsToCompactBytes += partitionBytes;
        } else {
          if (segmentsToCompact.size() > 1) {
            // We found some segmens to compact and cannot add more. End here.
            return Pair.of(searchInterval, new SegmentsToCompact(segmentsToCompact, totalSegmentsToCompactBytes));
          } else if (segmentsToCompact.size() == 1) {
            // We found a segment which is smaller than remainingBytes but too large to compact with other
            // segments. Skip this one.
            segmentsToCompact.clear();
            chunks.forEach(chunk -> segmentsToCompact.add(chunk.getObject()));
            totalSegmentsToCompactBytes = partitionBytes;
          } else {
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
          }
        }

        // Update searchInterval
        searchInterval = SegmentCompactorUtil.removeIntervalFromEnd(
            searchInterval, chunks.get(0).getObject().getInterval()
        );
      }
    }

    if (segmentsToCompact.size() == 0 || segmentsToCompact.size() == 1) {
      if (Intervals.isEmpty(searchInterval)) {
        // We found nothing to compact. End here.
        return Pair.of(intervalToFind, new SegmentsToCompact(ImmutableList.of(), 0));
      } else {
        // We found only 1 segment. Further find segments for the remaining interval.
        return findSegmentsToCompact(timeline, searchInterval, remainingBytes);
      }
    }

    return Pair.of(searchInterval, new SegmentsToCompact(segmentsToCompact, totalSegmentsToCompactBytes));
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
