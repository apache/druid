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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.metadata.IndexingStateFingerprintMapper;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.Partitions;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
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
  private final IndexingStateFingerprintMapper fingerprintMapper;

  private final List<CompactionCandidate> compactedSegments = new ArrayList<>();
  private final List<CompactionCandidate> skippedSegments = new ArrayList<>();

  // This is needed for datasource that has segmentGranularity configured
  // If configured segmentGranularity in config is finer than current segmentGranularity, the same set of segments
  // can belong to multiple intervals in the timeline. We keep track of the compacted intervals between each
  // run of the compaction job and skip any interval that was already previously compacted.
  private final Set<Interval> queuedIntervals = new HashSet<>();

  private final PriorityQueue<CompactionCandidate> queue;

  public DataSourceCompactibleSegmentIterator(
      DataSourceCompactionConfig config,
      SegmentTimeline timeline,
      List<Interval> skipIntervals,
      CompactionCandidateSearchPolicy searchPolicy,
      IndexingStateFingerprintMapper indexingStateFingerprintMapper
  )
  {
    this.config = config;
    this.dataSource = config.getDataSource();
    this.queue = new PriorityQueue<>(searchPolicy::compareCandidates);
    this.fingerprintMapper = indexingStateFingerprintMapper;

    if (skipIntervals.contains(Intervals.ETERNITY) || config.getSkipIntervals().contains(Intervals.ETERNITY)) {
      skipAllSegments(timeline, skipIntervals);
    } else {
      populateQueue(timeline, skipIntervals);
    }
  }

  private void skipAllSegments(SegmentTimeline timeline, List<Interval> skipIntervals)
  {
    if (timeline == null || timeline.isEmpty()) {
      return;
    }
    final List<DataSegment> allSegments = new ArrayList<>(
        timeline.findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE)
    );
    if (allSegments.isEmpty()) {
      return;
    }
    final String reason = skipIntervals.contains(Intervals.ETERNITY)
                           ? StringUtils.format("Interval[%s] locked by another task", Intervals.ETERNITY)
                           : StringUtils.format("Interval[%s] skipped by compaction config", Intervals.ETERNITY);
    skippedSegments.add(CompactionCandidate.from(allSegments, null, CompactionStatus.skipped(reason)));
  }

  private void populateQueue(SegmentTimeline timeline, List<Interval> skipIntervals)
  {
    if (timeline != null) {
      if (!timeline.isEmpty()) {
        SegmentTimeline originalTimeline = null;
        if (config.getSegmentGranularity() != null) {
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
            // Do not use the target segment granularity in the CompactionCandidate
            // as Granularities.getIterable() will cause OOM due to the above issue
            CompactionCandidate candidatesWithStatus = CompactionCandidate.from(
                partialEternitySegments,
                null,
                CompactionStatus.skipped("Segments have partial-eternity intervals")
            );
            skippedSegments.add(candidatesWithStatus);
            return;
          }

          // Convert original segmentGranularity to new granularities bucket by configuredSegmentGranularity
          // For example, if the original is interval of 2020-01-28/2020-02-03 with WEEK granularity
          // and the configuredSegmentGranularity is MONTH, the segment will be split to two segments
          // of 2020-01/2020-02 and 2020-02/2020-03.
          final SegmentTimeline timelineWithConfiguredSegmentGranularity = new SegmentTimeline();
          final Map<Interval, Set<DataSegment>> intervalToPartitionMap = new HashMap<>();
          for (DataSegment segment : segments) {
            for (Interval interval : config.getSegmentGranularity().getIterable(segment.getInterval())) {
              intervalToPartitionMap.computeIfAbsent(interval, k -> new HashSet<>())
                                    .add(segment);
            }
          }

          final String temporaryVersion = DateTimes.nowUtc().toString();
          for (Map.Entry<Interval, Set<DataSegment>> partitionsPerInterval : intervalToPartitionMap.entrySet()) {
            Interval interval = partitionsPerInterval.getKey();
            Set<DataSegment> segmentSet = partitionsPerInterval.getValue();
            int partitions = segmentSet.size();
            timelineWithConfiguredSegmentGranularity.addAll(
                Iterators.transform(
                    segmentSet.iterator(),
                    new Function<>()
                    {
                      int partitionNum = 0;

                      @Override
                      public VersionedIntervalTimeline.PartitionChunkEntry<String, DataSegment> apply(DataSegment segment)
                      {
                        final DataSegment segmentForCompact =
                            segment.withShardSpec(new NumberedShardSpec(partitionNum, partitions));
                        return new VersionedIntervalTimeline.PartitionChunkEntry<>(
                            interval,
                            temporaryVersion,
                            NumberedPartitionChunk.make(partitionNum++, partitions, segmentForCompact)
                        );
                      }
                    }
                )
            );
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
  public List<CompactionCandidate> getCompactedSegments()
  {
    return compactedSegments;
  }

  @Override
  public List<CompactionCandidate> getSkippedSegments()
  {
    return skippedSegments;
  }

  @Override
  public boolean hasNext()
  {
    return !queue.isEmpty();
  }

  @Override
  public CompactionCandidate next()
  {
    if (hasNext()) {
      return queue.poll();
    } else {
      throw new NoSuchElementException();
    }
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

      // Do not compact an interval which contains a single tombstone
      // If there are multiple tombstones in the interval, we may still want to compact them
      if (segments.size() == 1 && segments.get(0).isTombstone()) {
        continue;
      }

      final CompactionStatus compactionStatus = CompactionStatus.compute(segments, config, fingerprintMapper);
      final CompactionCandidate candidates = CompactionCandidate.from(
          segments,
          config.getSegmentGranularity(),
          compactionStatus
      );

      if (compactionStatus.isComplete()) {
        compactedSegments.add(candidates);
      } else if (compactionStatus.isSkipped()) {
        skippedSegments.add(candidates);
      } else if (!queuedIntervals.contains(candidates.getUmbrellaInterval())) {
        queue.add(candidates);
        queuedIntervals.add(candidates.getUmbrellaInterval());
      }
    }
  }

  /**
   * Returns the initial search intervals for compaction, excluding the provided skipIntervals,
   * {@code config.getSkipIntervals()} and the computed skip interval from
   * {@code config.getSkipOffsetFromLatest()}.
   */
  @VisibleForTesting
  List<Interval> findInitialSearchInterval(SegmentTimeline timeline, List<Interval> skipIntervals)
  {
    final Period skipOffset = config.getSkipOffsetFromLatest();
    Preconditions.checkArgument(timeline != null && !timeline.isEmpty(), "timeline should not be null or empty");
    Preconditions.checkNotNull(skipOffset, "skipOffset");

    final TimelineObjectHolder<String, DataSegment> first = Preconditions.checkNotNull(timeline.first(), "first");
    final TimelineObjectHolder<String, DataSegment> last = Preconditions.checkNotNull(timeline.last(), "last");
    final Granularity segmentGranularity = config.getSegmentGranularity();
    final DateTime latestDataTimestamp = last.getInterval().getEnd();
    final List<Interval> allSkipIntervals = JodaUtils.condenseIntervals(Iterables.concat(
        Iterables.transform(skipIntervals, this::alignToSegmentGranularity),
        Iterables.transform(config.getSkipIntervals(), this::alignToSegmentGranularity),
        List.of(alignToSegmentGranularity(new Interval(skipOffset, latestDataTimestamp)))
    ));

    // Collect stats for all skipped segments
    for (Interval skipInterval : allSkipIntervals) {
      final List<DataSegment> segments = new ArrayList<>(
          timeline.findNonOvershadowedObjectsInInterval(skipInterval, Partitions.ONLY_COMPLETE)
      );
      if (!segments.isEmpty()) {
        skippedSegments.add(CompactionCandidate.from(
            segments,
            segmentGranularity,
            CompactionStatus.skipped(describeSkipReason(skipInterval, skipOffset, config.getSkipIntervals(), skipIntervals))
        ));
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
          .toList();

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

      final Interval searchInterval = new Interval(searchStart, searchEnd);
      final Interval overlappingSkipInterval = allSkipIntervals.stream()
                                                                .filter(searchInterval::overlaps)
                                                                .findFirst()
                                                                .orElse(null);

      // Guardrail check, this should never happen
      if (overlappingSkipInterval != null) {
        log.warn(
            "searchInterval[%s] for datasource[%s] unexpectedly overlaps skipInterval[%s]: %s, skipping it",
            searchInterval, dataSource, overlappingSkipInterval,
            describeSkipReason(overlappingSkipInterval, skipOffset, config.getSkipIntervals(), skipIntervals)
        );
        continue;
      }
      searchIntervals.add(searchInterval);
    }

    return searchIntervals;
  }

  private static String describeSkipReason(
      Interval skipInterval,
      Period skipOffset,
      List<Interval> configuredSkipIntervals,
      List<Interval> lockedIntervals
  )
  {
    if (lockedIntervals.stream().anyMatch(skipInterval::overlaps)) {
      return StringUtils.format("Interval[%s] locked by another task", skipInterval);
    } else if (configuredSkipIntervals.stream().anyMatch(skipInterval::overlaps)) {
      return StringUtils.format("Interval[%s] skipped by compaction config", skipInterval);
    } else {
      return StringUtils.format("Skip offset from latest[%s]", skipOffset);
    }
  }

  private Interval alignToSegmentGranularity(Interval interval)
  {
    final Granularity segmentGranularity = config.getSegmentGranularity();
    if (segmentGranularity == null) {
      return interval;
    }
    final DateTime alignedStart = segmentGranularity.bucketStart(interval.getStart());
    final DateTime endBucketStart = segmentGranularity.bucketStart(interval.getEnd());
    final DateTime alignedEnd = endBucketStart.isEqual(interval.getEnd())
                                 ? interval.getEnd()
                                 : segmentGranularity.bucketEnd(interval.getEnd());
    return new Interval(alignedStart, alignedEnd);
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
        log.debug(
            "skipInterval[%s] is not contained in remainingInterval[%s]",
            skipInterval, new Interval(remainingStart, remainingEnd)
        );
      }
    }

    if (remainingStart.isBefore(remainingEnd)) {
      filteredIntervals.add(new Interval(remainingStart, remainingEnd));
    }

    return filteredIntervals;
  }

}
