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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
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
 * This class iterates all segments of the dataSources configured for compaction from the newest to the oldest.
 */
public class NewestSegmentFirstIterator implements CompactionSegmentIterator
{
  private static final Logger log = new Logger(NewestSegmentFirstIterator.class);

  private final ObjectMapper objectMapper;
  private final Map<String, DataSourceCompactionConfig> compactionConfigs;
  private final Map<String, CompactionStatistics> compactedSegmentStats = new HashMap<>();
  private final Map<String, CompactionStatistics> skippedSegmentStats = new HashMap<>();

  private final Map<String, CompactibleSegmentIterator> timelineIterators;

  // This is needed for datasource that has segmentGranularity configured
  // If configured segmentGranularity in config is finer than current segmentGranularity, the same set of segments
  // can belong to multiple intervals in the timeline. We keep track of the compacted intervals between each
  // run of the compaction job and skip any interval that was already previously compacted.
  private final Map<String, Set<Interval>> intervalCompactedForDatasource = new HashMap<>();

  private final PriorityQueue<SegmentsToCompact> queue = new PriorityQueue<>(
      (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o2.getUmbrellaInterval(), o1.getUmbrellaInterval())
  );

  NewestSegmentFirstIterator(
      ObjectMapper objectMapper,
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, SegmentTimeline> dataSources,
      Map<String, List<Interval>> skipIntervals
  )
  {
    this.objectMapper = objectMapper;
    this.compactionConfigs = compactionConfigs;
    this.timelineIterators = Maps.newHashMapWithExpectedSize(dataSources.size());

    dataSources.forEach((dataSource, timeline) -> {
      final DataSourceCompactionConfig config = compactionConfigs.get(dataSource);
      Granularity configuredSegmentGranularity = null;
      if (config != null && !timeline.isEmpty()) {
        SegmentTimeline originalTimeline = null;
        if (config.getGranularitySpec() != null && config.getGranularitySpec().getSegmentGranularity() != null) {
          String temporaryVersion = DateTimes.nowUtc().toString();
          Map<Interval, Set<DataSegment>> intervalToPartitionMap = new HashMap<>();
          configuredSegmentGranularity = config.getGranularitySpec().getSegmentGranularity();
          // Create a new timeline to hold segments in the new configured segment granularity
          SegmentTimeline timelineWithConfiguredSegmentGranularity = new SegmentTimeline();
          Set<DataSegment> segments = timeline.findNonOvershadowedObjectsInInterval(Intervals.ETERNITY, Partitions.ONLY_COMPLETE);
          for (DataSegment segment : segments) {
            // Convert original segmentGranularity to new granularities bucket by configuredSegmentGranularity
            // For example, if the original is interval of 2020-01-28/2020-02-03 with WEEK granularity
            // and the configuredSegmentGranularity is MONTH, the segment will be split to two segments
            // of 2020-01/2020-02 and 2020-02/2020-03.
            if (Intervals.ETERNITY.getStart().equals(segment.getInterval().getStart())
                || Intervals.ETERNITY.getEnd().equals(segment.getInterval().getEnd())) {
              // This is to prevent the coordinator from crashing as raised in https://github.com/apache/druid/issues/13208
              log.warn("Cannot compact datasource[%s] containing segments with partial-ETERNITY intervals", dataSource);
              return;
            }
            for (Interval interval : configuredSegmentGranularity.getIterable(segment.getInterval())) {
              intervalToPartitionMap.computeIfAbsent(interval, k -> new HashSet<>()).add(segment);
            }
          }
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
        final List<Interval> searchIntervals = findInitialSearchInterval(
            dataSource,
            timeline,
            config.getSkipOffsetFromLatest(),
            configuredSegmentGranularity,
            skipIntervals.get(dataSource)
        );
        if (!searchIntervals.isEmpty()) {
          timelineIterators.put(
              dataSource,
              new CompactibleSegmentIterator(timeline, searchIntervals, originalTimeline)
          );
        }
      }
    });

    compactionConfigs.forEach((dataSourceName, config) -> {
      if (config == null) {
        throw new ISE("Unknown dataSource[%s]", dataSourceName);
      }
      updateQueue(dataSourceName, config);
    });
  }

  @Override
  public Map<String, CompactionStatistics> totalCompactedStatistics()
  {
    return compactedSegmentStats;
  }

  @Override
  public Map<String, CompactionStatistics> totalSkippedStatistics()
  {
    return skippedSegmentStats;
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

    final String dataSource = resultSegments.get(0).getDataSource();
    updateQueue(dataSource, compactionConfigs.get(dataSource));

    return entry;
  }

  /**
   * Find the next segments to compact for the given dataSource and add them to the queue.
   * {@link #timelineIterators} is updated according to the found segments. That is, the found segments are removed from
   * the timeline of the given dataSource.
   */
  private void updateQueue(String dataSourceName, DataSourceCompactionConfig config)
  {
    final SegmentsToCompact segmentsToCompact = findSegmentsToCompact(dataSourceName, config);
    if (!segmentsToCompact.isEmpty()) {
      queue.add(segmentsToCompact);
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
   * Finds segments to compact together for the given datasource.
   *
   * @return An empty {@link SegmentsToCompact} if there are no eligible candidates.
   */
  private SegmentsToCompact findSegmentsToCompact(
      final String dataSourceName,
      final DataSourceCompactionConfig config
  )
  {
    final CompactibleSegmentIterator compactibleSegmentIterator
        = timelineIterators.get(dataSourceName);
    if (compactibleSegmentIterator == null) {
      log.warn(
          "Skipping compaction for datasource[%s] as there is no compactible segment in its timeline.",
          dataSourceName
      );
      return SegmentsToCompact.empty();
    }

    final long inputSegmentSize = config.getInputSegmentSizeBytes();

    while (compactibleSegmentIterator.hasNext()) {
      List<DataSegment> segments = compactibleSegmentIterator.next();

      // Do not compact an interval which comprises of a single tombstone
      // If there are multiple tombstones in the interval, we may still want to compact them
      if (segments.size() == 1 && segments.get(0).isTombstone()) {
        continue;
      }

      final SegmentsToCompact candidates = SegmentsToCompact.from(segments);
      final Interval interval = candidates.getUmbrellaInterval();

      final CompactionStatus compactionStatus = CompactionStatus.of(candidates, config, objectMapper);
      if (!compactionStatus.isComplete()) {
        log.debug(
            "Datasource[%s], interval[%s] has [%d] segments that need to be compacted because [%s].",
            dataSourceName, interval, candidates.size(), compactionStatus.getReasonToCompact()
        );
      }

      if (compactionStatus.isComplete()) {
        addSegmentStatsTo(compactedSegmentStats, dataSourceName, candidates);
      } else if (candidates.getTotalBytes() > inputSegmentSize) {
        addSegmentStatsTo(skippedSegmentStats, dataSourceName, candidates);
        log.warn(
            "Skipping compaction for datasource[%s], interval[%s] as total segment size[%d]"
            + " is larger than allowed inputSegmentSize[%d].",
            dataSourceName, interval, candidates.getTotalBytes(), inputSegmentSize
        );
      } else if (config.getGranularitySpec() != null
                 && config.getGranularitySpec().getSegmentGranularity() != null) {
        Set<Interval> compactedIntervals = intervalCompactedForDatasource
            .computeIfAbsent(dataSourceName, k -> new HashSet<>());

        if (compactedIntervals.contains(interval)) {
          // Skip these candidate segments as we have already compacted this interval
        } else {
          compactedIntervals.add(interval);
          return candidates;
        }
      } else {
        return candidates;
      }
    }

    log.debug("No more segments to compact for datasource[%s].", dataSourceName);
    return SegmentsToCompact.empty();
  }

  private void addSegmentStatsTo(
      Map<String, CompactionStatistics> statisticsMap,
      String dataSourceName,
      SegmentsToCompact segments
  )
  {
    statisticsMap.computeIfAbsent(dataSourceName, v -> CompactionStatistics.create())
                 .addFrom(segments);
  }

  /**
   * Returns the initial searchInterval which is {@code (timeline.first().start, timeline.last().end - skipOffset)}.
   *
   * @param timeline      timeline of a dataSource
   * @param skipIntervals intervals to skip
   *
   * @return found interval to search or null if it's not found
   */
  private List<Interval> findInitialSearchInterval(
      String dataSourceName,
      SegmentTimeline timeline,
      Period skipOffset,
      Granularity configuredSegmentGranularity,
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
        configuredSegmentGranularity,
        skipIntervals
    );

    // Collect stats for all skipped segments
    for (Interval skipInterval : fullSkipIntervals) {
      final List<DataSegment> segments = new ArrayList<>(
          timeline.findNonOvershadowedObjectsInInterval(skipInterval, Partitions.ONLY_COMPLETE)
      );
      addSegmentStatsTo(skippedSegmentStats, dataSourceName, SegmentsToCompact.from(segments));
    }

    final Interval totalInterval = new Interval(first.getInterval().getStart(), last.getInterval().getEnd());
    final List<Interval> filteredInterval = filterSkipIntervals(totalInterval, fullSkipIntervals);
    final List<Interval> searchIntervals = new ArrayList<>();

    for (Interval lookupInterval : filteredInterval) {
      if (Intervals.ETERNITY.getStart().equals(lookupInterval.getStart())
          || Intervals.ETERNITY.getEnd().equals(lookupInterval.getEnd())) {
        log.warn("Cannot compact datasource[%s] since interval[%s] coincides with ETERNITY.", dataSourceName, lookupInterval);
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

  @VisibleForTesting
  static List<Interval> sortAndAddSkipIntervalFromLatest(
      DateTime latest,
      Period skipOffset,
      Granularity configuredSegmentGranularity,
      @Nullable List<Interval> skipIntervals
  )
  {
    final List<Interval> nonNullSkipIntervals = skipIntervals == null
                                                ? new ArrayList<>(1)
                                                : new ArrayList<>(skipIntervals.size());
    final Interval skipFromLatest;
    if (configuredSegmentGranularity != null) {
      DateTime skipFromLastest = new DateTime(latest, latest.getZone()).minus(skipOffset);
      DateTime skipOffsetBucketToSegmentGranularity = configuredSegmentGranularity.bucketStart(skipFromLastest);
      skipFromLatest = new Interval(skipOffsetBucketToSegmentGranularity, latest);
    } else {
      skipFromLatest = new Interval(skipOffset, latest);
    }

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
