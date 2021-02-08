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
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
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
import org.apache.druid.utils.Streams;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * This class iterates all segments of the dataSources configured for compaction from the high score to the low score.
 */
public class HighScoreSegmentFirstIterator
    extends CompactionSegmentIterator<HighScoreSegmentFirstIterator.Tuple2<Float, List<DataSegment>>>
{
  private static final Logger log = new Logger(HighScoreSegmentFirstIterator.class);
  public static final float MAJOR_COMPACTION_IGNORE_TIME_LEVEL = 0.01f;

  private final Map<String, DataSourceCompactionConfig> compactionConfigs;
  private final Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources;

  // dataSource -> intervalToFind
  // searchIntervals keeps track of the current state of which interval should be considered to search segments to
  // compact.
  private final Map<String, CompactibleTimelineObjectHolderCursor> timelineIterators;

  // minor compact queue
  private final PriorityQueue<QueueEntry> minorQueue = new PriorityQueue<>(
      (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o2.interval, o1.interval)
  );
  // major compact queue
  private final PriorityQueue<QueueEntry> majorQueue = new PriorityQueue<>(
      (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o2.interval, o1.interval)
  );
  private volatile boolean isMajor = false;

  HighScoreSegmentFirstIterator(
      ObjectMapper objectMapper,
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources,
      Map<String, List<Interval>> skipIntervals,
      IndexingServiceClient indexingServiceClient
  )
  {
    super(objectMapper);
    this.compactionConfigs = compactionConfigs;
    this.dataSources = dataSources;
    this.timelineIterators = Maps.newHashMapWithExpectedSize(dataSources.size());

    reset(skipIntervals, indexingServiceClient);
  }

  @Override
  public void reset(
      Map<String, List<Interval>> skipIntervals,
      IndexingServiceClient indexingServiceClient
  )
  {
    dataSources.forEach((String dataSource, VersionedIntervalTimeline<String, DataSegment> timeline) -> {
      final DataSourceCompactionConfig config = compactionConfigs.get(dataSource);

      if (config != null && !timeline.isEmpty()) {
        List<Interval> searchIntervals =
            findInitialSearchInterval(timeline, config.getSkipOffsetFromLatest(), skipIntervals.get(dataSource));
        if (!searchIntervals.isEmpty()) {
          if (indexingServiceClient != null && config.getEnableFilterLockedInterval()) {
            Collections.sort(searchIntervals, (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o1, o2));
            Interval totalSearchInterval = new Interval(
                searchIntervals.get(0).getStart(),
                searchIntervals.get(searchIntervals.size() - 1).getEnd()
            );
            searchIntervals = indexingServiceClient.getNonLockIntervals(dataSource, totalSearchInterval);
          }
          if (!searchIntervals.isEmpty()) {
            timelineIterators.put(
                dataSource,
                new CompactibleTimelineObjectHolderCursor(timeline, searchIntervals, config)
            );
          }
        }
      }
    });

    compactionConfigs.forEach((String dataSourceName, DataSourceCompactionConfig config) -> {
      if (config == null) {
        throw new ISE("Unknown dataSource[%s]", dataSourceName);
      }
      updateQueue(dataSourceName, config, isMajor, 0);
    });
  }

  @Override
  public void setCompactType(boolean isMajorNew)
  {
    if (this.isMajor == isMajorNew) {
      return;
    }
    this.isMajor = isMajorNew;
    compactionConfigs.forEach((String dataSourceName, DataSourceCompactionConfig config) -> {
      if (config == null) {
        throw new ISE("Unknown dataSource[%s]", dataSourceName);
      }
      updateQueue(dataSourceName, config, isMajorNew, 0);
    });
  }

  @Override
  public boolean hasNext()
  {
    if (isMajor == false) {
      return !minorQueue.isEmpty();
    } else {
      return !majorQueue.isEmpty();
    }
  }

  @Override
  public Tuple2<Float, List<DataSegment>> next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    QueueEntry entry;
    if (isMajor == false) {
      entry = minorQueue.poll();
    } else {
      entry = majorQueue.poll();
    }

    if (entry == null) {
      throw new NoSuchElementException();
    }

    final List<DataSegment> resultSegments = entry.segmentsToCompactTuple2._2;

    Preconditions.checkState(!resultSegments.isEmpty(), "Queue entry must not be empty");

    final String dataSource = resultSegments.get(0).getDataSource();
    updateQueue(dataSource, compactionConfigs.get(dataSource), isMajor, 0);

    return entry.segmentsToCompactTuple2;
  }

  @Override
  public void skip(String dataSource, int skipCount)
  {
    updateQueue(dataSource, compactionConfigs.get(dataSource), isMajor, skipCount);
  }

  /**
   * Find the next segments to compact for the given dataSource and add them to the queue.
   * {@link #timelineIterators} is updated according to the found segments. That is, the found segments are removed from
   * the timeline of the given dataSource.
   */
  private void updateQueue(String dataSourceName, DataSourceCompactionConfig config, boolean isMajorNew, int skipCount)
  {
    final CompactibleTimelineObjectHolderCursor compactibleTimelineObjectHolderCursor = timelineIterators.get(
        dataSourceName
    );

    if (compactibleTimelineObjectHolderCursor == null) {
      log.warn("Cannot find timeline for dataSource[%s]. Skip this dataSource", dataSourceName);
      return;
    }

    AtomicInteger skipAtomic = new AtomicInteger(skipCount);
    for (; skipAtomic.get() > 0; ) {
      log.info("Skip[%s] latest candidate segments.", skipCount);
      findSegmentsToCompact(
          dataSourceName,
          compactibleTimelineObjectHolderCursor,
          config,
          isMajorNew,
          skipAtomic
      );
    }

    final Tuple2<Float, List<DataSegment>> segmentsToCompactTuple2 = findSegmentsToCompact(
        dataSourceName,
        compactibleTimelineObjectHolderCursor,
        config,
        isMajorNew,
        skipAtomic
    );

    if (!segmentsToCompactTuple2._2.isEmpty()) {
      if (isMajorNew == false) {
        minorQueue.add(new QueueEntry(segmentsToCompactTuple2));
      } else {
        majorQueue.add(new QueueEntry(segmentsToCompactTuple2));
      }
    }
  }

  @VisibleForTesting
  static List<Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> computeTimelineScore(
      List<TimelineObjectHolder<String, DataSegment>> holders,
      final float timeWeightForRecentDays,
      Period recentDays,
      float smallFileNumWeight
  )
  {
    List<Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> tempScoreHolders = new ArrayList<>();
    final List<Long> times = new ArrayList<>();
    final List<Long> timelineSizes = new ArrayList<>();
    final List<Long> segmentNums = new ArrayList<>();
    holders.forEach(h -> {
      final long endMillis = h.getInterval().getEndMillis();
      final List<Long> dataSegmentSizes = Streams.sequentialStreamFrom(h.getObject())
                                                 .map(chunk -> chunk.getObject().getSize())
                                                 .collect(Collectors.toList());
      times.add(endMillis);
      timelineSizes.add(-dataSegmentSizes.stream().mapToLong(Long::longValue).sum());
      segmentNums.add((long) dataSegmentSizes.size());
    });
    int totalDay = getTimesDays(times);
    float timeFactor = Math.max(1, totalDay / recentDays.getDays());
    // transform list using min-max and compute score
    double[] timeFloats = minMaxNormalization(times);
    double[] timelineSizeFloats = minMaxNormalization(timelineSizes);
    double[] segmentNumFloats = minMaxNormalization(segmentNums);
    for (int i = 0; i < holders.size(); i++) {
      float score = (float) (timeWeightForRecentDays * timeFactor * timeFloats[i] +
                             (1 - smallFileNumWeight) * timelineSizeFloats[i] +
                             smallFileNumWeight * segmentNumFloats[i]);
      tempScoreHolders.add(new Tuple2(score, holders.get(i)));
    }
    // high score first: score asc sort
    tempScoreHolders.sort(Comparator.comparingDouble(o -> o._1));
    log.info("Compact iterator[%s] top 20 high score interval and score [%s]", tempScoreHolders.size(),
             Iterables.skip(
                 Iterables.transform(tempScoreHolders, t -> t._2.getInterval() + "=" + t._1),
                 tempScoreHolders.size() > 20 ? tempScoreHolders.size() - 20 : 0
             )
    );
    return tempScoreHolders;
  }

  static int getTimesDays(List<Long> needNorms)
  {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (Long n : needNorms) {
      if (min > n) {
        min = n;
      }
      if (max < n) {
        max = n;
      }
    }
    long finalMin = min;
    long finalMax = max;
    long days = (finalMax - finalMin) / (24 * 3600 * 1000L);
    return (int) days;
  }

  @VisibleForTesting
  static double[] minMaxNormalization(List<Long> needNorms)
  {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (Long n : needNorms) {
      if (min > n) {
        min = n;
      }
      if (max < n) {
        max = n;
      }
    }
    long finalMin = min;
    long finalMax = max;
    if (finalMin == finalMax) {
      return needNorms.stream().mapToDouble(n -> 0).toArray();
    }
    return needNorms.stream().mapToDouble(n -> 1.0 * (n - finalMin) / (finalMax - finalMin)).toArray();
  }

  static boolean isCompactibleHolder(Interval interval, TimelineObjectHolder<String, DataSegment> holder)
  {
    final Iterator<PartitionChunk<DataSegment>> chunks = holder.getObject().iterator();
    if (!chunks.hasNext()) {
      return false; // There should be at least one chunk for a holder to be compactible.
    }
    PartitionChunk<DataSegment> firstChunk = chunks.next();
    if (!interval.contains(firstChunk.getObject().getInterval())) {
      return false;
    }
    long partitionBytes = firstChunk.getObject().getSize();
    while (partitionBytes == 0 && chunks.hasNext()) {
      partitionBytes += chunks.next().getObject().getSize();
    }
    return partitionBytes > 0;
  }

  /**
   * Iterates the given {@link VersionedIntervalTimeline}. Only compactible {@link TimelineObjectHolder}s are returned,
   * which means the holder always has at least one {@link DataSegment}.
   */
  static class CompactibleTimelineObjectHolderCursor implements Iterator<Tuple2<Float, List<DataSegment>>>
  {
    private final float timeWeightForRecentDays = DataSourceCompactionConfig.DEFAULT_COMPACTION_TIME_WEIGHT_FOR_RECENT_DAYS;
    private final float smallFileNumWeight = DataSourceCompactionConfig.DEFAULT_COMPACTION_SEGMENT_NUM_WEIGHT;
    private final Period recentDays;
    private volatile List<Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> minorHolders;
    private volatile List<Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> majorHolders;
    private volatile boolean curIsMajor = false;

    CompactibleTimelineObjectHolderCursor(
        VersionedIntervalTimeline<String, DataSegment> timeline,
        List<Interval> totalIntervalsToSearch,
        DataSourceCompactionConfig dataSourceCompactionConfig
    )
    {
      recentDays = dataSourceCompactionConfig.getMinorCompactRecentDays();
      List<TimelineObjectHolder<String, DataSegment>> holders = totalIntervalsToSearch
          .stream()
          .flatMap(interval -> timeline
              .lookup(interval)
              .stream()
              .filter(holder -> isCompactibleHolder(interval, holder))
          )
          .collect(Collectors.toList());
      // compute score--timeline
      minorHolders = computeTimelineScore(holders, timeWeightForRecentDays, recentDays, smallFileNumWeight);
    }

    public void compareAndUpdateCompactIterator(boolean isMajorNew)
    {
      if (this.curIsMajor != isMajorNew) {
        if (isMajorNew) {
          // update major from minor iterator
          log.info("Update major iterator from minor.");
          majorHolders = computeTimelineScore(minorHolders.stream().map(t -> t._2).collect(Collectors.toList()),
                                              timeWeightForRecentDays * MAJOR_COMPACTION_IGNORE_TIME_LEVEL, recentDays,
                                              smallFileNumWeight
          );
        } else {
          log.info("Update minor iterator from major.");
          minorHolders = computeTimelineScore(majorHolders.stream().map(t -> t._2).collect(Collectors.toList()),
                                              timeWeightForRecentDays * MAJOR_COMPACTION_IGNORE_TIME_LEVEL, recentDays,
                                              smallFileNumWeight
          );
        }
        this.curIsMajor = isMajorNew;
      }
    }

    @Override
    public boolean hasNext()
    {
      if (curIsMajor == false) {
        return !minorHolders.isEmpty();
      } else {
        return !majorHolders.isEmpty();
      }
    }

    @Override
    public Tuple2<Float, List<DataSegment>> next()
    {
      List<Tuple2<Float, TimelineObjectHolder<String, DataSegment>>> holders = curIsMajor == false
                                                                               ? minorHolders
                                                                               : majorHolders;
      if (holders.isEmpty()) {
        throw new NoSuchElementException();
      }
      final Tuple2<Float, TimelineObjectHolder<String, DataSegment>> remove = holders.remove(holders.size() - 1);
      return new Tuple2<>(remove._1, Streams.sequentialStreamFrom(remove._2.getObject())
                                            .map(PartitionChunk::getObject)
                                            .collect(Collectors.toList()));
    }
  }

  public static class Tuple2<T1, T2>
  {
    public T1 _1;
    public T2 _2;

    public Tuple2(T1 _1, T2 _2)
    {
      this._1 = _1;
      this._2 = _2;
    }
  }

  /**
   * Find segments to compact together for the given intervalToSearch. It progressively searches the given
   * intervalToSearch in time order (latest first). The timeline lookup duration is one day. It means, the timeline is
   * looked up for the last one day of the given intervalToSearch, and the next day is searched again if the size of
   * found segments are not enough to compact. This is repeated until enough amount of segments are found.
   *
   * @return segments to compact
   */
  private Tuple2<Float, List<DataSegment>> findSegmentsToCompact(
      String dataSourceName,
      final CompactibleTimelineObjectHolderCursor compactibleTimelineObjectHolderCursor,
      final DataSourceCompactionConfig config,
      boolean isMajorNew,
      AtomicInteger skipAtomic
  )
  {
    compactibleTimelineObjectHolderCursor.compareAndUpdateCompactIterator(isMajorNew);
    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    while (compactibleTimelineObjectHolderCursor.hasNext()) {
      final Tuple2<Float, List<DataSegment>> next = compactibleTimelineObjectHolderCursor.next();
      final SegmentsToCompact candidates = new SegmentsToCompact(next._2);
      if (!candidates.isEmpty() && candidates.getNumberOfSegments() >= config.getMinInputSegmentNum()) {
        final boolean isCompactibleSize = candidates.getTotalSize() <= inputSegmentSize;
        final boolean needsCompaction = needsCompaction(
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment()),
            candidates
        );

        if (isCompactibleSize && needsCompaction) {
          skipAtomic.incrementAndGet();
          return next;
        } else {
          if (!needsCompaction) {
            // Collect statistic for segments that is already compacted
            collectSegmentStatistics(compactedSegments, dataSourceName, candidates);
          } else {
            // Collect statistic for segments that is skipped
            // Note that if segments does not need compaction then we do not double count here
            collectSegmentStatistics(skippedSegments, dataSourceName, candidates);
            log.warn(
                "total segment size[%d] for datasource[%s] and interval[%s] is larger than inputSegmentSize[%d]."
                + " Continue to the next interval.",
                candidates.getTotalSize(),
                candidates.segments.get(0).getDataSource(),
                candidates.segments.get(0).getInterval(),
                inputSegmentSize
            );
          }
        }
      }
    }
    skipAtomic.incrementAndGet();
    log.info("All segments look good! Nothing to compact");
    return new Tuple2<>(0f, Collections.emptyList());
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
    // 查找过滤后的interval列表 filteredInterval
    final List<Interval> filteredInterval = filterSkipIntervals(totalInterval, fullSkipIntervals);
    final List<Interval> searchIntervals = new ArrayList<>();

    for (Interval lookupInterval : filteredInterval) {
      // 从filteredInterval列表中过滤具有完整partition的interval对应的DataSegment列表segments
      final List<DataSegment> segments = timeline
          .findNonOvershadowedObjectsInInterval(lookupInterval, Partitions.ONLY_COMPLETE)
          .stream()
          // findNonOvershadowedObjectsInInterval() may return segments merely intersecting with lookupInterval, while
          // we are interested only in segments fully lying within lookupInterval here.
          // 过滤掉没有完全被interval包含的DataSegment。
          .filter(segment -> lookupInterval.contains(segment.getInterval()))
          .collect(Collectors.toList());

      if (segments.isEmpty()) {
        continue;
      }

      // 从segments中选择一个最小和最大的时间，作为新的interval
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
    private final Tuple2<Float, List<DataSegment>> segmentsToCompactTuple2;

    private QueueEntry(Tuple2<Float, List<DataSegment>> segmentsToCompactTuple2)
    {
      Preconditions.checkArgument(segmentsToCompactTuple2._2 != null && !segmentsToCompactTuple2._2.isEmpty());
      Collections.sort(segmentsToCompactTuple2._2);
      this.interval = new Interval(
          segmentsToCompactTuple2._2.get(0).getInterval().getStart(),
          segmentsToCompactTuple2._2.get(segmentsToCompactTuple2._2.size() - 1).getInterval().getEnd()
      );
      this.segmentsToCompactTuple2 = segmentsToCompactTuple2;
    }

    private String getDataSource()
    {
      return segmentsToCompactTuple2._2.get(0).getDataSource();
    }
  }

}
