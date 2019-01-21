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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DatasourceWhitelist;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidCoordinatorSegmentMerger implements DruidCoordinatorHelper
{
  private static final Logger log = new Logger(DruidCoordinatorSegmentMerger.class);

  private final IndexingServiceClient indexingServiceClient;
  private final AtomicReference<DatasourceWhitelist> whiteListRef;

  @Inject
  public DruidCoordinatorSegmentMerger(
      IndexingServiceClient indexingServiceClient,
      JacksonConfigManager configManager
  )
  {
    this.indexingServiceClient = indexingServiceClient;
    this.whiteListRef = configManager.watch(DatasourceWhitelist.CONFIG_KEY, DatasourceWhitelist.class);
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    DatasourceWhitelist whitelist = whiteListRef.get();

    CoordinatorStats stats = new CoordinatorStats();
    Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources = new HashMap<>();

    // Find serviced segments by using a timeline
    for (DataSegment dataSegment : params.getAvailableSegments()) {
      if (whitelist == null || whitelist.contains(dataSegment.getDataSource())) {
        VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(dataSegment.getDataSource());
        if (timeline == null) {
          timeline = new VersionedIntervalTimeline<String, DataSegment>(Ordering.natural());
          dataSources.put(dataSegment.getDataSource(), timeline);
        }
        timeline.add(
            dataSegment.getInterval(),
            dataSegment.getVersion(),
            dataSegment.getShardSpec().createChunk(dataSegment)
        );
      }
    }

    // Find segments to merge
    for (final Map.Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      // Get serviced segments from the timeline
      VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      List<TimelineObjectHolder<String, DataSegment>> timelineObjects =
          timeline.lookup(new Interval(DateTimes.EPOCH, DateTimes.of("3000-01-01")));

      // Accumulate timelineObjects greedily until we reach our limits, then backtrack to the maximum complete set
      SegmentsToMerge segmentsToMerge = new SegmentsToMerge();

      for (int i = 0; i < timelineObjects.size(); i++) {
        if (!segmentsToMerge.add(timelineObjects.get(i))
            || segmentsToMerge.getByteCount() > params.getCoordinatorDynamicConfig().getMergeBytesLimit()
            || segmentsToMerge.getSegmentCount() >= params.getCoordinatorDynamicConfig().getMergeSegmentsLimit()) {
          i -= segmentsToMerge.backtrack(params.getCoordinatorDynamicConfig().getMergeBytesLimit());

          if (segmentsToMerge.getSegmentCount() > 1) {
            stats.addToGlobalStat("mergedCount", mergeSegments(segmentsToMerge, entry.getKey()));
          }

          if (segmentsToMerge.getSegmentCount() == 0) {
            // Backtracked all the way to zero. Increment by one so we continue to make progress.
            i++;
          }

          segmentsToMerge = new SegmentsToMerge();
        }
      }

      // Finish any timelineObjects to merge that may have not hit threshold
      segmentsToMerge.backtrack(params.getCoordinatorDynamicConfig().getMergeBytesLimit());
      if (segmentsToMerge.getSegmentCount() > 1) {
        stats.addToGlobalStat("mergedCount", mergeSegments(segmentsToMerge, entry.getKey()));
      }
    }

    log.info("Issued merge requests for %s segments", stats.getGlobalStat("mergedCount"));

    params.getEmitter().emit(
        new ServiceMetricEvent.Builder().build(
            "coordinator/merge/count", stats.getGlobalStat("mergedCount")
        )
    );

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  /**
   * Issue merge request for some list of segments.
   *
   * @return number of segments merged
   */
  private int mergeSegments(SegmentsToMerge segmentsToMerge, String dataSource)
  {
    final List<DataSegment> segments = segmentsToMerge.getSegments();
    final List<SegmentId> segmentNames = Lists.transform(segments, DataSegment::getId);

    log.info("[%s] Found %d segments to merge %s", dataSource, segments.size(), segmentNames);

    try {
      indexingServiceClient.mergeSegments(segments);
    }
    catch (Exception e) {
      log.error(e, "[%s] Merging error for segments %s", dataSource, segmentNames);
    }

    return segments.size();
  }

  private static class SegmentsToMerge
  {
    // set of already-included segments (to help us keep track of bytes accumulated)
    private final Multiset<DataSegment> segments;

    // (timeline object, union interval of underlying segments up to this point in the list)
    private final List<Pair<TimelineObjectHolder<String, DataSegment>, Interval>> timelineObjects;

    private long byteCount;

    private SegmentsToMerge()
    {
      this.timelineObjects = new ArrayList<>();
      this.segments = HashMultiset.create();
      this.byteCount = 0;
    }

    public List<DataSegment> getSegments()
    {
      return ImmutableSet.copyOf(
          FunctionalIterable.create(timelineObjects).transformCat(
              new Function<Pair<TimelineObjectHolder<String, DataSegment>, Interval>, Iterable<DataSegment>>()
              {
                @Override
                public Iterable<DataSegment> apply(Pair<TimelineObjectHolder<String, DataSegment>, Interval> input)
                {
                  return Iterables.transform(
                      input.lhs.getObject(),
                      new Function<PartitionChunk<DataSegment>, DataSegment>()
                      {
                        @Override
                        public DataSegment apply(PartitionChunk<DataSegment> input)
                        {
                          return input.getObject();
                        }
                      }
                  );
                }
              }
          )
      ).asList();
    }

    public boolean add(TimelineObjectHolder<String, DataSegment> timelineObject)
    {
      final Interval timelineObjectInterval = timelineObject.getInterval();

      if (timelineObjects.size() > 0) {
        Preconditions.checkArgument(
            timelineObjectInterval.getStart().getMillis() >=
            timelineObjects.get(timelineObjects.size() - 1).lhs.getInterval().getEnd().getMillis(),
            "timeline objects must be provided in order"
        );
      }

      PartitionChunk<DataSegment> firstChunk = Iterables.getFirst(timelineObject.getObject(), null);
      if (firstChunk == null) {
        throw new ISE("Unable to find an underlying interval");
      }
      Interval underlyingInterval = firstChunk.getObject().getInterval();

      for (final PartitionChunk<DataSegment> segment : timelineObject.getObject()) {
        if (!(segment.getObject().getShardSpec() instanceof NoneShardSpec)) {
          return false;
        }

        segments.add(segment.getObject());
        if (segments.count(segment.getObject()) == 1) {
          byteCount += segment.getObject().getSize();
        }
      }

      // Compute new underlying merged interval
      final Interval mergedUnderlyingInterval = getMergedUnderlyingInterval();
      if (mergedUnderlyingInterval == null) {
        timelineObjects.add(Pair.of(timelineObject, underlyingInterval));
      } else {
        final DateTime start = underlyingInterval.getStart().isBefore(mergedUnderlyingInterval.getStart())
                               ? underlyingInterval.getStart()
                               : mergedUnderlyingInterval.getStart();

        final DateTime end = underlyingInterval.getEnd().isAfter(mergedUnderlyingInterval.getEnd())
                             ? underlyingInterval.getEnd()
                             : mergedUnderlyingInterval.getEnd();

        timelineObjects.add(Pair.of(timelineObject, new Interval(start, end)));
      }

      return true;
    }

    public Interval getMergedTimelineInterval()
    {
      if (timelineObjects.isEmpty()) {
        return null;
      } else {
        return new Interval(
            timelineObjects.get(0).lhs.getInterval().getStart(),
            timelineObjects.get(timelineObjects.size() - 1).lhs.getInterval().getEnd()
        );
      }
    }

    public Interval getMergedUnderlyingInterval()
    {
      if (timelineObjects.isEmpty()) {
        return null;
      } else {
        return timelineObjects.get(timelineObjects.size() - 1).rhs;
      }
    }

    public long getByteCount()
    {
      return byteCount;
    }

    public int getSegmentCount()
    {
      return timelineObjects.size();
    }

    /**
     * Does this set of segments fully cover union(all segment intervals)?
     *
     * @return true if this set is complete
     */
    public boolean isComplete()
    {
      return timelineObjects.size() == 0 || getMergedTimelineInterval().equals(getMergedUnderlyingInterval());
    }

    /**
     * Remove timelineObjects from this holder until we have a complete set with total size <= maxSize.
     *
     * @return number of timeline object holders removed
     */
    public int backtrack(long maxSize)
    {
      Preconditions.checkArgument(maxSize >= 0, "maxSize >= 0");

      int removed = 0;
      while (!isComplete() || byteCount > maxSize) {
        removed++;

        final TimelineObjectHolder<String, DataSegment> removedHolder = timelineObjects.remove(
            timelineObjects.size()
            - 1
        ).lhs;
        for (final PartitionChunk<DataSegment> segment : removedHolder.getObject()) {
          segments.remove(segment.getObject());
          if (segments.count(segment.getObject()) == 0) {
            byteCount -= segment.getObject().getSize();
          }
        }
      }

      return removed;
    }
  }
}
