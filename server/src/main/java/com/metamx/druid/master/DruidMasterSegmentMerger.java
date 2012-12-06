/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.master;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import com.metamx.common.Pair;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.netflix.curator.x.discovery.ServiceProvider;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class DruidMasterSegmentMerger implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterSegmentMerger.class);

  private final MergerClient mergerClient;

  public DruidMasterSegmentMerger(MergerClient mergerClient)
  {
    this.mergerClient = mergerClient;
  }

  public DruidMasterSegmentMerger(ObjectMapper jsonMapper, ServiceProvider serviceProvider)
  {
    this.mergerClient = new HttpMergerClient(
        HttpClientInit.createClient(
            HttpClientConfig.builder().withNumConnections(1).build(),
            new Lifecycle()
        ),
        new ToStringResponseHandler(Charsets.UTF_8),
        jsonMapper,
        serviceProvider
    );
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    MasterStats stats = new MasterStats();
    Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources = Maps.newHashMap();

    // Find serviced segments by using a timeline
    for (DataSegment dataSegment : params.getAvailableSegments()) {
      VersionedIntervalTimeline<String, DataSegment> timeline = dataSources.get(dataSegment.getDataSource());
      if (timeline == null) {
        timeline = new VersionedIntervalTimeline<String, DataSegment>(Ordering.<String>natural());
        dataSources.put(dataSegment.getDataSource(), timeline);
      }
      timeline.add(
          dataSegment.getInterval(),
          dataSegment.getVersion(),
          dataSegment.getShardSpec().createChunk(dataSegment)
      );
    }

    // Find segments to merge
    for (final Map.Entry<String, VersionedIntervalTimeline<String, DataSegment>> entry : dataSources.entrySet()) {
      // Get serviced segments from the timeline
      VersionedIntervalTimeline<String, DataSegment> timeline = entry.getValue();
      List<TimelineObjectHolder<String, DataSegment>> timelineObjects =
          timeline.lookup(new Interval(new DateTime(0), new DateTime("3000-01-01")));

      // Accumulate timelineObjects greedily until we reach our limits, then backtrack to the maximum complete set
      SegmentsToMerge segmentsToMerge = new SegmentsToMerge();

      for(int i = 0 ; i < timelineObjects.size() ; i++) {

        segmentsToMerge.add(timelineObjects.get(i));

        if (segmentsToMerge.getByteCount() > params.getMergeBytesLimit()
            || segmentsToMerge.getSegmentCount() >= params.getMergeSegmentsLimit())
        {
          i -= segmentsToMerge.backtrack(params.getMergeBytesLimit());

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
      segmentsToMerge.backtrack(params.getMergeBytesLimit());
      if (segmentsToMerge.getSegmentCount() > 1) {
        stats.addToGlobalStat("mergedCount", mergeSegments(segmentsToMerge, entry.getKey()));
      }
    }

    return params.buildFromExisting()
                 .withMasterStats(stats)
                 .build();
  }

  /**
   * Issue merge request for some list of segments.
   * @return number of segments merged
   */
  private int mergeSegments(SegmentsToMerge segmentsToMerge, String dataSource)
  {
    final List<DataSegment> segments = segmentsToMerge.getSegments();
    final List<String> segmentNames = Lists.transform(
        segments,
        new Function<DataSegment, String>()
        {
          @Override
          public String apply(DataSegment input)
          {
            return input.getIdentifier();
          }
        }
    );

    log.info("[%s] Found %d segments to merge %s", dataSource, segments.size(), segmentNames);

    try {
      mergerClient.runRequest(dataSource, segments);
    }
    catch (Exception e) {
      log.error(
          e,
          "[%s] Merging error for segments [%s]",
          dataSource,
          segmentNames
      );
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
      this.timelineObjects = Lists.newArrayList();
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

    public void add(TimelineObjectHolder<String, DataSegment> timelineObject)
    {
      final Interval timelineObjectInterval = timelineObject.getInterval();
      final Interval underlyingInterval = timelineObject.getObject().getChunk(0).getObject().getInterval();

      if (timelineObjects.size() > 0) {
        Preconditions.checkArgument(
            timelineObjectInterval.getStart().getMillis() >=
            timelineObjects.get(timelineObjects.size() - 1).lhs.getInterval().getEnd().getMillis(),
            "timeline objects must be provided in order"
        );
      }

      for(final PartitionChunk<DataSegment> segment : timelineObject.getObject()) {
        segments.add(segment.getObject());
        if(segments.count(segment.getObject()) == 1) {
          byteCount += segment.getObject().getSize();
        }
      }

      // Compute new underlying merged interval
      final Interval mergedUnderlyingInterval = getMergedUnderlyingInterval();
      if(mergedUnderlyingInterval == null) {
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
    }

    public Interval getMergedTimelineInterval()
    {
      if(timelineObjects.isEmpty()) {
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
      if(timelineObjects.isEmpty()) {
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
     * @return true if this set is complete
     */
    public boolean isComplete()
    {
      return timelineObjects.size() == 0 || getMergedTimelineInterval().equals(getMergedUnderlyingInterval());
    }

    /**
     * Remove timelineObjects from this holder until we have a complete set with total size <= maxSize.
     * @return number of timeline object holders removed
     */
    public int backtrack(long maxSize)
    {
      Preconditions.checkArgument(maxSize >= 0, "maxSize >= 0");

      int removed = 0;
      while (!isComplete() || byteCount > maxSize) {
        removed ++;

        final TimelineObjectHolder<String, DataSegment> removedHolder = timelineObjects.remove(timelineObjects.size() - 1).lhs;
        for(final PartitionChunk<DataSegment> segment : removedHolder.getObject()) {
          segments.remove(segment.getObject());
          if(segments.count(segment.getObject()) == 0) {
            byteCount -= segment.getObject().getSize();
          }
        }
      }

      return removed;
    }
  }
}
