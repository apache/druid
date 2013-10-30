package com.metamx.druid.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.druid.Query;
import com.metamx.druid.TimelineObjectHolder;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.druid.query.segment.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: himadri
 * Date: 10/30/13
 * Time: 1:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class DruidUtils
{
  public static <T> Set<Pair<ServerSelector, SegmentDescriptor>> getSegments(
      Query<T> query,
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView
  )
  {
    Set<Pair<ServerSelector, SegmentDescriptor>> segments = Sets.newLinkedHashSet();
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    VersionedIntervalTimeline<String, ServerSelector> timeline = serverView.getTimeline(query.getDataSource());
    if (timeline == null) {
      return null;
    }

    List<TimelineObjectHolder<String, ServerSelector>> serversLookup = Lists.newLinkedList();
    for (Interval interval : query.getIntervals()) {
      serversLookup.addAll(timeline.lookup(interval));
    }
    final List<TimelineObjectHolder<String, ServerSelector>> filteredServersLookup =
        toolChest.filterSegments(query, serversLookup);

    for (TimelineObjectHolder<String, ServerSelector> holder : filteredServersLookup) {
      for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
        ServerSelector selector = chunk.getObject();
        final SegmentDescriptor descriptor = new SegmentDescriptor(
            holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
        );

        segments.add(Pair.of(selector, descriptor));
      }
    }

    return segments;
  }

}
