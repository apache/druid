/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import io.druid.client.selector.ServerSelector;
import io.druid.query.Query;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Utility class
 */
public class CacheClientUtils
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
      return Collections.EMPTY_SET;
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
