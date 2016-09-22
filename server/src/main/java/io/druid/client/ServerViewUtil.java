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

package io.druid.client;

import com.google.common.collect.Lists;
import io.druid.client.selector.ServerSelector;
import io.druid.query.DataSource;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 */
public class ServerViewUtil
{
  public static List<LocatedSegmentDescriptor> getTargetLocations(
      TimelineServerView serverView,
      String datasource,
      List<Interval> intervals,
      int numCandidates
  )
  {
    return getTargetLocations(serverView, new TableDataSource(datasource), intervals, numCandidates);
  }

  public static List<LocatedSegmentDescriptor> getTargetLocations(
      TimelineServerView serverView,
      DataSource datasource,
      List<Interval> intervals,
      int numCandidates
  )
  {
    TimelineLookup<String, ServerSelector> timeline = serverView.getTimeline(datasource);
    if (timeline == null) {
      return Collections.emptyList();
    }
    List<LocatedSegmentDescriptor> located = Lists.newArrayList();
    for (Interval interval : intervals) {
      for (TimelineObjectHolder<String, ServerSelector> holder : timeline.lookup(interval)) {
        for (PartitionChunk<ServerSelector> chunk : holder.getObject()) {
          ServerSelector selector = chunk.getObject();
          final SegmentDescriptor descriptor = new SegmentDescriptor(
              holder.getInterval(), holder.getVersion(), chunk.getChunkNumber()
          );
          long size = selector.getSegment().getSize();
          List<DruidServerMetadata> candidates = selector.getCandidates(numCandidates);
          located.add(new LocatedSegmentDescriptor(descriptor, size, candidates));
        }
      }
    }
    return located;
  }
}
