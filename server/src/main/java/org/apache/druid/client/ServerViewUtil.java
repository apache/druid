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

package org.apache.druid.client;

import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.LocatedSegmentDescriptor;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

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
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(datasource);
    final Optional<? extends TimelineLookup<String, ServerSelector>> maybeTimeline = serverView.getTimeline(analysis);
    if (!maybeTimeline.isPresent()) {
      return Collections.emptyList();
    }
    List<LocatedSegmentDescriptor> located = new ArrayList<>();
    for (Interval interval : intervals) {
      for (TimelineObjectHolder<String, ServerSelector> holder : maybeTimeline.get().lookup(interval)) {
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
