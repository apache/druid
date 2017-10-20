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

package io.druid.indexing.common.task;

import com.google.common.base.Preconditions;
import io.druid.java.util.common.guava.Comparators;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompactionTaskUtils
{
  public static List<TimelineObjectHolder<String, DataSegment>> toTimelineSegments(
      List<DataSegment> usedSegments,
      Interval interval
  ) throws IOException, SegmentLoadingException
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(
        Comparators.naturalNullsFirst()
    );

    for (DataSegment segment : usedSegments) {
      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
    }
    return timeline.lookup(interval);
  }

  public static List<QueryableIndex> loadSegments(
      List<TimelineObjectHolder<String, DataSegment>> timelineSegments,
      Map<DataSegment, File> segmentFileMap,
      IndexIO indexIO
  ) throws IOException
  {
    final List<QueryableIndex> segments = new ArrayList<>();

    for (TimelineObjectHolder<String, DataSegment> timelineSegment : timelineSegments) {
      final PartitionHolder<DataSegment> partitionHolder = timelineSegment.getObject();
      for (PartitionChunk<DataSegment> chunk : partitionHolder) {
        final DataSegment segment = chunk.getObject();
        segments.add(
            indexIO.loadIndex(
                Preconditions.checkNotNull(segmentFileMap.get(segment), "File for segment %s", segment.getIdentifier())
            )
        );
      }
    }

    return segments;
  }

  private CompactionTaskUtils() {}
}
