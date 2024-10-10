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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentTimeline;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestPublishedSegmentRetriever implements PublishedSegmentRetriever
{
  private final List<DataSegment> pushedSegments;

  public TestPublishedSegmentRetriever(List<DataSegment> pushedSegments)
  {
    this.pushedSegments = pushedSegments;
  }

  @Override
  public Set<DataSegment> findPublishedSegments(Set<SegmentId> identifiers)
  {
    final SegmentTimeline timeline = SegmentTimeline.forSegments(pushedSegments);
    final Set<DataSegment> retVal = new HashSet<>();
    for (SegmentId segmentId : identifiers) {
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.lookup(segmentId.getInterval())) {
        for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
          if (identifiers.contains(chunk.getObject().getId())) {
            retVal.add(chunk.getObject());
          }
        }
      }
    }

    return retVal;
  }
}
