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

import com.google.common.collect.Ordering;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;

import java.util.HashSet;
import java.util.Set;

public class TestUsedSegmentChecker implements UsedSegmentChecker
{
  private final AppenderatorTester appenderatorTester;

  public TestUsedSegmentChecker(AppenderatorTester appenderatorTester)
  {
    this.appenderatorTester = appenderatorTester;
  }

  @Override
  public Set<DataSegment> findUsedSegments(Set<SegmentIdWithShardSpec> identifiers)
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    VersionedIntervalTimeline.addSegments(timeline, appenderatorTester.getPushedSegments().iterator());

    final Set<DataSegment> retVal = new HashSet<>();
    for (SegmentIdWithShardSpec identifier : identifiers) {
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.lookup(identifier.getInterval())) {
        for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
          if (identifiers.contains(SegmentIdWithShardSpec.fromDataSegment(chunk.getObject()))) {
            retVal.add(chunk.getObject());
          }
        }
      }
    }

    return retVal;
  }
}
