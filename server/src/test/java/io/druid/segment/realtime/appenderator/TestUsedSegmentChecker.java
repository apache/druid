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

package io.druid.segment.realtime.appenderator;

import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;

import java.util.Set;

public class TestUsedSegmentChecker implements UsedSegmentChecker
{
  private final AppenderatorTester appenderatorTester;

  public TestUsedSegmentChecker(AppenderatorTester appenderatorTester)
  {
    this.appenderatorTester = appenderatorTester;
  }

  @Override
  public Set<DataSegment> findUsedSegments(Set<SegmentIdentifier> identifiers)
  {
    final VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    for (DataSegment dataSegment : appenderatorTester.getPushedSegments()) {
      timeline.add(
          dataSegment.getInterval(),
          dataSegment.getVersion(),
          dataSegment.getShardSpec().createChunk(dataSegment)
      );
    }

    final Set<DataSegment> retVal = Sets.newHashSet();
    for (SegmentIdentifier identifier : identifiers) {
      for (TimelineObjectHolder<String, DataSegment> holder : timeline.lookup(identifier.getInterval())) {
        for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
          if (identifiers.contains(SegmentIdentifier.fromDataSegment(chunk.getObject()))) {
            retVal.add(chunk.getObject());
          }
        }
      }
    }

    return retVal;
  }
}
