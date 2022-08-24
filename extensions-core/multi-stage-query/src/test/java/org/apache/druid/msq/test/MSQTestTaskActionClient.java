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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.msq.indexing.error.InsertLockPreemptedFaultTest;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MSQTestTaskActionClient implements TaskActionClient
{

  private static final String VERSION = "test";
  private final ObjectMapper mapper;
  private final ConcurrentHashMap<SegmentId, AtomicInteger> segmentIdPartitionIdMap = new ConcurrentHashMap<>();

  public MSQTestTaskActionClient(ObjectMapper mapper)
  {
    this.mapper = mapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    if (taskAction instanceof SegmentAllocateAction) {
      SegmentAllocateAction segmentAllocateAction = (SegmentAllocateAction) taskAction;
      InsertLockPreemptedFaultTest.LockPreemptedHelper.throwIfPreempted();
      Granularity granularity = segmentAllocateAction.getPreferredSegmentGranularity();
      Interval interval;

      if (granularity instanceof PeriodGranularity) {
        PeriodGranularity periodGranularity = (PeriodGranularity) granularity;
        interval = new Interval(
            segmentAllocateAction.getTimestamp().toInstant(),
            periodGranularity.getPeriod()
        );
      } else {
        interval = Intervals.ETERNITY;
      }

      SegmentId segmentId = SegmentId.of(segmentAllocateAction.getDataSource(), interval, VERSION, 0);
      AtomicInteger newPartitionId = segmentIdPartitionIdMap.computeIfAbsent(segmentId, k -> new AtomicInteger(-1));

      return (RetType) new SegmentIdWithShardSpec(
          segmentAllocateAction.getDataSource(),
          interval,
          VERSION,
          segmentAllocateAction.getPartialShardSpec().complete(mapper, newPartitionId.addAndGet(1), 100)
      );
    } else if (taskAction instanceof LockListAction) {
      return (RetType) ImmutableList.of(new TimeChunkLock(
          TaskLockType.EXCLUSIVE,
          "group",
          "ds",
          Intervals.ETERNITY,
          VERSION,
          0
      ));
    } else if (taskAction instanceof RetrieveUsedSegmentsAction) {
      return (RetType) ImmutableSet.of();
    } else {
      return null;
    }
  }
}
