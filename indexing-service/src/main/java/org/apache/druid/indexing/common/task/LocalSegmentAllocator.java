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

package org.apache.druid.indexing.common.task;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.BuildingNumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * Segment allocator which allocates new segments locally per request.
 */
class LocalSegmentAllocator implements SegmentAllocatorForBatch
{
  private final SegmentAllocator internalAllocator;
  private final SequenceNameFunction sequenceNameFunction;

  LocalSegmentAllocator(TaskToolbox toolbox, String taskId, String dataSource, GranularitySpec granularitySpec) throws IOException
  {
    final Map<Interval, String> intervalToVersion = toolbox
        .getTaskActionClient()
        .submit(new LockListAction())
        .stream()
        .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
    final Map<Interval, MutableInt> counters = Maps.newHashMapWithExpectedSize(intervalToVersion.size());

    internalAllocator = (row, sequenceName, previousSegmentId, skipSegmentLineageCheck) -> {
      final DateTime timestamp = row.getTimestamp();
      Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
      if (!maybeInterval.isPresent()) {
        throw new ISE("Could not find interval for timestamp [%s]", timestamp);
      }

      final Interval interval = maybeInterval.get();
      final String version = intervalToVersion
          .entrySet()
          .stream()
          .filter(entry -> entry.getKey().contains(interval))
          .map(Entry::getValue)
          .findFirst()
          .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));

      final int partitionId = counters.computeIfAbsent(interval, x -> new MutableInt()).getAndIncrement();
      return new SegmentIdWithShardSpec(
          dataSource,
          interval,
          version,
          new BuildingNumberedShardSpec(partitionId)
      );
    };
    sequenceNameFunction = new LinearlyPartitionedSequenceNameFunction(taskId);
  }

  @Nullable
  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return internalAllocator.allocate(row, sequenceName, previousSegmentId, skipSegmentLineageCheck);
  }

  @Override
  public SequenceNameFunction getSequenceNameFunction()
  {
    return sequenceNameFunction;
  }
}
