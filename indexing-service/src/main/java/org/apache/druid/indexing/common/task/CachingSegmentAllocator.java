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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTask.ShardSpecs;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// TODO: caching??
public abstract class CachingSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final TaskToolbox toolbox;
  private final String taskId;
  private final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec;
  @Nullable
  private final ShardSpecs shardSpecs;

  // sequenceName -> segmentId
  private final Map<String, SegmentIdWithShardSpec> sequenceNameToSegmentId;

  public CachingSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec
  ) throws IOException
  {
    this.toolbox = toolbox;
    this.taskId = taskId;
    this.allocateSpec = allocateSpec;
    this.sequenceNameToSegmentId = new HashMap<>();

    final Map<Interval, List<SegmentIdWithShardSpec>> intervalToIds = getIntervalToSegmentIds();
    final Map<Interval, List<ShardSpec>> shardSpecMap = new HashMap<>();

    for (Map.Entry<Interval, List<SegmentIdWithShardSpec>> entry : intervalToIds.entrySet()) {
      final Interval interval = entry.getKey();
      final List<SegmentIdWithShardSpec> idsPerInterval = intervalToIds.get(interval);

      for (SegmentIdWithShardSpec segmentIdentifier : idsPerInterval) {
        shardSpecMap.computeIfAbsent(interval, k -> new ArrayList<>()).add(segmentIdentifier.getShardSpec());
        // The shardSpecs for partitinoing and publishing can be different if isExtendableShardSpecs = true.
        sequenceNameToSegmentId.put(getSequenceName(interval, segmentIdentifier.getShardSpec()), segmentIdentifier);
      }
    }
    shardSpecs = new ShardSpecs(shardSpecMap);
  }

  abstract Map<Interval, List<SegmentIdWithShardSpec>> getIntervalToSegmentIds() throws IOException;

  TaskToolbox getToolbox()
  {
    return toolbox;
  }

  String getTaskId()
  {
    return taskId;
  }

  Map<Interval, Pair<ShardSpecFactory, Integer>> getAllocateSpec()
  {
    return allocateSpec;
  }

  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  )
  {
    return sequenceNameToSegmentId.get(sequenceName);
  }

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    return getSequenceName(interval, shardSpecs.getShardSpec(interval, inputRow));
  }

  /**
   * Create a sequence name from the given shardSpec and interval. The shardSpec must be the original one before calling
   * {@link #makeShardSpec(ShardSpec, int)} to apply the proper partitioning.
   *
   * See {@link org.apache.druid.timeline.partition.HashBasedNumberedShardSpec} as an example of partitioning.
   */
  private String getSequenceName(Interval interval, ShardSpec shardSpec)
  {
    return StringUtils.format("%s_%s_%d", taskId, interval, shardSpec.getPartitionNum());
  }
}
