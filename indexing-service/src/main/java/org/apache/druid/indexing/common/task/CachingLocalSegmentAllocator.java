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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.task.IndexTask.ShardSpecs;
import org.apache.druid.java.util.common.ISE;
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
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Allocates all necessary segments locally at the beginning and reuse them.
 */
public class CachingLocalSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final TaskToolbox toolbox;
  private final String taskId;
  private final String dataSource;
  private final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec;
  @Nullable
  private final ShardSpecs shardSpecs;

  // sequenceName -> segmentId
  private final Map<String, SegmentIdWithShardSpec> sequenceNameToSegmentId;

  public CachingLocalSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      String dataSource,
      Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec
  ) throws IOException
  {
    this.toolbox = toolbox;
    this.taskId = taskId;
    this.dataSource = dataSource;
    this.allocateSpec = allocateSpec;
    this.sequenceNameToSegmentId = new HashMap<>();

    final Map<Interval, List<SegmentIdWithShardSpec>> intervalToIds = getIntervalToSegmentIds();
    final Map<Interval, List<ShardSpec>> shardSpecMap = new HashMap<>();

    for (Map.Entry<Interval, List<SegmentIdWithShardSpec>> entry : intervalToIds.entrySet()) {
      final Interval interval = entry.getKey();
      final List<SegmentIdWithShardSpec> idsPerInterval = intervalToIds.get(interval);

      for (SegmentIdWithShardSpec segmentIdentifier : idsPerInterval) {
        shardSpecMap.computeIfAbsent(interval, k -> new ArrayList<>()).add(segmentIdentifier.getShardSpec());
        // The shardSpecs for partitioning and publishing can be different if isExtendableShardSpecs = true.
        sequenceNameToSegmentId.put(getSequenceName(interval, segmentIdentifier.getShardSpec()), segmentIdentifier);
      }
    }
    shardSpecs = new ShardSpecs(shardSpecMap);
  }

  private Map<Interval, List<SegmentIdWithShardSpec>> getIntervalToSegmentIds() throws IOException
  {
    final Map<Interval, String> intervalToVersion = getToolbox().getTaskActionClient()
                                                                .submit(new LockListAction())
                                                                .stream()
                                                                .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));
    final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec = getAllocateSpec();
    final Map<Interval, List<SegmentIdWithShardSpec>> intervalToSegmentIds = new HashMap<>(allocateSpec.size());
    for (Entry<Interval, Pair<ShardSpecFactory, Integer>> entry : allocateSpec.entrySet()) {
      final Interval interval = entry.getKey();
      final ShardSpecFactory shardSpecFactory = entry.getValue().lhs;
      final int numSegmentsToAllocate = Preconditions.checkNotNull(
          entry.getValue().rhs,
          "numSegmentsToAllocate for interval[%s]",
          interval
      );

      intervalToSegmentIds.put(
          interval,
          IntStream.range(0, numSegmentsToAllocate)
                   .mapToObj(i -> new SegmentIdWithShardSpec(
                       getDataSource(),
                       interval,
                       findVersion(intervalToVersion, interval),
                       shardSpecFactory.create(getToolbox().getObjectMapper(), i)
                   ))
                   .collect(Collectors.toList())
      );
    }
    return intervalToSegmentIds;
  }

  private static String findVersion(Map<Interval, String> intervalToVersion, Interval interval)
  {
    return intervalToVersion.entrySet().stream()
                            .filter(entry -> entry.getKey().contains(interval))
                            .map(Entry::getValue)
                            .findFirst()
                            .orElseThrow(() -> new ISE("Cannot find a version for interval[%s]", interval));
  }


  TaskToolbox getToolbox()
  {
    return toolbox;
  }

  String getTaskId()
  {
    return taskId;
  }

  String getDataSource()
  {
    return dataSource;
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
    // Sequence name is based solely on the shardSpec, and there will only be one segment per sequence.
    return getSequenceName(interval, shardSpecs.getShardSpec(interval, inputRow));
  }

  /**
   * Create a sequence name from the given shardSpec and interval.
   *
   * See {@link org.apache.druid.timeline.partition.HashBasedNumberedShardSpec} as an example of partitioning.
   */
  private String getSequenceName(Interval interval, ShardSpec shardSpec)
  {
    return StringUtils.format("%s_%s_%d", taskId, interval, shardSpec.getPartitionNum());
  }
}
