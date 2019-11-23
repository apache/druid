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
import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Allocates all necessary hash-partitioned segments locally at the beginning and reuses them.
 *
 * @see CachingLocalSegmentAllocator
 */
public class HashPartitionCachingLocalSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final TaskToolbox toolbox;
  private final String dataSource;
  private final Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec;
  private final IndexTaskSegmentAllocator delegate;

  public HashPartitionCachingLocalSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      String supervisorTaskId,
      String dataSource,
      Map<Interval, Pair<ShardSpecFactory, Integer>> allocateSpec
  ) throws IOException
  {
    this.toolbox = toolbox;
    this.dataSource = dataSource;
    this.allocateSpec = allocateSpec;

    this.delegate = new CachingLocalSegmentAllocator(
        toolbox,
        taskId,
        supervisorTaskId,
        this::getIntervalToSegmentIds
    );
  }

  private Map<Interval, List<SegmentIdWithShardSpec>> getIntervalToSegmentIds(Function<Interval, String> versionFinder)
  {
    final Map<Interval, List<SegmentIdWithShardSpec>> intervalToSegmentIds =
        Maps.newHashMapWithExpectedSize(allocateSpec.size());

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
                       dataSource,
                       interval,
                       versionFinder.apply(interval),
                       shardSpecFactory.create(toolbox.getJsonMapper(), i)
                   ))
                   .collect(Collectors.toList())
      );
    }
    return intervalToSegmentIds;
  }

  @Override
  public String getSequenceName(Interval interval, InputRow inputRow)
  {
    return delegate.getSequenceName(interval, inputRow);
  }

  @Override
  public SegmentIdWithShardSpec allocate(
      InputRow row,
      String sequenceName,
      String previousSegmentId,
      boolean skipSegmentLineageCheck
  ) throws IOException
  {
    return delegate.allocate(row, sequenceName, previousSegmentId, skipSegmentLineageCheck);
  }
}
