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

import com.google.common.collect.Maps;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.Partitions;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Allocates all necessary range-partitioned segments locally at the beginning and reuses them.
 *
 * @see CachingLocalSegmentAllocatorHelper
 */
public class RangePartitionCachingLocalSegmentAllocator implements IndexTaskSegmentAllocator
{
  private final String dataSource;
  private final String partitionDimension;
  private final Map<Interval, Partitions> intervalsToPartitions;
  private final IndexTaskSegmentAllocator delegate;

  public RangePartitionCachingLocalSegmentAllocator(
      TaskToolbox toolbox,
      String taskId,
      String supervisorTaskId,
      String dataSource,
      String partitionDimension,
      Map<Interval, Partitions> intervalsToPartitions
  ) throws IOException
  {
    this.dataSource = dataSource;
    this.partitionDimension = partitionDimension;
    this.intervalsToPartitions = intervalsToPartitions;

    this.delegate = new CachingLocalSegmentAllocatorHelper(
        toolbox,
        taskId,
        supervisorTaskId,
        this::getIntervalToSegmentIds
    );
  }

  private Map<Interval, List<SegmentIdWithShardSpec>> getIntervalToSegmentIds(Function<Interval, String> versionFinder)
  {
    Map<Interval, List<SegmentIdWithShardSpec>> intervalToSegmentIds =
        Maps.newHashMapWithExpectedSize(intervalsToPartitions.size());

    intervalsToPartitions.forEach(
        (interval, partitions) ->
            intervalToSegmentIds.put(
                interval,
                translatePartitions(interval, partitions, versionFinder)
            )
    );

    return intervalToSegmentIds;
  }

  /**
   * Translate {@link org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution} partititions
   * into the corresponding {@link org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec} with segment id.
   */
  private List<SegmentIdWithShardSpec> translatePartitions(
      Interval interval,
      Partitions partitions,
      Function<Interval, String> versionFinder
  )
  {
    if (partitions.isEmpty()) {
      return Collections.emptyList();
    }

    String[] uniquePartitions = partitions.stream().distinct().toArray(String[]::new);
    int numUniquePartition = uniquePartitions.length;

    List<SegmentIdWithShardSpec> segmentIds =
        IntStream.range(0, numUniquePartition - 1)
                 .mapToObj(i -> createSegmentIdWithShardSpec(
                     interval,
                     versionFinder.apply(interval),
                     uniquePartitions[i],
                     uniquePartitions[i + 1],
                     i
                 ))
                 .collect(Collectors.toCollection(ArrayList::new));
    segmentIds.add(
        createLastSegmentIdWithShardSpec(
            interval,
            versionFinder.apply(interval),
            uniquePartitions[numUniquePartition - 1],
            segmentIds.size()
        )
    );

    return segmentIds;
  }

  private SegmentIdWithShardSpec createLastSegmentIdWithShardSpec(
      Interval interval,
      String version,
      String partitionStart,
      int partitionNum
  )
  {
    return createSegmentIdWithShardSpec(interval, version, partitionStart, null, partitionNum);
  }

  private SegmentIdWithShardSpec createSegmentIdWithShardSpec(
      Interval interval,
      String version,
      String partitionStart,
      @Nullable String partitionEnd,
      int partitionNum
  )
  {
    // The shardSpec created here will be reused in PartialGenericSegmentMergeTask. This is ok because
    // all PartialSegmentGenerateTasks create the same set of segmentIds (and thus shardSpecs).
    return new SegmentIdWithShardSpec(
        dataSource,
        interval,
        version,
        new SingleDimensionShardSpec(
            partitionDimension,
            partitionStart,
            partitionEnd,
            partitionNum
        )
    );
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
