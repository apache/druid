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

package org.apache.druid.indexing.common.task.batch.partition;

import com.google.common.collect.Maps;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.apache.druid.timeline.partition.RangeBucketShardSpec;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RangePartitionAnalysis
    implements CompletePartitionAnalysis<PartitionBoundaries, SingleDimensionPartitionsSpec>
{
  private final Map<Interval, PartitionBoundaries> intervalToPartitionBoundaries = new HashMap<>();
  private final SingleDimensionPartitionsSpec partitionsSpec;

  public RangePartitionAnalysis(SingleDimensionPartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
  }

  @Override
  public SingleDimensionPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  public void updateBucket(Interval interval, PartitionBoundaries bucketAnalysis)
  {
    intervalToPartitionBoundaries.put(interval, bucketAnalysis);
  }

  @Override
  public PartitionBoundaries getBucketAnalysis(Interval interval)
  {
    final PartitionBoundaries bucketAnalysis = intervalToPartitionBoundaries.get(interval);
    if (bucketAnalysis == null) {
      throw new IAE("Missing bucket analysis for interval[%s]", interval);
    } else {
      return bucketAnalysis;
    }
  }

  @Override
  public Set<Interval> getAllIntervalsToIndex()
  {
    return Collections.unmodifiableSet(intervalToPartitionBoundaries.keySet());
  }

  private void forEach(BiConsumer<Interval, PartitionBoundaries> consumer)
  {
    intervalToPartitionBoundaries.forEach(consumer);
  }

  @Override
  public int getNumTimePartitions()
  {
    return intervalToPartitionBoundaries.size();
  }

  /**
   * Translate {@link PartitionBoundaries} into the corresponding
   * {@link SingleDimensionPartitionsSpec} with segment id.
   */
  private static List<BucketNumberedShardSpec<?>> translatePartitionBoundaries(
      String partitionDimension,
      PartitionBoundaries partitionBoundaries
  )
  {
    if (partitionBoundaries.isEmpty()) {
      return Collections.emptyList();
    }

    return IntStream.range(0, partitionBoundaries.size() - 1)
                    .mapToObj(i -> new RangeBucketShardSpec(
                        i,
                        partitionDimension,
                        partitionBoundaries.get(i),
                        partitionBoundaries.get(i + 1)
                    ))
                    .collect(Collectors.toList());
  }

  @Override
  public Map<Interval, List<BucketNumberedShardSpec<?>>> createBuckets(TaskToolbox toolbox)
  {
    final String partitionDimension = partitionsSpec.getPartitionDimension();
    final Map<Interval, List<BucketNumberedShardSpec<?>>> intervalToSegmentIds = Maps.newHashMapWithExpectedSize(
        getNumTimePartitions()
    );

    forEach((interval, partitionBoundaries) ->
                intervalToSegmentIds.put(
                    interval,
                    translatePartitionBoundaries(partitionDimension, partitionBoundaries)
                )
    );

    return intervalToSegmentIds;
  }
}
