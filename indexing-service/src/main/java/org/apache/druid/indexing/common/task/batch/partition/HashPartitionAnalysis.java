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
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.HashBucketShardSpec;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HashPartitionAnalysis implements CompletePartitionAnalysis<Integer, HashedPartitionsSpec>
{
  /**
   * Key is the time ranges for the primary partitioning.
   * Value is the number of partitions per time range for the secondary partitioning
   */
  private final Map<Interval, Integer> intervalToNumBuckets = new HashMap<>();
  private final HashedPartitionsSpec partitionsSpec;

  public HashPartitionAnalysis(HashedPartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
  }

  @Override
  public HashedPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  public void updateBucket(Interval interval, Integer bucketAnalysis)
  {
    intervalToNumBuckets.put(interval, bucketAnalysis);
  }

  @Override
  public Integer getBucketAnalysis(Interval interval)
  {
    final Integer bucketAnalysis = intervalToNumBuckets.get(interval);
    if (bucketAnalysis == null) {
      throw new IAE("Missing bucket analysis for interval[%s]", interval);
    } else {
      return bucketAnalysis;
    }
  }

  @Override
  public Set<Interval> getAllIntervalsToIndex()
  {
    return Collections.unmodifiableSet(intervalToNumBuckets.keySet());
  }

  @Override
  public int getNumTimePartitions()
  {
    return intervalToNumBuckets.size();
  }

  public void forEach(BiConsumer<Interval, Integer> consumer)
  {
    intervalToNumBuckets.forEach(consumer);
  }

  @Override
  public Map<Interval, List<BucketNumberedShardSpec<?>>> createBuckets(TaskToolbox toolbox)
  {
    final Map<Interval, List<BucketNumberedShardSpec<?>>> intervalToLookup = Maps.newHashMapWithExpectedSize(
        intervalToNumBuckets.size()
    );
    forEach((interval, numBuckets) -> {
      final List<BucketNumberedShardSpec<?>> buckets = IntStream
          .range(0, numBuckets)
          .mapToObj(i -> new HashBucketShardSpec(
              i,
              numBuckets,
              partitionsSpec.getPartitionDimensions(),
              toolbox.getJsonMapper()
          ))
          .collect(Collectors.toList());
      intervalToLookup.put(interval, buckets);
    });
    return intervalToLookup;
  }
}
