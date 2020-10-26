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

package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.RangeSet;

import java.util.List;
import java.util.Map;

/**
 * This is one of the special shardSpecs which are temporarily used during batch ingestion. In Druid, there is a
 * concept of core partition set which is a set of segments atomically becoming queryable together in Brokers. The core
 * partition set is represented as a range of partitionIds, i.e., [0, {@link ShardSpec#getNumCorePartitions()}).
 *
 * When you run a batch ingestion task with a non-linear partitioning scheme, the task populates all possible buckets
 * upfront at the beginning (see {@code CachingLocalSegmentAllocator}) and uses them to partition input rows. However,
 * some of the buckets can be empty even after the task consumes all inputs if the data is highly skewed. Since Druid
 * doesn't create empty segments, the partitionId should be dynamically allocated when a bucket is actually in use,
 * so that we can always create the packed core partition set without missing partitionIds.
 *
 * This BucketNumberedShardSpec is used for such use case. The task with a non-linear partitioning scheme uses it
 * to postpone the partitionId allocation until all empty buckets are identified. See
 * {@code ParallelIndexSupervisorTask.groupGenericPartitionLocationsPerPartition} and
 * {@code CachingLocalSegmentAllocator} for parallel and sequential ingestion, respectively.
 *
 * Note that {@link org.apache.druid.timeline.SegmentId} requires the partitionId. Since the segmentId is used
 * everwhere during ingestion, this class should implement {@link #getPartitionNum()} which returns the bucketId.
 * This should be fine because the segmentId is only used to identify each segment until pushing them to deep storage.
 * The bucketId should be enough to uniquely identify each segment. However, when pushing segments to deep storage,
 * the partitionId is used to create the path to store the segment on deep storage
 * ({@link org.apache.druid.segment.loading.DataSegmentPusher#getDefaultStorageDir} which should be correct.
 * As a result, this shardSpec should not be used in pushing segments.
 *
 * This class should be Jackson-serializable as the subtasks can send it to the parallel task in parallel ingestion.
 *
 * This interface doesn't really have to extend {@link ShardSpec}. The only reason is the ShardSpec is used in many
 * places such as {@link org.apache.druid.timeline.DataSegment}, and we have to modify those places to allow other
 * types than ShardSpec which seems pretty invasive. Maybe we could clean up this mess someday in the future.
 *
 * @see BuildingShardSpec
 */
public interface BucketNumberedShardSpec<T extends BuildingShardSpec> extends ShardSpec
{
  int getBucketId();

  T convert(int partitionId);

  @Override
  default <O> PartitionChunk<O> createChunk(O obj)
  {
    // The partitionId (or partitionNum, chunkNumber) is not determined yet. Use bucketId for now.
    return new NumberedPartitionChunk<>(getBucketId(), 0, obj);
  }

  @Override
  default int getPartitionNum()
  {
    // See the class-level Javadoc for returning bucketId here.
    return getBucketId();
  }

  @Override
  default int getNumCorePartitions()
  {
    throw new UnsupportedOperationException();
  }

  // The below methods are used on the query side, and so must not be called for this shardSpec.

  @JsonIgnore
  @Override
  default List<String> getDomainDimensions()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  default boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    throw new UnsupportedOperationException();
  }
}
