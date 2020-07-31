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
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * This is one of the special shardSpecs which are temporarily used during batch ingestion. In Druid, there is a
 * concept of core partition set which is a set of segments atomically becoming queryable together in Brokers. The core
 * partition set is represented as a range of partitionIds, i.e., [0, {@link ShardSpec#getNumCorePartitions()}).
 *
 * In streaming ingestion, the core partition set size cannot be determined since it's impossible to know how many
 * segments will be created per time chunk upfront. However, in batch ingestion with time chunk locking, the core
 * partition set is the set of segments created by an initial task or an overwriting task. Since the core partition
 * set is determined when the task publishes segments at the end, the task postpones creating proper {@link ShardSpec}
 * until the end.
 *
 * This BuildingShardSpec is used for such use case. A non-appending batch task can use this shardSpec until it
 * publishes segments at last. When it publishes segments, it should convert the buildingShardSpec of those segments
 * to a proper shardSpec type {@link T}. See {@code SegmentPublisherHelper#annotateShardSpec} for converting shardSpec.
 * Note that, when the segment lock is used, the Overlord coordinates the segment allocation and this class is never
 * used. Instead, the task sends {@link PartialShardSpec} to the Overlord to allocate a new segment. The result segment
 * could have either a {@link ShardSpec} (for root generation segments) or an {@link OverwriteShardSpec} (for non-root
 * generation segments).
 *
 * This class should be Jackson-serializable as the subtasks can send it to the parallel task in parallel ingestion.
 *
 * This interface doesn't really have to extend {@link ShardSpec}. The only reason is the ShardSpec is used in many
 * places such as {@link org.apache.druid.timeline.DataSegment}, and we have to modify those places to allow other
 * types than ShardSpec which seems pretty invasive. Maybe we could clean up this mess someday in the future.
 *
 * @see BucketNumberedShardSpec
 */
public interface BuildingShardSpec<T extends ShardSpec> extends ShardSpec
{
  int getBucketId();

  T convert(int numCorePartitions);

  @Override
  default int getNumCorePartitions()
  {
    throw new UnsupportedOperationException();
  }

  /**
   * {@link BucketNumberedShardSpec} should be used for shard spec lookup.
   */
  @Override
  default ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
  {
    throw new UnsupportedOperationException();
  }

  // The below methods are used on the query side, and so must not be called for this shardSpec.

  @Override
  default boolean isInChunk(long timestamp, InputRow inputRow)
  {
    throw new UnsupportedOperationException();
  }

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
