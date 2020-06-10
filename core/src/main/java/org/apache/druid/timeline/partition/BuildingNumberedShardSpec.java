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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * This is a special shardSpec which is temporarily used during batch ingestion. In Druid, there is a concept
 * of core partition set which is a set of segments atomically becoming queryable together in Brokers. The core
 * partition set is represented as a range of partitionIds. For {@link NumberedShardSpec}, the core partition set
 * is [0, {@link NumberedShardSpec#partitions}).
 *
 * The NumberedShardSpec is used for dynamic partitioning which is based on the number of rows in each segment.
 * In streaming ingestion, the core partition set size cannot be determined since it's impossible to know how many
 * segments will be created per time chunk. However, in batch ingestion with time chunk locking, the core partition
 * set is the set of segments created by an initial task or an overwriting task. Since the core partition set is
 * determined when the task publishes segments at the end, the task postpones creating proper NumberedShardSpec
 * until the end.
 *
 * This shardSpec is used for such use case. A non-appending batch task can use this shardSpec until it publishes
 * segments at last. When it publishes segments, it should convert the shardSpec of those segments to NumberedShardSpec.
 * See {@code SegmentPublisherHelper#annotateShardSpec} for converting to NumberedShardSpec. Note that, when
 * the segment lock is used, the Overlord coordinates the segment allocation and this class is never used. Instead,
 * the task sends {@link PartialShardSpec} to the Overlord to allocate a new segment. The result segment could have
 * either a {@link ShardSpec} (for root generation segments) or an {@link OverwriteShardSpec} (for non-root
 * generation segments).
 *
 * This class should be Jackson-serializable as the subtasks can send it to the parallel task in parallel ingestion.
 *
 * Finally, this shardSpec has only partitionId which is same as {@link LinearShardSpec}. The difference between
 * them is this shardSpec should never be published and so never be used in other places such as Broker timeline.
 *
 * @see NumberedShardSpec
 */
public class BuildingNumberedShardSpec implements ShardSpec
{
  private final int partitionId;

  @JsonCreator
  public BuildingNumberedShardSpec(int partitionId)
  {
    Preconditions.checkArgument(partitionId >= 0, "partitionId >= 0");
    this.partitionId = partitionId;
  }

  public NumberedShardSpec toNumberedShardSpec(int numTotalPartitions)
  {
    return new NumberedShardSpec(partitionId, numTotalPartitions);
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    // This method can be called in AppenderatorImpl to create a sinkTimeline.
    // The sinkTimeline doesn't seem in use in batch ingestion, let's set 'chunks' to 0 for now.
    return new NumberedPartitionChunk<>(partitionId, 0, obj);
  }

  @JsonProperty("partitionId")
  @Override
  public int getPartitionNum()
  {
    return partitionId;
  }

  @Override
  public ShardSpecLookup getLookup(List<ShardSpec> shardSpecs)
  {
    return NumberedShardSpec.createLookup(shardSpecs);
  }

  // The below methods are used on the query side, and so must not be called for this shardSpec.

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getDomainDimensions()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isCompatible(Class<? extends ShardSpec> other)
  {
    throw new UnsupportedOperationException();
  }
}
