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
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Objects;

public class SingleDimensionPartialShardSpec implements PartialShardSpec
{
  private final String partitionDimension;
  private final int bucketId;
  @Nullable
  private final String start;
  @Nullable
  private final String end;
  private final int numBuckets;

  @JsonCreator
  public SingleDimensionPartialShardSpec(
      @JsonProperty("partitionDimension") String partitionDimension,
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("start") @Nullable String start,
      @JsonProperty("end") @Nullable String end,
      @JsonProperty("numBuckets") int numBuckets
  )
  {
    this.partitionDimension = partitionDimension;
    this.bucketId = bucketId;
    this.start = start;
    this.end = end;
    this.numBuckets = numBuckets;
  }

  @JsonProperty
  public String getPartitionDimension()
  {
    return partitionDimension;
  }

  @JsonProperty
  public int getBucketId()
  {
    return bucketId;
  }

  @JsonProperty
  @Nullable
  public String getStart()
  {
    return start;
  }

  @JsonProperty
  @Nullable
  public String getEnd()
  {
    return end;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @Override
  public ShardSpec complete(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId)
  {
    // The shardSpec is created by the Overlord.
    // For batch tasks, this code can be executed only with segment locking (forceTimeChunkLock = false).
    // In this mode, you can have 2 or more tasks concurrently ingesting into the same time chunk of
    // the same datasource. Since there is no restriction for those tasks in segment allocation, the
    // allocated IDs for each task can interleave. As a result, the core partition set cannot be
    // represented as a range. We always set 0 for the core partition set size if this is an initial segment.
    return new SingleDimensionShardSpec(
        partitionDimension,
        start,
        end,
        specOfPreviousMaxPartitionId == null ? 0 : specOfPreviousMaxPartitionId.getPartitionNum() + 1,
        specOfPreviousMaxPartitionId == null ? 0 : specOfPreviousMaxPartitionId.getNumCorePartitions()
    );
  }

  @Override
  public ShardSpec complete(ObjectMapper objectMapper, int partitionId)
  {
    // TODO: bucketId and numBuckets should be added to SingleDimensionShardSpec in a follow-up PR.
    return new SingleDimensionShardSpec(
        partitionDimension,
        start,
        end,
        partitionId,
        0
    );
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return SingleDimensionShardSpec.class;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SingleDimensionPartialShardSpec that = (SingleDimensionPartialShardSpec) o;
    return bucketId == that.bucketId &&
           numBuckets == that.numBuckets &&
           Objects.equals(partitionDimension, that.partitionDimension) &&
           Objects.equals(start, that.start) &&
           Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionDimension, bucketId, start, end, numBuckets);
  }
}
