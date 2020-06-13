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

import java.util.Objects;

/**
 * See {@link BuildingShardSpec} for how this class is used.
 *
 * This shardSpec has only partitionId which is same as {@link LinearShardSpec}. The difference between
 * them is this shardSpec should never be published and so never be used in other places such as Broker timeline.
 *
 * @see NumberedShardSpec
 */
public class BuildingNumberedShardSpec implements BuildingShardSpec<NumberedShardSpec>
{
  public static final String TYPE = "building_numbered";

  private final int partitionId;

  @JsonCreator
  public BuildingNumberedShardSpec(@JsonProperty("partitionId") int partitionId)
  {
    Preconditions.checkArgument(partitionId >= 0, "partitionId >= 0");
    this.partitionId = partitionId;
  }

  @Override
  public int getBucketId()
  {
    // This method is currently not called when the shardSpec type is this class.
    throw new UnsupportedOperationException();
  }

  @Override
  public NumberedShardSpec convert(int numTotalPartitions)
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BuildingNumberedShardSpec shardSpec = (BuildingNumberedShardSpec) o;
    return partitionId == shardSpec.partitionId;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionId);
  }

  @Override
  public String toString()
  {
    return "BuildingNumberedShardSpec{" +
           "partitionId=" + partitionId +
           '}';
  }
}
