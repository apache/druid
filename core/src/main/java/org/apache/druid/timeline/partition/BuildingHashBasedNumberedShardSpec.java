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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * See {@link BuildingShardSpec} for how this class is used.
 *
 * @see HashBasedNumberedShardSpec
 */
public class BuildingHashBasedNumberedShardSpec implements BuildingShardSpec<HashBasedNumberedShardSpec>
{
  public static final String TYPE = "building_hashed";

  private final int partitionId;
  private final int bucketId;
  private final int numBuckets;
  private final List<String> partitionDimensions;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public BuildingHashBasedNumberedShardSpec(
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.partitionId = partitionId;
    this.bucketId = bucketId;
    this.numBuckets = numBuckets;
    this.partitionDimensions = partitionDimensions == null
                               ? HashBasedNumberedShardSpec.DEFAULT_PARTITION_DIMENSIONS
                               : partitionDimensions;
    this.jsonMapper = jsonMapper;
  }

  @JsonProperty("partitionId")
  @Override
  public int getPartitionNum()
  {
    return partitionId;
  }

  @Override
  @JsonProperty
  public int getBucketId()
  {
    return bucketId;
  }

  @JsonProperty
  public int getNumBuckets()
  {
    return numBuckets;
  }

  @JsonProperty
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    // This method can be called in AppenderatorImpl to create a sinkTimeline.
    // The sinkTimeline doesn't seem in use in batch ingestion, let's set 'chunks' to 0 for now.
    // HashBasedNumberedShardSpec is using NumberedPartitionChunk, so we use it here too.
    return new NumberedPartitionChunk<>(partitionId, 0, obj);
  }

  @Override
  public HashBasedNumberedShardSpec convert(int numCorePartitions)
  {
    return new HashBasedNumberedShardSpec(
        partitionId,
        numCorePartitions,
        bucketId,
        numBuckets,
        partitionDimensions,
        jsonMapper
    );
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
    BuildingHashBasedNumberedShardSpec that = (BuildingHashBasedNumberedShardSpec) o;
    return partitionId == that.partitionId &&
           bucketId == that.bucketId &&
           numBuckets == that.numBuckets &&
           Objects.equals(partitionDimensions, that.partitionDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionId, bucketId, numBuckets, partitionDimensions);
  }

  @Override
  public String toString()
  {
    return "BuildingHashBasedNumberedShardSpec{" +
           "partitionId=" + partitionId +
           ", bucketId=" + bucketId +
           ", numBuckets=" + numBuckets +
           ", partitionDimensions=" + partitionDimensions +
           '}';
  }
}
