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
import org.apache.druid.com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * See {@link BucketNumberedShardSpec} for how this class is used.
 *
 * @see BuildingHashBasedNumberedShardSpec
 */
public class HashBucketShardSpec implements BucketNumberedShardSpec<BuildingHashBasedNumberedShardSpec>
{
  public static final String TYPE = "bucket_hash";

  private final int bucketId;
  private final int numBuckets;
  private final List<String> partitionDimensions;
  private final HashPartitionFunction partitionFunction;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public HashBucketShardSpec(
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("partitionDimensions") List<String> partitionDimensions,
      @JsonProperty("partitionFunction") HashPartitionFunction partitionFunction,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.bucketId = bucketId;
    this.numBuckets = numBuckets;
    this.partitionDimensions = partitionDimensions == null
                               ? HashBasedNumberedShardSpec.DEFAULT_PARTITION_DIMENSIONS
                               : partitionDimensions;
    this.partitionFunction = Preconditions.checkNotNull(partitionFunction, "partitionFunction");
    this.jsonMapper = jsonMapper;
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

  @JsonProperty
  public HashPartitionFunction getPartitionFunction()
  {
    return partitionFunction;
  }

  @Override
  public BuildingHashBasedNumberedShardSpec convert(int partitionId)
  {
    return new BuildingHashBasedNumberedShardSpec(
        partitionId,
        bucketId,
        numBuckets,
        partitionDimensions,
        partitionFunction,
        jsonMapper
    );
  }

  @Override
  public ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
  {
    return new HashPartitioner(
        jsonMapper,
        partitionFunction,
        partitionDimensions,
        numBuckets
    ).createHashLookup(shardSpecs);
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
    HashBucketShardSpec that = (HashBucketShardSpec) o;
    return bucketId == that.bucketId &&
           numBuckets == that.numBuckets &&
           Objects.equals(partitionDimensions, that.partitionDimensions) &&
           partitionFunction == that.partitionFunction;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucketId, numBuckets, partitionDimensions, partitionFunction);
  }

  @Override
  public String toString()
  {
    return "HashBucketShardSpec{" +
           "bucketId=" + bucketId +
           ", numBuckets=" + numBuckets +
           ", partitionDimensions=" + partitionDimensions +
           ", partitionFunction=" + partitionFunction +
           '}';
  }
}
