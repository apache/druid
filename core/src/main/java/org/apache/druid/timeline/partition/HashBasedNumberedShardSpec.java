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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class HashBasedNumberedShardSpec extends NumberedShardSpec
{
  static final List<String> DEFAULT_PARTITION_DIMENSIONS = ImmutableList.of();

  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();

  private final int bucketId;
  /**
   * Number of hash buckets
   */
  private final int numBuckets;
  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final List<String> partitionDimensions;

  @JsonCreator
  public HashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,    // partitionId
      @JsonProperty("partitions") int partitions,        // core partition set size
      @JsonProperty("bucketId") @Nullable Integer bucketId, // nullable for backward compatibility
      @JsonProperty("numBuckets") @Nullable Integer numBuckets, // nullable for backward compatibility
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions);
    // Use partitionId as bucketId if it's missing.
    this.bucketId = bucketId == null ? partitionNum : bucketId;
    // If numBuckets is missing, assume that any hash bucket is not empty.
    // Use the core partition set size as the number of buckets.
    this.numBuckets = numBuckets == null ? partitions : numBuckets;
    this.jsonMapper = jsonMapper;
    this.partitionDimensions = partitionDimensions == null ? DEFAULT_PARTITION_DIMENSIONS : partitionDimensions;
  }

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

  @JsonProperty("partitionDimensions")
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return (((long) hash(timestamp, inputRow)) - bucketId) % numBuckets == 0;
  }

  /**
   * This method calculates the hash based on whether {@param partitionDimensions} is null or not.
   * If yes, then both {@param timestamp} and dimension columns in {@param inputRow} are used {@link Rows#toGroupKey}
   * Or else, columns in {@param partitionDimensions} are used
   *
   * @param timestamp should be bucketed with query granularity
   * @param inputRow row from input data
   *
   * @return hash value
   */
  protected int hash(long timestamp, InputRow inputRow)
  {
    return hash(jsonMapper, partitionDimensions, timestamp, inputRow);
  }

  public static int hash(ObjectMapper jsonMapper, List<String> partitionDimensions, long timestamp, InputRow inputRow)
  {
    final List<Object> groupKey = getGroupKey(partitionDimensions, timestamp, inputRow);
    try {
      return hash(jsonMapper, groupKey);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  static List<Object> getGroupKey(final List<String> partitionDimensions, final long timestamp, final InputRow inputRow)
  {
    if (partitionDimensions.isEmpty()) {
      return Rows.toGroupKey(timestamp, inputRow);
    } else {
      return Lists.transform(partitionDimensions, inputRow::getDimension);
    }
  }

  @VisibleForTesting
  public static int hash(ObjectMapper jsonMapper, List<Object> objects) throws JsonProcessingException
  {
    return HASH_FUNCTION.hashBytes(jsonMapper.writeValueAsBytes(objects)).asInt();
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    return createHashLookup(jsonMapper, partitionDimensions, shardSpecs, numBuckets);
  }

  static ShardSpecLookup createHashLookup(
      ObjectMapper jsonMapper,
      List<String> partitionDimensions,
      List<? extends ShardSpec> shardSpecs,
      int numBuckets
  )
  {
    return (long timestamp, InputRow row) -> {
      int index = Math.abs(hash(jsonMapper, partitionDimensions, timestamp, row) % numBuckets);
      return shardSpecs.get(index);
    };
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
    if (!super.equals(o)) {
      return false;
    }
    HashBasedNumberedShardSpec that = (HashBasedNumberedShardSpec) o;
    return bucketId == that.bucketId &&
           numBuckets == that.numBuckets &&
           Objects.equals(partitionDimensions, that.partitionDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), bucketId, numBuckets, partitionDimensions);
  }

  @Override
  public String toString()
  {
    return "HashBasedNumberedShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getNumCorePartitions() +
           ", bucketId=" + bucketId +
           ", numBuckets=" + numBuckets +
           ", partitionDimensions=" + partitionDimensions +
           '}';
  }
}
