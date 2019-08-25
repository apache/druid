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

public class HashBasedNumberedShardSpec extends NumberedShardSpec
{
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_32();
  private static final List<String> DEFAULT_PARTITION_DIMENSIONS = ImmutableList.of();

  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final List<String> partitionDimensions;

  @JsonCreator
  public HashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,    // partitionId
      @JsonProperty("partitions") int partitions,        // # of partitions
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions);
    this.jsonMapper = jsonMapper;
    this.partitionDimensions = partitionDimensions == null ? DEFAULT_PARTITION_DIMENSIONS : partitionDimensions;
  }

  @JsonProperty("partitionDimensions")
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public boolean isCompatible(Class<? extends ShardSpec> other)
  {
    return other == HashBasedNumberedShardSpec.class;
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return (((long) hash(timestamp, inputRow)) - getPartitionNum()) % getPartitions() == 0;
  }

  protected int hash(long timestamp, InputRow inputRow)
  {
    final List<Object> groupKey = getGroupKey(timestamp, inputRow);
    try {
      return hash(jsonMapper, groupKey);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  List<Object> getGroupKey(final long timestamp, final InputRow inputRow)
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
  public String toString()
  {
    return "HashBasedNumberedShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getPartitions() +
           ", partitionDimensions=" + getPartitionDimensions() +
           '}';
  }

  @Override
  public ShardSpecLookup getLookup(final List<ShardSpec> shardSpecs)
  {
    return (long timestamp, InputRow row) -> {
      int index = Math.abs(hash(timestamp, row) % getPartitions());
      return shardSpecs.get(index);
    };
  }
}
