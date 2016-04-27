/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;

import javax.annotation.Nullable;
import java.util.List;

public class HashBasedNumberedShardSpec extends NumberedShardSpec
{
  private static final HashFunction hashFunction = Hashing.murmur3_32();
  private static final List<String> DEFAULT_PARTITION_DIMENSIONS = ImmutableList.of();

  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final List<String> partitionDimensions;

  @JsonCreator
  public HashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
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
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return (((long) hash(timestamp, inputRow)) - getPartitionNum()) % getPartitions() == 0;
  }

  protected int hash(long timestamp, InputRow inputRow)
  {
    final List<Object> groupKey = getGroupKey(timestamp, inputRow);
    try {
      return hashFunction.hashBytes(jsonMapper.writeValueAsBytes(groupKey)).asInt();
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  List<Object> getGroupKey(final long timestamp, final InputRow inputRow)
  {
    if (partitionDimensions.isEmpty()) {
      return Rows.toGroupKey(timestamp, inputRow);
    } else {
      return Lists.transform(partitionDimensions, new Function<String, Object>()
      {
        @Override
        public Object apply(final String dim)
        {
          return inputRow.getDimension(dim);
        }
      });
    }
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
    return new ShardSpecLookup()
    {
      @Override
      public ShardSpec getShardSpec(long timestamp, InputRow row)
      {
        int index = Math.abs(hash(timestamp, row) % getPartitions());
        return shardSpecs.get(index);
      }
    };
  }
}
