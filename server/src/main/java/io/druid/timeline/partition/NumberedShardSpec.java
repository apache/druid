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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * An extendable linear shard spec containing the information of core partitions.  This class contains two variables of
 * {@link #partitionNum} and {@link #partitions}, which represent the unique id of a partition and the number of core
 * partitions, respectively.  {@link #partitions} simply indicates that the atomic update is regarded as completed when
 * {@link #partitions} partitions are successfully updated, and {@link #partitionNum} can go beyond it when some types
 * of index tasks are trying to append to existing partitions.
 */
public class NumberedShardSpec implements ShardSpec
{
  @JsonIgnore
  final private int partitionNum;

  @JsonIgnore
  final private int partitions;

  @JsonCreator
  public NumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions
  )
  {
    Preconditions.checkArgument(partitionNum >= 0, "partitionNum >= 0");
    Preconditions.checkArgument(partitions >= 0, "partitions >= 0");
    this.partitionNum = partitionNum;
    this.partitions = partitions;
  }

  @JsonProperty("partitionNum")
  @Override
  public int getPartitionNum()
  {
    return partitionNum;
  }

  @Override
  public ShardSpecLookup getLookup(final List<ShardSpec> shardSpecs)
  {
    return new ShardSpecLookup()
    {
      @Override
      public ShardSpec getShardSpec(long timestamp, InputRow row)
      {
        return shardSpecs.get(0);
      }
    };
  }

  @Override
  public Map<String, Range<String>> getDomain()
  {
    return ImmutableMap.of();
  }

  @JsonProperty("partitions")
  public int getPartitions()
  {
    return partitions;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return NumberedPartitionChunk.make(partitionNum, partitions, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "NumberedShardSpec{" +
           "partitionNum=" + partitionNum +
           ", partitions=" + partitions +
           '}';
  }
}
