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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;
import java.util.Objects;

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
  private final int partitionNum;

  @JsonIgnore
  private final int partitions;

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
    return (long timestamp, InputRow row) -> shardSpecs.get(0);
  }

  @Override
  public List<String> getDomainDimensions()
  {
    return ImmutableList.of();
  }

  @Override
  public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
  {
    return true;
  }

  @Override
  public boolean isCompatible(Class<? extends ShardSpec> other)
  {
    return other == NumberedShardSpec.class || other == NumberedOverwriteShardSpec.class;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (!(o instanceof NumberedShardSpec)) {
      return false;
    }

    final NumberedShardSpec that = (NumberedShardSpec) o;
    if (partitionNum != that.partitionNum) {
      return false;
    }
    return partitions == that.partitions;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(partitionNum, partitions);
  }
}
