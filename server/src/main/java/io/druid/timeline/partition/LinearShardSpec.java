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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import io.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * An extendable linear shard spec.  {@link #partitionNum} represents an unique id of a partition.
 */
public class LinearShardSpec implements ShardSpec
{
  private int partitionNum;

  @JsonCreator
  public LinearShardSpec(
      @JsonProperty("partitionNum") Integer partitionNum
  )
  {
    this.partitionNum = Preconditions.checkNotNull(partitionNum, "Must set partitionNum on LinearShardSpec");
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

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new LinearPartitionChunk<T>(partitionNum, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "LinearShardSpec{" +
           "partitionNum=" + partitionNum +
           '}';
  }
}
