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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * An extendable linear shard spec.  {@link #partitionNum} represents an unique id of a partition.
 */
public final class LinearShardSpec implements ShardSpec
{
  private final int partitionNum;

  @JsonCreator
  public LinearShardSpec(@JsonProperty("partitionNum") Integer partitionNum)
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
  public int getNumCorePartitions()
  {
    return 0;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
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
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new LinearPartitionChunk<>(partitionNum, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LinearShardSpec)) {
      return false;
    }
    LinearShardSpec that = (LinearShardSpec) o;
    return partitionNum == that.partitionNum;
  }

  @Override
  public int hashCode()
  {
    return partitionNum;
  }

  @Override
  public String toString()
  {
    return "LinearShardSpec{" +
           "partitionNum=" + partitionNum +
           '}';
  }
}
