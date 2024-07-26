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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A shard spec to represent tombstones. Its partition number is always zero and contains zero core partitions as it
 * contains no data. This allows other shard types appending to an existing {@link TombstoneShardSpec} to exist independently
 * in the timeline even if the {@link TombstoneShardSpec} is dropped.
 */
public class TombstoneShardSpec implements ShardSpec
{
  public static TombstoneShardSpec INSTANCE = new TombstoneShardSpec();

  @JsonProperty("partitionNum")
  @Override
  public int getPartitionNum()
  {
    return 0;
  }

  @Override
  public ShardSpecLookup getLookup(final List<? extends ShardSpec> shardSpecs)
  {
    return createLookup(shardSpecs);
  }

  static ShardSpecLookup createLookup(List<? extends ShardSpec> shardSpecs)
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
  @JsonProperty("partitions")
  public int getNumCorePartitions()
  {
    return 0;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return TombstonePartitionedChunk.make(obj);
  }

  @Override
  public String getType()
  {
    return Type.TOMBSTONE;
  }

  @Override
  public String toString()
  {
    return "TombstoneShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getNumCorePartitions() +
           '}';
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
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(0);
  }

}
