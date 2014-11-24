/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;

import java.util.List;

public class HashBasedNumberedShardSpec extends NumberedShardSpec
{
  private static final HashFunction hashFunction = Hashing.murmur3_32();
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public HashBasedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions);
    this.jsonMapper = jsonMapper;
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return (((long) hash(timestamp, inputRow)) - getPartitionNum()) % getPartitions() == 0;
  }

  protected int hash(long timestamp, InputRow inputRow)
  {
    final List<Object> groupKey = Rows.toGroupKey(timestamp, inputRow);
    try {
      return hashFunction.hashBytes(jsonMapper.writeValueAsBytes(groupKey)).asInt();
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String toString()
  {
    return "HashBasedNumberedShardSpec{" +
           "partitionNum=" + getPartitionNum() +
           ", partitions=" + getPartitions() +
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
