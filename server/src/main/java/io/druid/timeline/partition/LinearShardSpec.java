/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.input.InputRow;

import java.util.List;

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
  public int getPartitionNum() {
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
  public <T> PartitionChunk<T> createChunk(T obj) {
    return new LinearPartitionChunk<T>(partitionNum, obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow) {
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
