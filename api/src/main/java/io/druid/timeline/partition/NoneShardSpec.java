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
import io.druid.data.input.InputRow;

import java.util.List;

/**
 */
public class NoneShardSpec implements ShardSpec
{
  private final static NoneShardSpec INSTANCE = new NoneShardSpec();

  @JsonCreator
  public static NoneShardSpec instance() { return INSTANCE; }

  @Deprecated
  // Use NoneShardSpec.instance() instead
  public NoneShardSpec(){

  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new SingleElementPartitionChunk<T>(obj);
  }

  @Override
  public boolean isInChunk(long timestamp, InputRow inputRow)
  {
    return true;
  }

  @Override
  public int getPartitionNum()
  {
    return 0;
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
  public boolean equals(Object obj)
  {
    return obj instanceof NoneShardSpec;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "NoneShardSpec";
  }
}
