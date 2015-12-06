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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.timeline.partition.ShardSpec;

/**
 * ShardSpec + a shard ID that is unique across this run. The shard ID is used for grouping and partitioning.
 * There is one HadoopyShardSpec per Bucket, and our shardNum matches the Bucket shardNum.
 */
public class HadoopyShardSpec
{
  private final ShardSpec actualSpec;
  private final int shardNum;

  @JsonCreator
  public HadoopyShardSpec(
      @JsonProperty("actualSpec") ShardSpec actualSpec,
      @JsonProperty("shardNum") int shardNum
  )
  {
    this.actualSpec = actualSpec;
    this.shardNum = shardNum;
  }

  @JsonProperty
  public ShardSpec getActualSpec()
  {
    return actualSpec;
  }

  @JsonProperty
  public int getShardNum()
  {
    return shardNum;
  }

  @Override
  public String toString()
  {
    return "HadoopyShardSpec{" +
           "actualSpec=" + actualSpec +
           ", shardNum=" + shardNum +
           '}';
  }
}
