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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.input.InputRow;

import java.util.List;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
                  @JsonSubTypes.Type(name = "none", value = NoneShardSpec.class),
              })
public interface ShardSpec
{
  public <T> PartitionChunk<T> createChunk(T obj);

  public boolean isInChunk(long timestamp, InputRow inputRow);

  public int getPartitionNum();

  public ShardSpecLookup getLookup(List<ShardSpec> shardSpecs);
}
