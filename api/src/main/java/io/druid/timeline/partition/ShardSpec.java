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

package io.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.RangeSet;
import io.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson. Note that this is not an
 * extension API. Extensions are not expected to create new kinds of ShardSpecs.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
                  @JsonSubTypes.Type(name = "none", value = NoneShardSpec.class),
              })
public interface ShardSpec
{
  <T> PartitionChunk<T> createChunk(T obj);

  boolean isInChunk(long timestamp, InputRow inputRow);

  int getPartitionNum();

  ShardSpecLookup getLookup(List<ShardSpec> shardSpecs);

  /**
   * Get the possible range of each dimension for the rows this shard contains.
   *
   * @return map of dimensions to its possible range. Dimensions with unknown possible range are not mapped
   */
  Map<String, RangeSet<String>> getDomain();
}
