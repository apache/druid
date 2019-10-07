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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.InputRow;

import java.util.List;
import java.util.Map;

/**
 * A Marker interface that exists to combine ShardSpec objects together for Jackson. Note that this is not an
 * extension API. Extensions are not expected to create new kinds of ShardSpecs.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = "none", value = NoneShardSpec.class),
    @JsonSubTypes.Type(name = "single", value = SingleDimensionShardSpec.class),
    @JsonSubTypes.Type(name = "linear", value = LinearShardSpec.class),
    @JsonSubTypes.Type(name = "numbered", value = NumberedShardSpec.class),
    @JsonSubTypes.Type(name = "hashed", value = HashBasedNumberedShardSpec.class),
    @JsonSubTypes.Type(name = "numbered_overwrite", value = NumberedOverwriteShardSpec.class)
})
public interface ShardSpec
{
  <T> PartitionChunk<T> createChunk(T obj);

  boolean isInChunk(long timestamp, InputRow inputRow);

  int getPartitionNum();

  default int getStartRootPartitionId()
  {
    return getPartitionNum();
  }

  default int getEndRootPartitionId()
  {
    return getPartitionNum() + 1;
  }

  default short getMinorVersion()
  {
    return 0;
  }

  default short getAtomicUpdateGroupSize()
  {
    return 1;
  }

  ShardSpecLookup getLookup(List<ShardSpec> shardSpecs);

  /**
   * Get dimensions who have possible range for the rows this shard contains.
   *
   * @return list of dimensions who has its possible range. Dimensions with unknown possible range are not listed
   */
  List<String> getDomainDimensions();

  /**
   * if given domain ranges are not possible in this shard, return false; otherwise return true;
   * @return possibility of in domain
   */
  boolean possibleInDomain(Map<String, RangeSet<String>> domain);

  /**
   * Returns true if two segments of this and other shardSpecs can exist in the same timeChunk.
   */
  boolean isCompatible(Class<? extends ShardSpec> other);
}
