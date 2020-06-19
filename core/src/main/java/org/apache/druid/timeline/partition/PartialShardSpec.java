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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

/**
 * This interface is used in the segment allocation protocol when it is coordinated by the Overlord; when appending
 * segments to an existing datasource (either streaming ingestion or batch append) or any case when segment
 * lock is used. The implementations of this interface contain all information of the corresponding {@link ShardSpec}
 * except the partition ID.
 * The ingestion tasks send all information required for allocating a new segment using this interface and the Overlord
 * determines the partition ID to create a new segment.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @Type(name = "numbered", value = NumberedPartialShardSpec.class),
    @Type(name = HashBasedNumberedPartialShardSpec.TYPE, value = HashBasedNumberedPartialShardSpec.class),
    @Type(name = "single_dim", value = SingleDimensionPartialShardSpec.class),
    @Type(name = "numbered_overwrite", value = NumberedOverwritePartialShardSpec.class),
})
public interface PartialShardSpec
{
  /**
   * Creates a new ShardSpec based on {@code specOfPreviousMaxPartitionId}. If it's null, it assumes that this is the
   * first call for the time chunk where the new segment is created.
   * Note that {@code specOfPreviousMaxPartitionId} can also be null for {@link OverwriteShardSpec} if all segments
   * in the timeChunk are first-generation segments.
   */
  ShardSpec complete(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId);

  /**
   * Creates a new shardSpec having the given partitionId.
   */
  ShardSpec complete(ObjectMapper objectMapper, int partitionId);

  /**
   * Returns the class of the shardSpec created by this factory.
   */
  @JsonIgnore
  Class<? extends ShardSpec> getShardSpecClass();
}
