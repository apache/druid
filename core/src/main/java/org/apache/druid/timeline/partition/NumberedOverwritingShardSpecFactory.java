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
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

public class NumberedOverwritingShardSpecFactory implements ShardSpecFactory
{
  private final int startRootPartitionId;
  private final int endRootPartitionId;
  private final short minorVersion;

  @JsonCreator
  public NumberedOverwritingShardSpecFactory(
      @JsonProperty("startRootPartitionId") int startRootPartitionId,
      @JsonProperty("endRootPartitionId") int endRootPartitionId,
      @JsonProperty("minorVersion") short minorVersion
  )
  {
    this.startRootPartitionId = startRootPartitionId;
    this.endRootPartitionId = endRootPartitionId;
    this.minorVersion = minorVersion;
  }

  @JsonProperty
  public int getStartRootPartitionId()
  {
    return startRootPartitionId;
  }

  @JsonProperty
  public int getEndRootPartitionId()
  {
    return endRootPartitionId;
  }

  @JsonProperty
  public short getMinorVersion()
  {
    return minorVersion;
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, @Nullable ShardSpec specOfPreviousMaxPartitionId)
  {
    // specOfPreviousMaxPartitionId is the max partitionId of the same shardSpec
    // and could be null if all existing segments are first-generation segments.
    return new NumberedOverwriteShardSpec(
        specOfPreviousMaxPartitionId == null
        ? PartitionIds.NON_ROOT_GEN_START_PARTITION_ID
        : specOfPreviousMaxPartitionId.getPartitionNum() + 1,
        startRootPartitionId,
        endRootPartitionId,
        minorVersion
    );
  }

  @Override
  public ShardSpec create(ObjectMapper objectMapper, int partitionId)
  {
    return new NumberedOverwriteShardSpec(partitionId, startRootPartitionId, endRootPartitionId, minorVersion);
  }

  @Override
  public Class<? extends ShardSpec> getShardSpecClass()
  {
    return NumberedOverwriteShardSpec.class;
  }
}
