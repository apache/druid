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

package org.apache.druid.timeline.partition.extension.hash.named.numbered.shard.spec;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NamedNumberedPartitionChunk;
import org.apache.druid.timeline.partition.PartitionChunk;

import javax.annotation.Nullable;
import java.util.List;

public class HashBasedNamedNumberedShardSpec extends HashBasedNumberedShardSpec
{
  @JsonIgnore
  private final String partitionName;

  @JsonCreator
  public HashBasedNamedNumberedShardSpec(
      @JsonProperty("partitionNum") int partitionNum,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,
      @JsonProperty("partitionName") @Nullable String partitionName,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(partitionNum, partitions, partitionDimensions, jsonMapper);
    this.partitionName = partitionName;
  }

  @JsonProperty("partitionName")
  public String getPartitionName()
  {
    return this.partitionName;
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return NamedNumberedPartitionChunk.make(getPartitionNum(), getPartitions(), partitionName, obj);
  }

  @Override
  public String toString()
  {
    return "HashBasedNamedNumberedShardSpec{" +
        "partitionNum=" + getPartitionNum() +
        ", partitions=" + getPartitions() +
        ", partitionDimensions=" + getPartitionDimensions() +
        ", partitionName=" + getPartitionName() +
        '}';
  }

  @Override
  public Object getIdentifier()
  {
    return this.partitionName + "_" + this.getPartitionNum();
  }
}
