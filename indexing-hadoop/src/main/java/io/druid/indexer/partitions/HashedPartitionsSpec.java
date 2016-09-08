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

package io.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.druid.indexer.DetermineHashedPartitionsJob;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.Jobby;

import javax.annotation.Nullable;
import java.util.List;

public class HashedPartitionsSpec extends AbstractPartitionsSpec
{
  private static final List<String> DEFAULT_PARTITION_DIMENSIONS = ImmutableList.of();

  public static HashedPartitionsSpec makeDefaultHashedPartitionsSpec()
  {
    return new HashedPartitionsSpec(null, null, null, null, null);
  }

  @JsonIgnore
  private final List<String> partitionDimensions;

  @JsonCreator
  public HashedPartitionsSpec(
      @JsonProperty("targetPartitionSize") @Nullable Long targetPartitionSize,
      @JsonProperty("maxPartitionSize") @Nullable Long maxPartitionSize,
      @JsonProperty("assumeGrouped") @Nullable Boolean assumeGrouped,
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions
  )
  {
    super(targetPartitionSize, maxPartitionSize, assumeGrouped, numShards);
    this.partitionDimensions = partitionDimensions == null ? DEFAULT_PARTITION_DIMENSIONS : partitionDimensions;
  }

  @Override
  public Jobby getPartitionJob(HadoopDruidIndexerConfig config)
  {
    return new DetermineHashedPartitionsJob(config);
  }

  @Override
  @JsonProperty
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }
}
