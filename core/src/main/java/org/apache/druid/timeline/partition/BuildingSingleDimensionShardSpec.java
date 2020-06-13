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

import javax.annotation.Nullable;

public class BuildingSingleDimensionShardSpec implements BuildingShardSpec<SingleDimensionShardSpec>
{
  private final String dimension;
  @Nullable
  private final String start;
  @Nullable
  private final String end;
  private final int partitionNum;

  @JsonCreator
  public BuildingSingleDimensionShardSpec(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("start") @Nullable String start,
      @JsonProperty("end") @Nullable String end,
      @JsonProperty("partitionNum") int partitionNum
  )
  {
    this.dimension = dimension;
    this.start = start;
    this.end = end;
    this.partitionNum = partitionNum;
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  @Nullable
  @JsonProperty("start")
  public String getStart()
  {
    return start;
  }

  @Nullable
  @JsonProperty("end")
  public String getEnd()
  {
    return end;
  }

  @Override
  @JsonProperty("partitionNum")
  public int getPartitionNum()
  {
    return partitionNum;
  }

  @Override
  public SingleDimensionShardSpec convert(int numCorePartitions)
  {
    return new SingleDimensionShardSpec(dimension, start, end, partitionNum, numCorePartitions);
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new NumberedPartitionChunk<>(partitionNum, 0, obj);
  }
}
