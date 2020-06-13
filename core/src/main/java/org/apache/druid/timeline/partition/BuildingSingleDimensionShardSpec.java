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
import java.util.Objects;

/**
 * See {@link BuildingShardSpec} for how this class is used.
 *
 * @see SingleDimensionShardSpec
 */
public class BuildingSingleDimensionShardSpec implements BuildingShardSpec<SingleDimensionShardSpec>
{
  public static final String TYPE = "building_single_dim";

  private final int bucketId;
  private final String dimension;
  @Nullable
  private final String start;
  @Nullable
  private final String end;
  private final int partitionId;

  @JsonCreator
  public BuildingSingleDimensionShardSpec(
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("dimension") String dimension,
      @JsonProperty("start") @Nullable String start,
      @JsonProperty("end") @Nullable String end,
      @JsonProperty("partitionNum") int partitionNum
  )
  {
    this.bucketId = bucketId;
    this.dimension = dimension;
    this.start = start;
    this.end = end;
    this.partitionId = partitionNum;
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
    return partitionId;
  }

  @Override
  @JsonProperty("bucketId")
  public int getBucketId()
  {
    return bucketId;
  }

  @Override
  public SingleDimensionShardSpec convert(int numCorePartitions)
  {
    return new SingleDimensionShardSpec(dimension, start, end, partitionId, numCorePartitions);
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new NumberedPartitionChunk<>(partitionId, 0, obj);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BuildingSingleDimensionShardSpec that = (BuildingSingleDimensionShardSpec) o;
    return bucketId == that.bucketId &&
           partitionId == that.partitionId &&
           Objects.equals(dimension, that.dimension) &&
           Objects.equals(start, that.start) &&
           Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucketId, dimension, start, end, partitionId);
  }

  @Override
  public String toString()
  {
    return "BuildingSingleDimensionShardSpec{" +
           "bucketId=" + bucketId +
           ", dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + partitionId +
           '}';
  }
}
