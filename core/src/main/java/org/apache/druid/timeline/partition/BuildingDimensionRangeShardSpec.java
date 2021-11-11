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
import org.apache.druid.data.input.StringTuple;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * See {@link BuildingShardSpec} for how this class is used.
 * <p>
 * Calling {@link #convert(int)} on an instance of this class creates a
 * {@link SingleDimensionShardSpec} if there is a single dimension or a
 * {@link DimensionRangeShardSpec} if there are multiple dimensions.
 *
 * @see SingleDimensionShardSpec
 * @see DimensionRangeShardSpec
 */
public class BuildingDimensionRangeShardSpec implements BuildingShardSpec<DimensionRangeShardSpec>
{
  public static final String TYPE = "building_range";

  private final int bucketId;
  private final List<String> dimensions;
  @Nullable
  private final StringTuple start;
  @Nullable
  private final StringTuple end;
  private final int partitionId;

  @JsonCreator
  public BuildingDimensionRangeShardSpec(
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("start") @Nullable StringTuple start,
      @JsonProperty("end") @Nullable StringTuple end,
      @JsonProperty("partitionNum") int partitionNum
  )
  {
    this.bucketId = bucketId;
    this.dimensions = dimensions;
    this.start = start;
    this.end = end;
    this.partitionId = partitionNum;
  }

  @JsonProperty("dimensions")
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Nullable
  @JsonProperty("start")
  public StringTuple getStartTuple()
  {
    return start;
  }

  @Nullable
  @JsonProperty("end")
  public StringTuple getEndTuple()
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
  public DimensionRangeShardSpec convert(int numCorePartitions)
  {
    return dimensions != null && dimensions.size() == 1
           ? new SingleDimensionShardSpec(
        dimensions.get(0),
        StringTuple.firstOrNull(start),
        StringTuple.firstOrNull(end),
        partitionId,
        numCorePartitions
    ) : new DimensionRangeShardSpec(
        dimensions,
        start,
        end,
        partitionId,
        numCorePartitions
    );
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
    BuildingDimensionRangeShardSpec that = (BuildingDimensionRangeShardSpec) o;
    return bucketId == that.bucketId &&
           partitionId == that.partitionId &&
           Objects.equals(dimensions, that.dimensions) &&
           Objects.equals(start, that.start) &&
           Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucketId, dimensions, start, end, partitionId);
  }

  @Override
  public String toString()
  {
    return "BuildingDimensionRangeShardSpec{" +
           "bucketId=" + bucketId +
           ", dimension='" + dimensions + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + partitionId +
           '}';
  }
}
