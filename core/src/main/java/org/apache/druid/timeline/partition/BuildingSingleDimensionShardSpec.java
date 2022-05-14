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
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.data.input.StringTuple;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * See {@link BuildingShardSpec} for how this class is used.
 *
 * @see SingleDimensionShardSpec
 */
public class BuildingSingleDimensionShardSpec extends BuildingDimensionRangeShardSpec
{
  private final String dimension;

  @Nullable
  private final String start;

  @Nullable
  private final String end;

  @JsonCreator
  public BuildingSingleDimensionShardSpec(
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("dimension") String dimension,
      @JsonProperty("start") @Nullable String start,
      @JsonProperty("end") @Nullable String end,
      @JsonProperty("partitionNum") int partitionNum
  )
  {
    super(
        bucketId,
        dimension == null ? Collections.emptyList() : Collections.singletonList(dimension),
        start == null ? null : StringTuple.create(start),
        end == null ? null : StringTuple.create(end),
        partitionNum
    );
    this.dimension = dimension;
    this.start = start;
    this.end = end;
  }

  @JsonValue
  public Map<String, Object> getSerializableObject()
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("dimension", dimension);
    jsonMap.put("start", start);
    jsonMap.put("end", end);
    jsonMap.put("bucketId", getBucketId());
    jsonMap.put("partitionNum", getPartitionNum());

    return jsonMap;
  }

  public String getDimension()
  {
    return dimension;
  }

  @Nullable
  public String getStart()
  {
    return start;
  }

  @Nullable
  public String getEnd()
  {
    return end;
  }

  @Override
  public SingleDimensionShardSpec convert(int numCorePartitions)
  {
    return new SingleDimensionShardSpec(dimension, start, end, getPartitionNum(), numCorePartitions);
  }

  @Override
  public <T> PartitionChunk<T> createChunk(T obj)
  {
    return new NumberedPartitionChunk<>(getPartitionNum(), 0, obj);
  }

  @Override
  public String getType()
  {
    return Type.BUILDING_SINGLE_DIM;
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
    return getBucketId() == that.getBucketId() &&
           getPartitionNum() == that.getPartitionNum() &&
           Objects.equals(dimension, that.dimension) &&
           Objects.equals(start, that.start) &&
           Objects.equals(end, that.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getBucketId(), dimension, start, end, getPartitionNum());
  }

  @Override
  public String toString()
  {
    return "BuildingSingleDimensionShardSpec{" +
           "bucketId=" + getBucketId() +
           ", dimension='" + dimension + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           ", partitionNum=" + getPartitionNum() +
           '}';
  }
}
