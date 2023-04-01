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
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.StringTuple;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * See {@link BucketNumberedShardSpec} for how this class is used.
 * <p>
 * Calling {@link #convert(int)} on an instance of this class creates a
 * {@link BuildingSingleDimensionShardSpec} if there is a single dimension
 * or {@link BuildingDimensionRangeShardSpec} if there are multiple dimensions.
 *
 * @see BuildingSingleDimensionShardSpec
 * @see BuildingDimensionRangeShardSpec
 */
public class DimensionRangeBucketShardSpec extends BaseDimensionRangeShardSpec
    implements BucketNumberedShardSpec<BuildingDimensionRangeShardSpec>
{
  private final int bucketId;

  @JsonCreator
  public DimensionRangeBucketShardSpec(
      @JsonProperty("bucketId") int bucketId,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("start") @Nullable StringTuple start,
      @JsonProperty("end") @Nullable StringTuple end
  )
  {
    super(dimensions, start, end);
    // Verify that the tuple sizes and number of dimensions are the same
    Preconditions.checkArgument(
        start == null || start.size() == dimensions.size(),
        "Start tuple must either be null or of the same size as the number of partition dimensions"
    );
    Preconditions.checkArgument(
        end == null || end.size() == dimensions.size(),
        "End tuple must either be null or of the same size as the number of partition dimensions"
    );

    this.bucketId = bucketId;
  }

  @Override
  @JsonProperty
  public int getBucketId()
  {
    return bucketId;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @Nullable
  @JsonProperty
  public StringTuple getStart()
  {
    return start;
  }

  @Nullable
  @JsonProperty
  public StringTuple getEnd()
  {
    return end;
  }

  @Override
  public BuildingDimensionRangeShardSpec convert(int partitionId)
  {
    return dimensions != null && dimensions.size() == 1
           ? new BuildingSingleDimensionShardSpec(
        bucketId,
        dimensions.get(0),
        StringTuple.firstOrNull(start),
        StringTuple.firstOrNull(end),
        partitionId
    ) : new BuildingDimensionRangeShardSpec(
        bucketId,
        dimensions,
        start,
        end,
        partitionId
    );
  }

  @Override
  public String getType()
  {
    return Type.BUCKET_RANGE;
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
    DimensionRangeBucketShardSpec bucket = (DimensionRangeBucketShardSpec) o;
    return bucketId == bucket.bucketId &&
           Objects.equals(dimensions, bucket.dimensions) &&
           Objects.equals(start, bucket.start) &&
           Objects.equals(end, bucket.end);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucketId, dimensions, start, end);
  }

  @Override
  public String toString()
  {
    return "DimensionRangeBucketShardSpec{" +
           ", bucketId=" + bucketId +
           ", dimension='" + dimensions + '\'' +
           ", start='" + start + '\'' +
           ", end='" + end + '\'' +
           '}';
  }
}
