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

package org.apache.druid.indexer.partitions;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SingleDimensionPartitionsSpec implements DimensionBasedPartitionsSpec
{
  private final int maxRowsPerSegment;
  private final int maxPartitionSize;
  @Nullable
  private final String partitionDimension;
  private final boolean assumeGrouped;

  public SingleDimensionPartitionsSpec(
      int maxRowsPerSegment,
      @Nullable Integer maxPartitionSize,
      @Nullable String partitionDimension,
      boolean assumeGrouped
  )
  {
    this(null, maxRowsPerSegment, maxPartitionSize, partitionDimension, assumeGrouped);
  }

  @JsonCreator
  public SingleDimensionPartitionsSpec(
      @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxPartitionSize") @Nullable Integer maxPartitionSize,
      @JsonProperty("partitionDimension") @Nullable String partitionDimension,
      @JsonProperty("assumeGrouped") boolean assumeGrouped // false by default
  )
  {
    Preconditions.checkArgument(
        PartitionsSpec.isEffectivelyNull(targetPartitionSize) || PartitionsSpec.isEffectivelyNull(maxRowsPerSegment),
        "Can't set both targetPartitionSize and maxRowsPerSegment"
    );
    Preconditions.checkArgument(
        !PartitionsSpec.isEffectivelyNull(targetPartitionSize) || !PartitionsSpec.isEffectivelyNull(maxRowsPerSegment),
        "Either targetPartitionSize or maxRowsPerSegment must be specified"
    );
    final int realMaxRowsPerSegment = targetPartitionSize == null ? maxRowsPerSegment : targetPartitionSize;
    Preconditions.checkArgument(realMaxRowsPerSegment > 0, "maxRowsPerSegment must be specified");
    this.maxRowsPerSegment = realMaxRowsPerSegment;
    this.maxPartitionSize = PartitionsSpec.isEffectivelyNull(maxPartitionSize)
                            ? Math.addExact(realMaxRowsPerSegment, (int) (realMaxRowsPerSegment * 0.5))
                            : maxPartitionSize;
    this.partitionDimension = partitionDimension;
    this.assumeGrouped = assumeGrouped;
  }

  @Override
  @JsonProperty
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @Override
  public boolean needsDeterminePartitions(boolean useForHadoopTask)
  {
    return true;
  }

  @JsonProperty
  public int getMaxPartitionSize()
  {
    return maxPartitionSize;
  }

  @JsonProperty
  @Nullable
  public String getPartitionDimension()
  {
    return partitionDimension;
  }

  @JsonProperty
  public boolean isAssumeGrouped()
  {
    return assumeGrouped;
  }

  @Override
  public List<String> getPartitionDimensions()
  {
    return partitionDimension == null ? Collections.emptyList() : Collections.singletonList(partitionDimension);
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
    SingleDimensionPartitionsSpec that = (SingleDimensionPartitionsSpec) o;
    return maxRowsPerSegment == that.maxRowsPerSegment &&
           maxPartitionSize == that.maxPartitionSize &&
           assumeGrouped == that.assumeGrouped &&
           Objects.equals(partitionDimension, that.partitionDimension);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxRowsPerSegment, maxPartitionSize, partitionDimension, assumeGrouped);
  }

  @Override
  public String toString()
  {
    return "SingleDimensionPartitionsSpec{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", maxPartitionSize=" + maxPartitionSize +
           ", partitionDimension='" + partitionDimension + '\'' +
           ", assumeGrouped=" + assumeGrouped +
           '}';
  }
}
