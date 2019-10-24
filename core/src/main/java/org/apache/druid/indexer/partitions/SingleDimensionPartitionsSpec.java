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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Partition a segment by a single dimension.
 */
public class SingleDimensionPartitionsSpec implements DimensionBasedPartitionsSpec
{
  static final String NAME = "single_dim";
  static final String OLD_NAME = "dimension";  // for backward compatibility

  private static final String MAX_PARTITION_SIZE = "maxPartitionSize";

  private final Integer targetRowsPerSegment;
  private final Integer maxRowsPerSegment;
  private final String partitionDimension;
  private final boolean assumeGrouped;

  // Values for these fields are derived from the one above:
  private final int resolvedMaxRowPerSegment;

  @JsonCreator
  public SingleDimensionPartitionsSpec(
      @JsonProperty(DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT) @Nullable Integer targetRowsPerSegment,
      @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT) @Nullable Integer maxRowsPerSegment,
      @JsonProperty("partitionDimension") @Nullable String partitionDimension,
      @JsonProperty("assumeGrouped") boolean assumeGrouped,  // false by default

      // Deprecated properties preserved for backward compatibility:
      @Deprecated @JsonProperty(DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE) @Nullable
          Integer targetPartitionSize,  // prefer targetRowsPerSegment
      @Deprecated @JsonProperty(MAX_PARTITION_SIZE) @Nullable
          Integer maxPartitionSize  // prefer maxRowsPerSegment
  )
  {
    Integer adjustedTargetRowsPerSegment = PartitionsSpec.resolveHistoricalNullIfNeeded(targetRowsPerSegment);
    Integer adjustedMaxRowsPerSegment = PartitionsSpec.resolveHistoricalNullIfNeeded(maxRowsPerSegment);
    Integer adjustedTargetPartitionSize = PartitionsSpec.resolveHistoricalNullIfNeeded(targetPartitionSize);
    Integer adjustedMaxPartitionSize = PartitionsSpec.resolveHistoricalNullIfNeeded(maxPartitionSize);

    Property<Integer> target = Checks.checkAtMostOneNotNull(
        DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT,
        adjustedTargetRowsPerSegment,
        DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE,
        adjustedTargetPartitionSize
    );

    Property<Integer> max = Checks.checkAtMostOneNotNull(
        PartitionsSpec.MAX_ROWS_PER_SEGMENT,
        adjustedMaxRowsPerSegment,
        MAX_PARTITION_SIZE,
        adjustedMaxPartitionSize
    );

    Preconditions.checkArgument(
        (target.getValue() == null) != (max.getValue() == null),
        "Exactly one of " + target.getName() + " or " + max.getName() + " must be present"
    );

    this.partitionDimension = partitionDimension;
    this.assumeGrouped = assumeGrouped;
    this.targetRowsPerSegment = target.getValue();
    this.maxRowsPerSegment = max.getValue();

    this.resolvedMaxRowPerSegment = resolveMaxRowsPerSegment(target, max);
  }

  @VisibleForTesting
  public SingleDimensionPartitionsSpec(
      @Nullable Integer targetRowsPerSegment,
      @Nullable Integer maxRowsPerSegment,
      @Nullable String partitionDimension,
      boolean assumeGrouped
  )
  {
    this(targetRowsPerSegment, maxRowsPerSegment, partitionDimension, assumeGrouped, null, null);
  }

  private static int resolveMaxRowsPerSegment(Property<Integer> target, Property<Integer> max)
  {
    final int resolvedValue;

    if (target.getValue() != null) {
      Preconditions.checkArgument(target.getValue() > 0, target.getName() + " must be greater than 0");
      try {
        resolvedValue = Math.addExact(target.getValue(), (target.getValue() / 2));
      }
      catch (ArithmeticException e) {
        throw new IllegalArgumentException(target.getName() + " is too large");
      }
    } else {
      Preconditions.checkArgument(max.getValue() > 0, max.getName() + " must be greater than 0");
      resolvedValue = max.getValue();
    }
    return resolvedValue;
  }

  @JsonProperty
  @Override
  @Nullable
  public Integer getTargetRowsPerSegment()
  {
    return targetRowsPerSegment;
  }

  @JsonIgnore
  @Override
  @NotNull
  public Integer getMaxRowsPerSegment()
  {
    return resolvedMaxRowPerSegment;  // NOTE: This returns the *resolved* value
  }

  @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT)
  private Integer getMaxRowsPerSegmentForJson()
  {
    return maxRowsPerSegment;
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

  @JsonIgnore
  @Override
  public List<String> getPartitionDimensions()
  {
    return partitionDimension == null ? Collections.emptyList() : Collections.singletonList(partitionDimension);
  }

  @Override
  public boolean needsDeterminePartitions(boolean useForHadoopTask)
  {
    return true;
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
    return assumeGrouped == that.assumeGrouped &&
           resolvedMaxRowPerSegment == that.resolvedMaxRowPerSegment &&
           Objects.equals(targetRowsPerSegment, that.targetRowsPerSegment) &&
           Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(partitionDimension, that.partitionDimension);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        targetRowsPerSegment,
        maxRowsPerSegment,
        partitionDimension,
        assumeGrouped,
        resolvedMaxRowPerSegment
    );
  }

  @Override
  public String toString()
  {
    return "SingleDimensionPartitionsSpec{" +
           "targetRowsPerSegment=" + targetRowsPerSegment +
           ", maxRowsPerSegment=" + maxRowsPerSegment +
           ", partitionDimension='" + partitionDimension + '\'' +
           ", assumeGrouped=" + assumeGrouped +
           ", resolvedMaxRowPerSegment=" + resolvedMaxRowPerSegment +
           '}';
  }
}
