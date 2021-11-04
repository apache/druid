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
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

/**
 * Spec to create partitions based on value ranges of multiple dimensions.
 * <p>
 * A MultiDimensionPartitionSpec has the following fields:
 * <ul>
 *   <li>either targetRowsPerSegment or maxRowsPerSegment</li>
 *   <li>partitionDimensions: List of dimension names to be used for partitioning</li>
 *   <li>assumeGrouped: true if input data has already been grouped on time and dimensions</li>
 * </ul>
 */
public class DimensionRangePartitionsSpec implements DimensionBasedPartitionsSpec
{
  public static final String NAME = "range";

  private final Integer targetRowsPerSegment;
  private final Integer maxRowsPerSegment;
  private final List<String> partitionDimensions;
  private final boolean assumeGrouped;

  // Value of this field is derived from targetRows and maxRows
  private final int resolvedMaxRowPerSegment;

  @JsonCreator
  public DimensionRangePartitionsSpec(
      @JsonProperty(TARGET_ROWS_PER_SEGMENT) @Nullable Integer targetRowsPerSegment,
      @JsonProperty(MAX_ROWS_PER_SEGMENT) @Nullable Integer maxRowsPerSegment,
      @JsonProperty(PARTITION_DIMENSIONS) List<String> partitionDimensions,
      @JsonProperty(ASSUME_GROUPED) boolean assumeGrouped  // false by default
  )
  {
    Preconditions.checkArgument(partitionDimensions != null, "partitionDimensions must be specified");
    this.partitionDimensions = partitionDimensions;
    this.assumeGrouped = assumeGrouped;

    final Property<Integer> target = new Property<>(
        TARGET_ROWS_PER_SEGMENT,
        PartitionsSpec.resolveHistoricalNullIfNeeded(targetRowsPerSegment)
    );
    final Property<Integer> max = new Property<>(
        MAX_ROWS_PER_SEGMENT,
        PartitionsSpec.resolveHistoricalNullIfNeeded(maxRowsPerSegment)
    );

    Preconditions.checkArgument(
        (target.getValue() == null) != (max.getValue() == null),
        "Exactly one of " + target.getName() + " or " + max.getName() + " must be present"
    );

    this.resolvedMaxRowPerSegment = resolveMaxRowsPerSegment(target, max);
    this.targetRowsPerSegment = target.getValue();
    this.maxRowsPerSegment = max.getValue();
  }

  private static int resolveMaxRowsPerSegment(Property<Integer> targetRows, Property<Integer> maxRows)
  {
    if (targetRows.getValue() != null) {
      Preconditions.checkArgument(targetRows.getValue() > 0, targetRows.getName() + " must be greater than 0");
      try {
        return Math.addExact(targetRows.getValue(), (targetRows.getValue() / 2));
      }
      catch (ArithmeticException e) {
        throw new IllegalArgumentException(targetRows.getName() + " is too large");
      }
    } else {
      Preconditions.checkArgument(maxRows.getValue() > 0, maxRows.getName() + " must be greater than 0");
      return maxRows.getValue();
    }
  }

  @JsonProperty
  @Override
  @Nullable
  public Integer getTargetRowsPerSegment()
  {
    return targetRowsPerSegment;
  }

  @Override
  public SecondaryPartitionType getType()
  {
    return SecondaryPartitionType.RANGE;
  }

  /**
   * @return Resolved value of max rows per segment.
   */
  @JsonIgnore
  @Override
  @NotNull
  public Integer getMaxRowsPerSegment()
  {
    return resolvedMaxRowPerSegment;  // NOTE: This returns the *resolved* value
  }

  @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT)
  protected Integer getMaxRowsPerSegmentForJson()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  public boolean isAssumeGrouped()
  {
    return assumeGrouped;
  }

  @JsonProperty
  @Override
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public String getForceGuaranteedRollupIncompatiblityReason()
  {
    if (getPartitionDimensions() == null || getPartitionDimensions().isEmpty()) {
      return PARTITION_DIMENSIONS + " must be specified";
    }

    return FORCE_GUARANTEED_ROLLUP_COMPATIBLE;
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
    DimensionRangePartitionsSpec that = (DimensionRangePartitionsSpec) o;
    return assumeGrouped == that.assumeGrouped &&
           resolvedMaxRowPerSegment == that.resolvedMaxRowPerSegment &&
           Objects.equals(targetRowsPerSegment, that.targetRowsPerSegment) &&
           Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(partitionDimensions, that.partitionDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        targetRowsPerSegment,
        maxRowsPerSegment,
        partitionDimensions,
        assumeGrouped,
        resolvedMaxRowPerSegment
    );
  }

  @Override
  public String toString()
  {
    return "DimensionRangePartitionsSpec{" +
           "targetRowsPerSegment=" + targetRowsPerSegment +
           ", maxRowsPerSegment=" + maxRowsPerSegment +
           ", partitionDimension='" + partitionDimensions + '\'' +
           ", assumeGrouped=" + assumeGrouped +
           ", resolvedMaxRowPerSegment=" + resolvedMaxRowPerSegment +
           '}';
  }
}
