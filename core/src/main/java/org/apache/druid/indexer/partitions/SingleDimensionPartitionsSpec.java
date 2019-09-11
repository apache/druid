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
    Property<Integer> target = checkAtMostOneNotNull(
        TARGET_ROWS_PER_SEGMENT,
        targetRowsPerSegment,
        TARGET_PARTITION_SIZE,
        targetPartitionSize
    );

    Property<Integer> max = checkAtMostOneNotNull(
        MAX_ROWS_PER_SEGMENT,
        maxRowsPerSegment,
        MAX_PARTITION_SIZE,
        maxPartitionSize
    );

    Preconditions.checkArgument(
        (target.value == null) != (max.value == null),
        "Exactly one of " + target.name + " or " + max.name + " must be present"
    );

    this.partitionDimension = partitionDimension;
    this.assumeGrouped = assumeGrouped;
    this.targetRowsPerSegment = target.value;
    this.maxRowsPerSegment = max.value;

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

  /**
   * @return Non-null value, or first one if both are null
   */
  @SuppressWarnings("VariableNotUsedInsideIf")  // false positive: checked for 'null' not used inside 'if
  private static Property<Integer> checkAtMostOneNotNull(String name1, Integer value1, String name2, Integer value2)
  {
    final Property<Integer> property;

    if (value1 == null && value2 == null) {
      property = new Property<>(name1, value1);
    } else if (value1 == null) {
      property = new Property<>(name2, value2);
    } else if (value2 == null) {
      property = new Property<>(name1, value1);
    } else {
      throw new IllegalArgumentException("At most one of " + name1 + " or " + name2 + " must be present");
    }

    return property;
  }

  private static int resolveMaxRowsPerSegment(Property<Integer> target, Property<Integer> max)
  {
    final int resolvedValue;

    if (target.value != null) {
      Preconditions.checkArgument(target.value > 0, target.name + " must be greater than 0");
      try {
        resolvedValue = Math.addExact(target.value, (target.value / 2));
      }
      catch (ArithmeticException e) {
        throw new IllegalArgumentException(target.name + " is too large");
      }
    } else {
      Preconditions.checkArgument(max.value > 0, max.name + " must be greater than 0");
      resolvedValue = max.value;
    }
    return resolvedValue;
  }

  private static class Property<T>
  {
    private final String name;
    private final T value;

    Property(String name, T value)
    {
      this.name = name;
      this.value = value;
    }
  }

  @JsonProperty
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

  @JsonProperty(MAX_ROWS_PER_SEGMENT)
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
