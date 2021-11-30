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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Partition a segment by a single dimension.
 */
public class SingleDimensionPartitionsSpec extends DimensionRangePartitionsSpec
{
  public static final String NAME = "single_dim";
  static final String OLD_NAME = "dimension";  // for backward compatibility

  private static final String PARTITION_DIMENSION = "partitionDimension";

  private final String partitionDimension;

  @JsonCreator
  public SingleDimensionPartitionsSpec(
      @JsonProperty(TARGET_ROWS_PER_SEGMENT) @Nullable Integer targetRowsPerSegment,
      @JsonProperty(MAX_ROWS_PER_SEGMENT) @Nullable Integer maxRowsPerSegment,
      @JsonProperty(PARTITION_DIMENSION) @Nullable String partitionDimension,
      @JsonProperty(ASSUME_GROUPED) boolean assumeGrouped,  // false by default

      // Deprecated properties preserved for backward compatibility:
      @Deprecated @JsonProperty(TARGET_PARTITION_SIZE) @Nullable
          Integer targetPartitionSize,  // prefer targetRowsPerSegment
      @Deprecated @JsonProperty(MAX_PARTITION_SIZE) @Nullable
          Integer maxPartitionSize  // prefer maxRowsPerSegment
  )
  {
    super(
        computeTargetRows(targetRowsPerSegment, targetPartitionSize),
        computeMaxRows(maxRowsPerSegment, maxPartitionSize),
        partitionDimension == null ? Collections.emptyList() : Collections.singletonList(partitionDimension),
        assumeGrouped
    );
    this.partitionDimension = partitionDimension;
  }

  private static Integer computeTargetRows(Integer targetRows, Integer targetPartitionSize)
  {
    Integer adjustedTargetRowsPerSegment = PartitionsSpec.resolveHistoricalNullIfNeeded(targetRows);
    Integer adjustedTargetPartitionSize = PartitionsSpec.resolveHistoricalNullIfNeeded(targetPartitionSize);

    Property<Integer> target = Checks.checkAtMostOneNotNull(
        TARGET_ROWS_PER_SEGMENT,
        adjustedTargetRowsPerSegment,
        TARGET_PARTITION_SIZE,
        adjustedTargetPartitionSize
    );

    return target.getValue();
  }

  private static Integer computeMaxRows(Integer maxRows, Integer maxPartitionSize)
  {
    Integer adjustedMaxRowsPerSegment = PartitionsSpec.resolveHistoricalNullIfNeeded(maxRows);
    Integer adjustedMaxPartitionSize = PartitionsSpec.resolveHistoricalNullIfNeeded(maxPartitionSize);

    Property<Integer> max = Checks.checkAtMostOneNotNull(
        MAX_ROWS_PER_SEGMENT,
        adjustedMaxRowsPerSegment,
        MAX_PARTITION_SIZE,
        adjustedMaxPartitionSize
    );

    return max.getValue();
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

  @JsonProperty
  @Nullable
  public String getPartitionDimension()
  {
    return partitionDimension;
  }

  /**
   * Returns a Map to be used for serializing objects of this class. This is to
   * ensure that a new field added in {@link DimensionRangePartitionsSpec} does
   * not get serialized when serializing a {@code SingleDimensionPartitionsSpec}.
   *
   * @return A map containing only the keys {@code "partitionDimension"},
   * {@code "targetRowsPerSegment"}, {@code "maxRowsPerSegments"} and
   * {@code "assumeGrouped"}.
   */
  @JsonValue
  public Map<String, Object> getSerializableObject()
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(TARGET_ROWS_PER_SEGMENT, getTargetRowsPerSegment());
    jsonMap.put(MAX_ROWS_PER_SEGMENT, getMaxRowsPerSegmentForJson());
    jsonMap.put(PARTITION_DIMENSION, getPartitionDimension());
    jsonMap.put(ASSUME_GROUPED, isAssumeGrouped());

    return jsonMap;
  }

  @Override
  public String getForceGuaranteedRollupIncompatiblityReason()
  {
    if (getPartitionDimension() == null) {
      return PARTITION_DIMENSION + " must be specified";
    }

    return FORCE_GUARANTEED_ROLLUP_COMPATIBLE;
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
    return super.equals(that);
  }

  @Override
  public int hashCode()
  {
    return super.hashCode();
  }

  @Override
  public String toString()
  {
    return "SingleDimensionPartitionsSpec{" +
           "targetRowsPerSegment=" + getTargetRowsPerSegment() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegmentForJson() +
           ", partitionDimension='" + partitionDimension + '\'' +
           ", assumeGrouped=" + isAssumeGrouped() +
           ", resolvedMaxRowPerSegment=" + getMaxRowsPerSegment() +
           '}';
  }
}
