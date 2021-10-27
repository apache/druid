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

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * Partition a segment by a single dimension.
 */
public class SingleDimensionPartitionsSpec extends MultiDimensionPartitionsSpec
{
  public static final String NAME = "single_dim";
  static final String OLD_NAME = "dimension";  // for backward compatibility

  private static final String PARITION_DIMENSION = "partitionDimension";
  private static final String MAX_PARTITION_SIZE = "maxPartitionSize";
  private static final String FORCE_GUARANTEED_ROLLUP_COMPATIBLE = "";

  private final String partitionDimension;

  @JsonCreator
  public SingleDimensionPartitionsSpec(
      @JsonProperty(DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT) @Nullable Integer targetRowsPerSegment,
      @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT) @Nullable Integer maxRowsPerSegment,
      @JsonProperty(PARITION_DIMENSION) @Nullable String partitionDimension,
      @JsonProperty("assumeGrouped") boolean assumeGrouped,  // false by default

      // Deprecated properties preserved for backward compatibility:
      @Deprecated @JsonProperty(DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE) @Nullable
          Integer targetPartitionSize,  // prefer targetRowsPerSegment
      @Deprecated @JsonProperty(MAX_PARTITION_SIZE) @Nullable
          Integer maxPartitionSize  // prefer maxRowsPerSegment
  )
  {
    super(
        targetRowsPerSegment,
        maxRowsPerSegment,
        partitionDimension == null ? Collections.emptyList() : Collections.singletonList(partitionDimension),
        assumeGrouped,
        targetPartitionSize,
        maxPartitionSize
    );
    this.partitionDimension = partitionDimension;
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

  @JsonIgnore
  @Override
  public List<String> getPartitionDimensions()
  {
    return super.getPartitionDimensions();
  }

  @Override
  public String getForceGuaranteedRollupIncompatiblityReason()
  {
    if (getPartitionDimension() == null) {
      return PARITION_DIMENSION + " must be specified";
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
