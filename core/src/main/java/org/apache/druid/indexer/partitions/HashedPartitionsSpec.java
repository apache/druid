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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HashedPartitionsSpec implements DimensionBasedPartitionsSpec
{
  static final String NAME = "hashed";
  @VisibleForTesting
  static final String NUM_SHARDS = "numShards";

  private static final String FORCE_GUARANTEED_ROLLUP_COMPATIBLE = "";

  @Nullable
  private final Integer maxRowsPerSegment;
  @Nullable
  private final Integer numShards;
  private final List<String> partitionDimensions;

  public static HashedPartitionsSpec defaultSpec()
  {
    return new HashedPartitionsSpec(null, null, null, null, null);
  }

  @JsonCreator
  public HashedPartitionsSpec(
      @JsonProperty(DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT) @Nullable Integer targetRowsPerSegment,
      @JsonProperty(NUM_SHARDS) @Nullable Integer numShards,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,

      // Deprecated properties preserved for backward compatibility:
      @Deprecated @JsonProperty(DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE) @Nullable
          Integer targetPartitionSize,  // prefer targetRowsPerSegment
      @Deprecated @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT) @Nullable
          Integer maxRowsPerSegment  // prefer targetRowsPerSegment
  )
  {
    Integer adjustedTargetRowsPerSegment = PartitionsSpec.resolveHistoricalNullIfNeeded(targetRowsPerSegment);
    Integer adjustedNumShards = PartitionsSpec.resolveHistoricalNullIfNeeded(numShards);
    Integer adjustedTargetPartitionSize = PartitionsSpec.resolveHistoricalNullIfNeeded(targetPartitionSize);
    Integer adjustedMaxRowsPerSegment = PartitionsSpec.resolveHistoricalNullIfNeeded(maxRowsPerSegment);

    // targetRowsPerSegment, targetPartitionSize, and maxRowsPerSegment are aliases
    Property<Integer> target = Checks.checkAtMostOneNotNull(
        DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT,
        adjustedTargetRowsPerSegment,
        DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE,
        adjustedTargetPartitionSize
    );
    target = Checks.checkAtMostOneNotNull(
        target,
        new Property<>(PartitionsSpec.MAX_ROWS_PER_SEGMENT, adjustedMaxRowsPerSegment)
    );

    // targetRowsPerSegment/targetPartitionSize/maxRowsPerSegment and numShards are incompatible
    Checks.checkAtMostOneNotNull(target, new Property<>(NUM_SHARDS, adjustedNumShards));

    this.partitionDimensions = partitionDimensions == null ? Collections.emptyList() : partitionDimensions;
    this.numShards = adjustedNumShards;

    // Supply default for targetRowsPerSegment if needed
    if (target.getValue() == null) {
      //noinspection VariableNotUsedInsideIf (false positive for this.numShards)
      this.maxRowsPerSegment = (this.numShards == null ? PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT : null);
    } else {
      this.maxRowsPerSegment = target.getValue();
    }

    Preconditions.checkArgument(
        this.maxRowsPerSegment == null || this.maxRowsPerSegment > 0,
        "%s[%s] should be positive",
        target.getName(),
        target.getValue()
    );
    Preconditions.checkArgument(
        this.numShards == null || this.numShards > 0,
        "numShards[%s] should be positive",
        this.numShards
    );
  }

  public HashedPartitionsSpec(
      @Nullable Integer maxRowsPerSegment,
      @Nullable Integer numShards,
      @Nullable List<String> partitionDimensions
  )
  {
    this(null, numShards, partitionDimensions, null, maxRowsPerSegment);
  }

  @Nullable
  @Override
  public Integer getTargetRowsPerSegment()
  {
    return null;
  }

  @Override
  public SecondaryPartitionType getType()
  {
    return SecondaryPartitionType.HASH;
  }

  @Nullable
  @Override
  @JsonProperty
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @Override
  public boolean needsDeterminePartitions(boolean useForHadoopTask)
  {
    return useForHadoopTask ? maxRowsPerSegment != null : numShards == null;
  }

  @Nullable
  @JsonProperty
  public Integer getNumShards()
  {
    return numShards;
  }

  @Override
  @JsonProperty
  public List<String> getPartitionDimensions()
  {
    return partitionDimensions;
  }

  @Override
  public String getForceGuaranteedRollupIncompatiblityReason()
  {
    return getNumShards() == null ? NUM_SHARDS + " must be specified" : FORCE_GUARANTEED_ROLLUP_COMPATIBLE;
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
    HashedPartitionsSpec that = (HashedPartitionsSpec) o;
    return Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(numShards, that.numShards) &&
           Objects.equals(partitionDimensions, that.partitionDimensions);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxRowsPerSegment, numShards, partitionDimensions);
  }

  @Override
  public String toString()
  {
    return "HashedPartitionsSpec{" +
           "maxRowsPerSegment=" + maxRowsPerSegment +
           ", numShards=" + numShards +
           ", partitionDimensions=" + partitionDimensions +
           '}';
  }
}
