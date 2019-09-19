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
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class HashedPartitionsSpec implements DimensionBasedPartitionsSpec
{
  static final String NAME = "hashed";
  private static final Logger LOG = new Logger(HashedPartitionsSpec.class);

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
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("partitionDimensions") @Nullable List<String> partitionDimensions,

      // Deprecated properties preserved for backward compatibility:
      @Deprecated @JsonProperty(DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE) @Nullable
          Integer targetPartitionSize,
      @Deprecated @JsonProperty(PartitionsSpec.MAX_ROWS_PER_SEGMENT) @Nullable
          Integer maxRowsPerSegment
  )
  {
    Property<Integer> target = Checks.checkAtMostOneNotNull(
        DimensionBasedPartitionsSpec.TARGET_ROWS_PER_SEGMENT,
        targetRowsPerSegment,
        DimensionBasedPartitionsSpec.TARGET_PARTITION_SIZE,
        targetPartitionSize
    );

    Preconditions.checkArgument(
        PartitionsSpec.isEffectivelyNull(target.getValue()) || PartitionsSpec.isEffectivelyNull(maxRowsPerSegment),
        "Can't set both " + target.getName() + " and maxRowsPerSegment"
    );
    final Integer realMaxRowsPerSegment = target.getValue() == null ? maxRowsPerSegment : target.getValue();
    Preconditions.checkArgument(
        PartitionsSpec.isEffectivelyNull(realMaxRowsPerSegment) || PartitionsSpec.isEffectivelyNull(numShards),
        "Can't use maxRowsPerSegment or " + target.getName() + " and numShards together"
    );
    // Needs to determine partitions if the _given_ numShards is null
    this.maxRowsPerSegment = getValidMaxRowsPerSegment(realMaxRowsPerSegment, numShards);
    this.numShards = PartitionsSpec.isEffectivelyNull(numShards) ? null : numShards;
    this.partitionDimensions = partitionDimensions == null ? Collections.emptyList() : partitionDimensions;

    Preconditions.checkArgument(
        this.maxRowsPerSegment == null || this.maxRowsPerSegment > 0,
        "maxRowsPerSegment[%s] should be positive",
        this.maxRowsPerSegment
    );
    Preconditions.checkArgument(
        this.numShards == null || this.numShards > 0,
        "numShards[%s] should be positive",
        this.numShards
    );

    final boolean needsPartitionDetermination = needsDeterminePartitions(numShards);
    if (!needsPartitionDetermination) {
      Preconditions.checkState(
          this.maxRowsPerSegment == null,
          "maxRowsPerSegment[%s] must be null if we don't need to determine partitions",
          this.maxRowsPerSegment
      );
      Preconditions.checkState(
          this.numShards != null,
          "numShards must not be null if we don't need to determine partitions"
      );
    }
  }

  public HashedPartitionsSpec(
      @Nullable Integer maxRowsPerSegment,
      @Nullable Integer numShards,
      @Nullable List<String> partitionDimensions
  )
  {
    this(null, numShards, partitionDimensions, null, maxRowsPerSegment);
  }


  private static boolean needsDeterminePartitions(@Nullable Integer numShards)
  {
    return PartitionsSpec.isEffectivelyNull(numShards);
  }

  @Nullable
  private static Integer getValidMaxRowsPerSegment(@Nullable Integer maxRowsPerSegment, @Nullable Integer numShards)
  {
    if (needsDeterminePartitions(numShards)) {
      return PartitionsSpec.isEffectivelyNull(maxRowsPerSegment) ? null : maxRowsPerSegment;
    } else {
      if (!PartitionsSpec.isEffectivelyNull(maxRowsPerSegment)) {
        LOG.warn("maxRowsPerSegment[%s] is ignored since numShards[%s] is specified", maxRowsPerSegment, numShards);
      }
      return null;
    }
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
