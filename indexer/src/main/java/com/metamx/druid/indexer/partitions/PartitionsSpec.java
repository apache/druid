package com.metamx.druid.indexer.partitions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

public class PartitionsSpec
{
  private static final double DEFAULT_OVERSIZE_THRESHOLD = 1.5;

  @Nullable
  private final String partitionDimension;

  private final long targetPartitionSize;

  private final long maxPartitionSize;

  private final boolean assumeGrouped;

  @JsonCreator
  public PartitionsSpec(
      @JsonProperty("partitionDimension") @Nullable String partitionDimension,
      @JsonProperty("targetPartitionSize") @Nullable Long targetPartitionSize,
      @JsonProperty("maxPartitionSize") @Nullable Long maxPartitionSize,
      @JsonProperty("assumeGrouped") @Nullable Boolean assumeGrouped
  )
  {
    this.partitionDimension = partitionDimension;
    this.targetPartitionSize = targetPartitionSize == null ? -1 : targetPartitionSize;
    this.maxPartitionSize = maxPartitionSize == null
                            ? (long) (this.targetPartitionSize * DEFAULT_OVERSIZE_THRESHOLD)
                            : maxPartitionSize;
    this.assumeGrouped = assumeGrouped == null ? false : assumeGrouped;
  }

  @JsonIgnore
  public boolean isDeterminingPartitions()
  {
    return targetPartitionSize > 0;
  }

  @JsonProperty
  @Nullable
  public String getPartitionDimension()
  {
    return partitionDimension;
  }

  @JsonProperty
  public long getTargetPartitionSize()
  {
    return targetPartitionSize;
  }

  @JsonProperty
  public long getMaxPartitionSize()
  {
    return maxPartitionSize;
  }

  @JsonProperty
  public boolean isAssumeGrouped()
  {
    return assumeGrouped;
  }
}
