package com.metamx.druid.indexer.partitions;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;

public class PartitionsSpec
{
  @Nullable
  private final String partitionDimension;

  private final long targetPartitionSize;

  private final boolean assumeGrouped;

  public PartitionsSpec(
      @JsonProperty("partitionDimension") @Nullable String partitionDimension,
      @JsonProperty("targetPartitionSize") @Nullable Long targetPartitionSize,
      @JsonProperty("assumeGrouped") @Nullable Boolean assumeGrouped
  )
  {
    this.partitionDimension = partitionDimension;
    this.targetPartitionSize = targetPartitionSize == null ? -1 : targetPartitionSize;
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
  public boolean isAssumeGrouped()
  {
    return assumeGrouped;
  }
}
