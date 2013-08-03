package com.metamx.druid.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 */
public class BatchDataSegmentAnnouncerConfig
{
  @JsonProperty
  @Min(1)
  private int segmentsPerNode = 50;

  @JsonProperty
  @Max(1024 * 1024)
  @Min(1024)
  private long maxBytesPerNode = 512 * 1024;

  public int getSegmentsPerNode()
  {
    return segmentsPerNode;
  }

  public long getMaxBytesPerNode()
  {
    return maxBytesPerNode;
  }
}
