package com.metamx.druid.merger.coordinator.setup;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class EC2NodeData implements WorkerNodeData
{
  private final String amiId;
  private final String instanceType;
  private final int minInstances;
  private final int maxInstances;

  @JsonCreator
  public EC2NodeData(
      @JsonProperty("amiId") String amiId,
      @JsonProperty("instanceType") String instanceType,
      @JsonProperty("minInstances") int minInstances,
      @JsonProperty("maxInstances") int maxInstances
  )
  {
    this.amiId = amiId;
    this.instanceType = instanceType;
    this.minInstances = minInstances;
    this.maxInstances = maxInstances;
  }

  @JsonProperty
  public String getAmiId()
  {
    return amiId;
  }

  @JsonProperty
  public String getInstanceType()
  {
    return instanceType;
  }

  @JsonProperty
  public int getMinInstances()
  {
    return minInstances;
  }

  @JsonProperty
  public int getMaxInstances()
  {
    return maxInstances;
  }
}
