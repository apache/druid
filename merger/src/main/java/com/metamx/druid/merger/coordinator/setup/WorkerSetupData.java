package com.metamx.druid.merger.coordinator.setup;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class WorkerSetupData
{
  private final String minVersion;
  private final int minNumWorkers;
  private final WorkerNodeData nodeData;
  private final WorkerUserData userData;

  @JsonCreator
  public WorkerSetupData(
      @JsonProperty("minVersion") String minVersion,
      @JsonProperty("minNumWorkers") int minNumWorkers,
      @JsonProperty("nodeData") WorkerNodeData nodeData,
      @JsonProperty("userData") WorkerUserData userData
  )
  {
    this.minVersion = minVersion;
    this.minNumWorkers = minNumWorkers;
    this.nodeData = nodeData;
    this.userData = userData;
  }

  @JsonProperty
  public String getMinVersion()
  {
    return minVersion;
  }

  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
  }

  @JsonProperty
  public WorkerNodeData getNodeData()
  {
    return nodeData;
  }

  @JsonProperty
  public WorkerUserData getUserData()
  {
    return userData;
  }
}
