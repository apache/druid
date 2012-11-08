package com.metamx.druid.merger.coordinator.scaling;

import java.util.List;

/**
 */
public class AutoScalingData<T>
{
  private final List<String> nodeIds;
  private final List<T> nodes;

  public AutoScalingData(List<String> nodeIds, List<T> nodes)
  {
    this.nodeIds = nodeIds;
    this.nodes = nodes;
  }

  public List<String> getNodeIds()
  {
    return nodeIds;
  }

  public List<T> getNodes()
  {
    return nodes;
  }
}
