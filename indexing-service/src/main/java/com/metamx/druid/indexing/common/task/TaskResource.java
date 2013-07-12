package com.metamx.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class TaskResource
{
  private final String availabilityGroup;
  private final int capacity;

  @JsonCreator
  public TaskResource(
      @JsonProperty("availabilityGroup") String availabilityGroup,
      @JsonProperty("capacity") int capacity
  )
  {
    this.availabilityGroup = availabilityGroup;
    this.capacity = capacity;
  }

  /**
   * Returns availability group ID of this task. Tasks the same availability group cannot be assigned to the same
   * worker. If tasks do not have this restriction, a common convention is to set the availability group ID to the
   * task ID.
   */
  @JsonProperty
  public String getAvailabilityGroup()
  {
    return availabilityGroup;
  }

  @JsonProperty
  public int getCapacity()
  {
    return capacity;
  }
}
