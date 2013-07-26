package com.metamx.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class TaskResource
{
  private final String availabilityGroup;
  private final int requiredCapacity;

  @JsonCreator
  public TaskResource(
      @JsonProperty("availabilityGroup") String availabilityGroup,
      @JsonProperty("requiredCapacity") int requiredCapacity
  )
  {
    this.availabilityGroup = availabilityGroup;
    this.requiredCapacity = requiredCapacity;
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


  /**
   * Returns the number of worker slots this task will take.
   */
  @JsonProperty
  public int getRequiredCapacity()
  {
    return requiredCapacity;
  }

  @Override
  public String toString()
  {
    return "TaskResource{" +
           "availabilityGroup='" + availabilityGroup + '\'' +
           ", requiredCapacity=" + requiredCapacity +
           '}';
  }
}
