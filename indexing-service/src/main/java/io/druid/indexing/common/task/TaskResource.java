/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.task;

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
   *
   * @return task availability group
   */
  @JsonProperty
  public String getAvailabilityGroup()
  {
    return availabilityGroup;
  }


  /**
   * @return the number of worker slots this task will take
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
