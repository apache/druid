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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

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
    this.availabilityGroup = Preconditions.checkNotNull(availabilityGroup, "availabilityGroup");
    Preconditions.checkArgument(requiredCapacity > 0);
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
