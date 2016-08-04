/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.indexing.common.actions;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import io.druid.indexing.common.task.Task;
import org.joda.time.Interval;

import java.io.IOException;

public class SetLockCriticalStateAction implements TaskAction<Boolean> {

  @JsonIgnore
  private final Interval interval;
  @JsonIgnore
  private final TaskLockCriticalState taskLockCriticalState;

  @JsonCreator
  public SetLockCriticalStateAction(
      @JsonProperty("interval") Interval interval,
      @JsonProperty("taskLockCriticalState") TaskLockCriticalState taskLockCriticalState
  )
  {
    this.taskLockCriticalState = taskLockCriticalState;
    this.interval = interval;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public TaskLockCriticalState getTaskLockCriticalState()
  {
    return taskLockCriticalState;
  }

  @Override
  public TypeReference<Boolean> getReturnTypeReference()
  {
    return new TypeReference<Boolean>()
    {
    };
  }

  @Override
  public Boolean perform(
      Task task, TaskActionToolbox toolbox
  ) throws IOException
  {
    return toolbox.getTaskLockbox().setTaskLockCriticalState(task, interval, taskLockCriticalState);
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString(){
    return "SetLockCriticalStateAction{ " +
           "Interval = " + interval + ", TaskLockCriticalState = " + taskLockCriticalState +
           " }";
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SetLockCriticalStateAction that = (SetLockCriticalStateAction) o;

    if (!getInterval().equals(that.getInterval())) {
      return false;
    }
    return getTaskLockCriticalState() == that.getTaskLockCriticalState();

  }

  @Override
  public int hashCode()
  {
    int result = getInterval().hashCode();
    result = 31 * result + getTaskLockCriticalState().hashCode();
    return result;
  }
}
