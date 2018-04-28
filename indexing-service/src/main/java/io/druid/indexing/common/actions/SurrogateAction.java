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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.ISE;

/**
 * Perform the given action using {@link #surrogateId} on behalf of the caller task.
 */
public class SurrogateAction<R, T extends TaskAction<R>> implements TaskAction<R>
{
  private final String surrogateId;
  private final T taskAction;

  @JsonCreator
  public SurrogateAction(
      @JsonProperty("surrogateId") String surrogateId,
      @JsonProperty("taskAction") T taskAction
  )
  {
    this.surrogateId = surrogateId;
    this.taskAction = taskAction;
  }

  @JsonProperty
  public String getSurrogateId()
  {
    return surrogateId;
  }

  @JsonProperty
  public T getTaskAction()
  {
    return taskAction;
  }

  @Override
  public TypeReference<R> getReturnTypeReference()
  {
    return taskAction.getReturnTypeReference();
  }

  @Override
  public R perform(Task task, TaskActionToolbox toolbox)
  {
    final Optional<Task> maybeSurrogateTask = toolbox.getTaskStorage().getTask(surrogateId);
    if (maybeSurrogateTask.isPresent()) {
      return taskAction.perform(maybeSurrogateTask.get(), toolbox);
    } else {
      throw new ISE("Can't find surrogate task[%s]", surrogateId);
    }
  }

  @Override
  public boolean isAudited()
  {
    return taskAction.isAudited();
  }

  @Override
  public String toString()
  {
    return "SurrogateAction{" +
           "surrogateId='" + surrogateId + '\'' +
           ", taskAction=" + taskAction +
           '}';
  }
}
