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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;

import java.util.Objects;

/**
 * Notifies the Overlord that the location of a task is now updated.
 * Used when running K8s-based tasks in encapsulated mode.
 */
public class UpdateLocationAction implements TaskAction<Void>
{
  private final TaskLocation location;

  @JsonCreator
  public UpdateLocationAction(
      @JsonProperty("location") TaskLocation location
  )
  {
    this.location = location;
  }

  @JsonProperty
  public TaskLocation getLocation()
  {
    return location;
  }

  @Override
  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    Optional<TaskRunner> taskRunner = toolbox.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().updateLocation(task, location);
    }
    return null;
  }

  @Override
  public String toString()
  {
    return "UpdateLocationAction{" +
           "taskLocation=" + location +
           '}';
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    UpdateLocationAction that = (UpdateLocationAction) object;
    return Objects.equals(location, that.location);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(location);
  }
}
