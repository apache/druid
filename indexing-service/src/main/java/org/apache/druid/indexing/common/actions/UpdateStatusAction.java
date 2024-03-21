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
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;

import java.util.Objects;

public class UpdateStatusAction implements TaskAction<Void>
{
  private final String status;
  private final TaskStatus statusFull;

  @Deprecated
  public UpdateStatusAction(
      String status
  )
  {
    this(status, null);
  }

  @JsonCreator
  public UpdateStatusAction(
      @JsonProperty("status") String status,
      @JsonProperty("statusFull") TaskStatus statusFull
  )
  {
    this.status = status;
    this.statusFull = statusFull;
  }


  @JsonProperty
  public String getStatus()
  {
    return status;
  }

  @JsonProperty
  public TaskStatus getStatusFull()
  {
    return statusFull;
  }

  @Override
  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>()
    {
    };
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    Optional<TaskRunner> taskRunner = toolbox.getTaskRunner();
    if (taskRunner.isPresent()) {
      // Fall back to checking status instead of statusFull for backwards compatibility
      TaskStatus result = statusFull != null ? statusFull : "successful".equals(status) ? TaskStatus.success(task.getId()) : TaskStatus.failure(task.getId(), "Error with task");
      taskRunner.get().updateStatus(task, result);
    }
    return null;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "UpdateStatusAction{" +
           "status=" + status +
           ", statusFull=" + statusFull +
           '}';
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
    UpdateStatusAction that = (UpdateStatusAction) o;
    return Objects.equals(status, that.status) && Objects.equals(statusFull, that.statusFull);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(status, statusFull);
  }
}
