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

package io.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;

/**
 * Used by workers to announce the status of tasks they are currently running. This class is immutable.
 */
public class TaskAnnouncement
{
  private final TaskStatus taskStatus;
  private final TaskResource taskResource;

  public static TaskAnnouncement create(Task task, TaskStatus status)
  {
    return create(task.getId(), task.getTaskResource(), status);
  }

  public static TaskAnnouncement create(String taskId, TaskResource resource, TaskStatus status)
  {
    Preconditions.checkArgument(status.getId().equals(taskId), "task id == status id");
    return new TaskAnnouncement(null, null, status, resource);
  }

  @JsonCreator
  private TaskAnnouncement(
      @JsonProperty("id") String taskId,
      @JsonProperty("status") TaskStatus.Status status,
      @JsonProperty("taskStatus") TaskStatus taskStatus,
      @JsonProperty("taskResource") TaskResource taskResource
  )
  {
    if (taskStatus != null) {
      this.taskStatus = taskStatus;
    } else {
      // Can be removed when backwards compat is no longer needed
      this.taskStatus = TaskStatus.fromCode(taskId, status);
    }
    this.taskResource = taskResource == null ? new TaskResource(this.taskStatus.getId(), 1) : taskResource;
  }

  // Can be removed when backwards compat is no longer needed
  @JsonProperty("id")
  @Deprecated
  public String getTaskId()
  {
    return taskStatus.getId();
  }

  // Can be removed when backwards compat is no longer needed
  @JsonProperty("status")
  @Deprecated
  public TaskStatus.Status getStatus()
  {
    return taskStatus.getStatusCode();
  }

  @JsonProperty("taskStatus")
  public TaskStatus getTaskStatus()
  {
    return taskStatus;
  }

  @JsonProperty("taskResource")
  public TaskResource getTaskResource()
  {
    return taskResource;
  }
}
