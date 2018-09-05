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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;

import javax.annotation.Nullable;

/**
 * Used by workers to announce the status of tasks they are currently running. This class is immutable.
 */
public class TaskAnnouncement
{
  private final String taskType;
  private final TaskStatus taskStatus;
  private final TaskResource taskResource;
  private final TaskLocation taskLocation;

  @Nullable
  private final String taskDataSource; // nullable for backward compatibility

  public static TaskAnnouncement create(Task task, TaskStatus status, TaskLocation location)
  {
    return create(task.getId(), task.getType(), task.getTaskResource(), status, location, task.getDataSource());
  }

  public static TaskAnnouncement create(
      String taskId,
      String taskType,
      TaskResource resource,
      TaskStatus status,
      TaskLocation location,
      String taskDataSource
  )
  {
    Preconditions.checkArgument(status.getId().equals(taskId), "task id == status id");
    return new TaskAnnouncement(null, taskType, null, status, resource, location, taskDataSource);
  }

  @JsonCreator
  private TaskAnnouncement(
      @JsonProperty("id") String taskId,
      @JsonProperty("type") String taskType,
      @JsonProperty("status") TaskState status,
      @JsonProperty("taskStatus") TaskStatus taskStatus,
      @JsonProperty("taskResource") TaskResource taskResource,
      @JsonProperty("taskLocation") TaskLocation taskLocation,
      @JsonProperty("taskDataSource") String taskDataSource
  )
  {
    this.taskType = taskType;
    if (taskStatus != null) {
      this.taskStatus = taskStatus;
    } else {
      // Can be removed when backwards compat is no longer needed
      this.taskStatus = TaskStatus.fromCode(taskId, status);
    }
    this.taskResource = taskResource == null ? new TaskResource(this.taskStatus.getId(), 1) : taskResource;
    this.taskLocation = taskLocation == null ? TaskLocation.unknown() : taskLocation;
    this.taskDataSource = taskDataSource;
  }

  @JsonProperty("id")
  public String getTaskId()
  {
    return taskStatus.getId();
  }

  @JsonProperty("type")
  public String getTaskType()
  {
    return taskType;
  }

  @JsonProperty("status")
  public TaskState getStatus()
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

  @JsonProperty("taskLocation")
  public TaskLocation getTaskLocation()
  {
    return taskLocation;
  }

  @JsonProperty("taskDataSource")
  public String getTaskDataSource()
  {
    return taskDataSource;
  }

  @Override
  public String toString()
  {
    return "TaskAnnouncement{" +
           "taskType=" + taskType +
           ", taskStatus=" + taskStatus +
           ", taskResource=" + taskResource +
           ", taskLocation=" + taskLocation +
           ", taskDataSource=" + taskDataSource +
           '}';
  }
}
