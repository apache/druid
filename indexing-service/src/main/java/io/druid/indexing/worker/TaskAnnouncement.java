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
    Preconditions.checkArgument(status.getId().equals(task.getId()), "task id == status id");
    return new TaskAnnouncement(null, null, status, task.getTaskResource());
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
