package com.metamx.druid.indexing.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.common.task.TaskResource;

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
