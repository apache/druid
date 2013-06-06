package com.metamx.druid.indexing.coordinator;

public class TaskExistsException extends RuntimeException
{
  private final String taskId;

  public TaskExistsException(String taskId, Throwable t)
  {
    super(String.format("Task exists: %s", taskId), t);
    this.taskId = taskId;
  }

  public TaskExistsException(String taskId)
  {
    this(taskId, null);
  }

  public String getTaskId()
  {
    return taskId;
  }
}
