package com.metamx.druid.merger.common;

import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.TaskContext;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class TaskHolder
{
  private final Task task;
  private final TaskContext taskContext;

  @JsonCreator
  public TaskHolder(
      @JsonProperty("task") Task task,
      @JsonProperty("taskContext") TaskContext taskContext
  )
  {
    this.task = task;
    this.taskContext = taskContext;
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
  }

  @JsonProperty
  public TaskContext getTaskContext()
  {
    return taskContext;
  }
}
