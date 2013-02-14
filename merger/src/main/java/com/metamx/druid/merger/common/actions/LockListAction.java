package com.metamx.druid.merger.common.actions;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class LockListAction implements TaskAction<List<TaskLock>>
{
  private final Task task;

  @JsonCreator
  public LockListAction(
      @JsonProperty("task") Task task
  )
  {
    this.task = task;
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
  }

  public TypeReference<List<TaskLock>> getReturnTypeReference()
  {
    return new TypeReference<List<TaskLock>>() {};
  }

  @Override
  public List<TaskLock> perform(TaskActionToolbox toolbox)
  {
    try {
      return toolbox.getTaskLockbox().findLocksForTask(task);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
