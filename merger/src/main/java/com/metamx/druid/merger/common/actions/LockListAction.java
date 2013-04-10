package com.metamx.druid.merger.common.actions;

import com.metamx.druid.merger.common.TaskLock;
import com.metamx.druid.merger.common.task.Task;
import com.fasterxml.jackson.core.type.TypeReference;

import java.util.List;

public class LockListAction implements TaskAction<List<TaskLock>>
{
  public TypeReference<List<TaskLock>> getReturnTypeReference()
  {
    return new TypeReference<List<TaskLock>>() {};
  }

  @Override
  public List<TaskLock> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getTaskLockbox().findLocksForTask(task);
  }

  @Override
  public String toString()
  {
    return "LockListAction{}";
  }
}
