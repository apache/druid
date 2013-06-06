package com.metamx.druid.indexing.common.actions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.metamx.druid.indexing.common.TaskLock;
import com.metamx.druid.indexing.common.task.Task;

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
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "LockListAction{}";
  }
}
