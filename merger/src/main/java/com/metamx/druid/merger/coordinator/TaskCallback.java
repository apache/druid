package com.metamx.druid.merger.coordinator;

import com.metamx.druid.merger.common.TaskStatus;

public interface TaskCallback
{
  public void notify(TaskStatus status);
}
