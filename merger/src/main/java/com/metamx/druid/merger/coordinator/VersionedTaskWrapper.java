package com.metamx.druid.merger.coordinator;

import com.metamx.druid.merger.common.task.Task;

public class VersionedTaskWrapper
{
  final Task task;
  final String version;

  public VersionedTaskWrapper(Task task, String version)
  {
    this.task = task;
    this.version = version;
  }

  public Task getTask()
  {
    return task;
  }

  public String getVersion()
  {
    return version;
  }
}
