package com.metamx.druid.merger.common.actions;

public interface TaskActionClient
{
  public <RetType> RetType submit(TaskAction<RetType> taskAction);
}
