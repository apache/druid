package com.metamx.druid.merger.common.actions;

import java.io.IOException;

public interface TaskActionClient
{
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException;
}
