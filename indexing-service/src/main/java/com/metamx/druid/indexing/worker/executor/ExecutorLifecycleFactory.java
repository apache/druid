package com.metamx.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.indexing.coordinator.TaskRunner;

import java.io.File;

public class ExecutorLifecycleFactory
{
  private final File taskFile;
  private final File statusFile;

  public ExecutorLifecycleFactory(File taskFile, File statusFile)
  {
    this.taskFile = taskFile;
    this.statusFile = statusFile;
  }

  public ExecutorLifecycle build(TaskRunner taskRunner, ObjectMapper jsonMapper)
  {
    return new ExecutorLifecycle(
        new ExecutorLifecycleConfig().setTaskFile(taskFile).setStatusFile(statusFile), taskRunner, jsonMapper
    );
  }
}
