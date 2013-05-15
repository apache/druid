package com.metamx.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.indexing.coordinator.TaskRunner;

import java.io.File;
import java.io.InputStream;

public class ExecutorLifecycleFactory
{
  private final File taskFile;
  private final File statusFile;
  private final InputStream parentStream;

  public ExecutorLifecycleFactory(File taskFile, File statusFile, InputStream parentStream)
  {
    this.taskFile = taskFile;
    this.statusFile = statusFile;
    this.parentStream = parentStream;
  }

  public ExecutorLifecycle build(TaskRunner taskRunner, ObjectMapper jsonMapper)
  {
    return new ExecutorLifecycle(taskFile, statusFile, taskRunner, parentStream, jsonMapper);
  }
}
