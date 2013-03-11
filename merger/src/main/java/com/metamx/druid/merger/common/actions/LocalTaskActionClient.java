package com.metamx.druid.merger.common.actions;

import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.TaskStorage;
import com.metamx.emitter.EmittingLogger;

import java.io.IOException;

public class LocalTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final TaskStorage storage;
  private final TaskActionToolbox toolbox;

  private static final EmittingLogger log = new EmittingLogger(LocalTaskActionClient.class);

  public LocalTaskActionClient(Task task, TaskStorage storage, TaskActionToolbox toolbox)
  {
    this.task = task;
    this.storage = storage;
    this.toolbox = toolbox;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    final RetType ret = taskAction.perform(task, toolbox);

    // Add audit log
    try {
      storage.addAuditLog(task, taskAction);
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to record action in audit log")
         .addData("task", task.getId())
         .addData("actionClass", taskAction.getClass().getName())
         .emit();
    }

    return ret;
  }
}
