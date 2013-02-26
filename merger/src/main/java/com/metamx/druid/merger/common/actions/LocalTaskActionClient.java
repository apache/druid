package com.metamx.druid.merger.common.actions;

import com.metamx.druid.merger.coordinator.TaskStorage;
import com.metamx.emitter.EmittingLogger;

public class LocalTaskActionClient implements TaskActionClient
{
  private final TaskStorage storage;
  private final TaskActionToolbox toolbox;

  private static final EmittingLogger log = new EmittingLogger(LocalTaskActionClient.class);

  public LocalTaskActionClient(TaskStorage storage, TaskActionToolbox toolbox)
  {
    this.storage = storage;
    this.toolbox = toolbox;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    final RetType ret = taskAction.perform(toolbox);

    // Add audit log
    try {
      storage.addAuditLog(taskAction);
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to record action in audit log")
         .addData("task", taskAction.getTask().getId())
         .addData("actionClass", taskAction.getClass().getName())
         .emit();
    }

    return ret;
  }
}
