package com.metamx.druid.indexing.common.actions;

import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.TaskStorage;
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
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    final RetType ret = taskAction.perform(task, toolbox);

    if (taskAction.isAudited()) {
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
    }

    return ret;
  }
}
