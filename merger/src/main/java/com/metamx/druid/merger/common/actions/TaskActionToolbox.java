package com.metamx.druid.merger.common.actions;

import com.metamx.druid.merger.coordinator.MergerDBCoordinator;
import com.metamx.druid.merger.coordinator.TaskLockbox;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.emitter.service.ServiceEmitter;

public class TaskActionToolbox
{
  private final TaskQueue taskQueue;
  private final TaskLockbox taskLockbox;
  private final MergerDBCoordinator mergerDBCoordinator;
  private final ServiceEmitter emitter;

  public TaskActionToolbox(
      TaskQueue taskQueue,
      TaskLockbox taskLockbox,
      MergerDBCoordinator mergerDBCoordinator,
      ServiceEmitter emitter
  )
  {
    this.taskQueue = taskQueue;
    this.taskLockbox = taskLockbox;
    this.mergerDBCoordinator = mergerDBCoordinator;
    this.emitter = emitter;
  }

  public TaskQueue getTaskQueue()
  {
    return taskQueue;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public MergerDBCoordinator getMergerDBCoordinator()
  {
    return mergerDBCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }
}
