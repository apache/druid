package com.metamx.druid.merger.coordinator;

import com.metamx.druid.merger.common.task.Task;

/**
 * Interface for handing off tasks. Used by a {@link com.metamx.druid.merger.coordinator.exec.TaskConsumer} to run tasks that
 * have been locked.
 */
public interface TaskRunner
{
  /**
   * Run a task with a particular context and call a callback. The callback should be called exactly once.
   *
   * @param task task to run
   * @param context task context to run under
   * @param callback callback to be called exactly once
   */
  public void run(Task task, TaskContext context, TaskCallback callback);
}
