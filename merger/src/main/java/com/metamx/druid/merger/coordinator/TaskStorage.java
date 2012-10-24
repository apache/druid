package com.metamx.druid.merger.coordinator;

import com.google.common.base.Optional;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;

import java.util.List;

public interface TaskStorage
{
  /**
   * Adds a task to the storage facility with a particular status. If the task ID already exists, this method
   * will throw an exception.
   */
  public void insert(Task task, TaskStatus status);

  /**
   * Updates task status in the storage facility.
   */
  public void setStatus(String taskid, TaskStatus status);

  /**
   * Updates task version in the storage facility. If the task already has a version, this method will throw
   * an exception.
   */
  public void setVersion(String taskid, String version);

  /**
   * Returns task status as stored in the storage facility. If the task ID does not exist, this will return
   * an absentee Optional.
   */
  public Optional<TaskStatus> getStatus(String taskid);

  /**
   * Returns task version as stored in the storage facility. If the task ID does not exist, or if the task ID exists
   * but was not yet assigned a version, this will return an absentee Optional.
   */
  public Optional<String> getVersion(String taskid);

  /**
   * Returns a list of currently-running tasks as stored in the storage facility, in no particular order.
   */
  public List<Task> getRunningTasks();
}
