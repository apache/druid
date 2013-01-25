/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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
   * Returns task as stored in the storage facility. If the task ID does not exist, this will return an
   * absentee Optional.
   *
   * TODO -- This method probably wants to be combined with {@link #getStatus}.
   */
  public Optional<Task> getTask(String taskid);

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
