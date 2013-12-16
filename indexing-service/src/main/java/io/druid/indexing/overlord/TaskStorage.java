/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.task.Task;

import java.util.List;

public interface TaskStorage
{
  /**
   * Adds a task to the storage facility with a particular status. If the task ID already exists, this method
   * will throw a {@link TaskExistsException}.
   */
  public void insert(Task task, TaskStatus status);

  /**
   * Persists task status in the storage facility. This method should throw an exception if the task status lifecycle
   * is not respected (absent -> RUNNING -> SUCCESS/FAILURE).
   */
  public void setStatus(TaskStatus status);

  /**
   * Persists lock state in the storage facility.
   */
  public void addLock(String taskid, TaskLock taskLock);

  /**
   * Removes lock state from the storage facility. It is harmless to keep old locks in the storage facility, but
   * this method can help reclaim wasted space.
   */
  public void removeLock(String taskid, TaskLock taskLock);

  /**
   * Returns task as stored in the storage facility. If the task ID does not exist, this will return an
   * absentee Optional.
   *
   * NOTE: This method really feels like it should be combined with {@link #getStatus}.  Expect that in the future.
   */
  public Optional<Task> getTask(String taskid);

  /**
   * Returns task status as stored in the storage facility. If the task ID does not exist, this will return
   * an absentee Optional.
   */
  public Optional<TaskStatus> getStatus(String taskid);

  /**
   * Add an action taken by a task to the audit log.
   */
  public <T> void addAuditLog(Task task, TaskAction<T> taskAction);

  /**
   * Returns all actions taken by a task.
   */
  public List<TaskAction> getAuditLogs(String taskid);

  /**
   * Returns a list of currently running or pending tasks as stored in the storage facility. No particular order
   * is guaranteed, but implementations are encouraged to return tasks in ascending order of creation.
   */
  public List<Task> getActiveTasks();

  /**
   * Returns a list of recently finished task statuses as stored in the storage facility. No particular order
   * is guaranteed, but implementations are encouraged to return tasks in descending order of creation. No particular
   * standard of "recent" is guaranteed, and in fact, this method is permitted to simply return nothing.
   */
  public List<TaskStatus> getRecentlyFinishedTaskStatuses();

  /**
   * Returns a list of locks for a particular task.
   */
  public List<TaskLock> getLocks(String taskid);
}
