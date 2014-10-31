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
import com.metamx.common.Pair;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

public interface MetadataStorageActionHandler<TaskType, TaskStatusType, TaskActionType, TaskLockType>
{
  /* Insert stuff on the table */
  public void insert(
      String id,
      DateTime createdDate,
      String dataSource,
      TaskType task,
      boolean active,
      TaskStatusType status
  ) throws TaskExistsException;

  /* Insert stuff. @returns 1 if status of the task with the given id has been updated successfully */
  public boolean setStatus(String taskId, boolean active, TaskStatusType statusPayload);

  /* Retrieve a task with the given ID */
  public Optional<TaskType> getTask(String taskId);

  /* Retrieve a task status with the given ID */
  public Optional<TaskStatusType> getTaskStatus(String taskId);

  /* Retrieve active tasks */
  public List<Pair<TaskType, TaskStatusType>> getActiveTasksWithStatus();

  /* Retrieve task statuses that have been created sooner than the given time */
  public List<TaskStatusType> getRecentlyFinishedTaskStatuses(DateTime start);

  /* Add lock to the task with given ID */
  public int addLock(String taskId, TaskLockType lock);

  /* Remove taskLock with given ID */
  public int removeLock(long lockId);

  public int addAuditLog(String taskId, TaskActionType taskAction);

  /* Get logs for task with given ID */
  public List<TaskActionType> getTaskLogs(String taskId);

  /* Get locks for task with given ID */
  public Map<Long, TaskLockType> getTaskLocks(String taskId);
}
