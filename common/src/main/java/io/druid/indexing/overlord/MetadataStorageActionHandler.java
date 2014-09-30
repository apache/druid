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

import java.util.List;
import java.util.Map;

public interface MetadataStorageActionHandler
{
  /* Insert stuff on the table */
  public void insert(
      String tableName,
      String id,
      String createdDate,
      String dataSource,
      byte[] payload,
      int active,
      byte[] statusPayload
  ) throws Exception;

  /* Insert stuff. @returns 1 if status of the task with the given id has been updated successfully */
  public int setStatus(String tableName, String Id, int active, byte[] statusPayload);

  /* Retrieve a task with the given ID */
  public List<Map<String, Object>> getTask(String tableName, String Id);

  /* Retrieve a task status with the given ID */
  public List<Map<String, Object>> getTaskStatus(String tableName, String Id);

  /* Retrieve active tasks */
  public List<Map<String, Object>> getActiveTasks(String tableName);

  /* Retrieve task statuses that have been created sooner than the given time */
  public List<Map<String, Object>> getRecentlyFinishedTaskStatuses(String tableName, String recent);

  /* Add lock to the task with given ID */
  public int addLock(String tableName, String Id, byte[] lock);

  /* Remove taskLock with given ID */
  public int removeLock(String tableName, long lockId);

  public int addAuditLog(String tableName, String Id, byte[] taskAction);

  /* Get logs for task with given ID */
  public List<Map<String, Object>> getTaskLogs(String tableName, String Id);

  /* Get locks for task with given ID */
  public List<Map<String, Object>> getTaskLocks(String tableName, String Id);
}
