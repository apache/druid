/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord;

import com.google.common.base.Optional;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.metadata.EntryExistsException;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.List;

public interface TaskStorage
{
  /**
   * Adds a task to the storage facility with a particular status.
   *
   * @param task   task to add
   * @param status task status
   *
   * @throws EntryExistsException if the task ID already exists
   */
  void insert(Task task, TaskStatus status) throws EntryExistsException;

  /**
   * Persists task status in the storage facility. This method should throw an exception if the task status lifecycle
   * is not respected (absent -&gt; RUNNING -&gt; SUCCESS/FAILURE).
   *
   * @param status task status
   */
  void setStatus(TaskStatus status);

  /**
   * Persists lock state in the storage facility.
   *
   * @param taskid   task ID
   * @param taskLock lock state
   */
  void addLock(String taskid, TaskLock taskLock);

  /**
   * Replace the old lock with the new lock. This method is not thread-safe.
   *
   * @param taskid  an id of the task holding the old lock and new lock
   * @param oldLock old lock
   * @param newLock new lock
   */
  void replaceLock(String taskid, TaskLock oldLock, TaskLock newLock);

  /**
   * Removes lock state from the storage facility. It is harmless to keep old locks in the storage facility, but
   * this method can help reclaim wasted space.
   *
   * @param taskid   task ID
   * @param taskLock lock state
   */
  void removeLock(String taskid, TaskLock taskLock);

  /**
   * Remove the tasks created older than the given timestamp.
   *
   * @param timestamp timestamp in milliseconds
   */
  void removeTasksOlderThan(long timestamp);

  /**
   * Returns task as stored in the storage facility. If the task ID does not exist, this will return an
   * absentee Optional.
   * <p>
   * NOTE: This method really feels like it should be combined with {@link #getStatus}.  Expect that in the future.
   *
   * @param taskid task ID
   *
   * @return optional task
   */
  Optional<Task> getTask(String taskid);

  /**
   * Returns task status as stored in the storage facility. If the task ID does not exist, this will return
   * an absentee Optional.
   *
   * @param taskid task ID
   *
   * @return task status
   */
  Optional<TaskStatus> getStatus(String taskid);

  @Nullable
  TaskInfo<Task, TaskStatus> getTaskInfo(String taskId);

  /**
   * Add an action taken by a task to the audit log.
   *
   * @param task       task to record action for
   * @param taskAction task action to record
   * @param <T>        task action return type
   */
  @Deprecated
  <T> void addAuditLog(Task task, TaskAction<T> taskAction);

  /**
   * Returns all actions taken by a task.
   *
   * @param taskid task ID
   *
   * @return list of task actions
   */
  @Deprecated
  List<TaskAction> getAuditLogs(String taskid);

  /**
   * Returns a list of currently running or pending tasks as stored in the storage facility. No particular order
   * is guaranteed, but implementations are encouraged to return tasks in ascending order of creation.
   *
   * @return list of active tasks
   */
  List<Task> getActiveTasks();

  /**
   * Returns a list of currently running or pending tasks as stored in the storage facility. No particular order
   * is guaranteed, but implementations are encouraged to return tasks in ascending order of creation.
   *
   * @param datasource datasource
   *
   * @return list of {@link Task}
   */
  List<Task> getActiveTasksByDatasource(String datasource);

  /**
   * Returns a list of currently running or pending tasks as stored in the storage facility as {@link TaskInfo}. No
   * particular order is guaranteed, but implementations are encouraged to return tasks in ascending order of creation.
   *
   * @return list of {@link TaskInfo}
   */
  List<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(@Nullable String dataSource);

  /**
   * Returns up to {@code maxTaskStatuses} {@link TaskInfo} objects of recently finished tasks as stored in the storage
   * facility. No particular order is guaranteed, but implementations are encouraged to return tasks in descending order
   * of creation. No particular standard of "recent" is guaranteed, and in fact, this method is permitted to simply
   * return nothing.
   *
   * @param maxTaskStatuses   maxTaskStatuses
   * @param durationBeforeNow duration
   * @param datasource        datasource
   *
   * @return list of {@link TaskInfo}
   */
  List<TaskInfo<Task, TaskStatus>> getRecentlyCreatedAlreadyFinishedTaskInfo(
      @Nullable Integer maxTaskStatuses,
      @Nullable Duration durationBeforeNow,
      @Nullable String datasource
  );

  /**
   * Returns a list of locks for a particular task.
   *
   * @param taskid task ID
   *
   * @return list of TaskLocks for the given task
   */
  List<TaskLock> getLocks(String taskid);
}
