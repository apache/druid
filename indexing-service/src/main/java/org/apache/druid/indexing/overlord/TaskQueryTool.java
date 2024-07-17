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
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Provides read-only methods to fetch information related to tasks.
 * This class may serve information that is cached in memory in {@link TaskQueue}
 * or {@link TaskLockbox}. If not present in memory, then the underlying
 * {@link TaskStorage} is queried.
 */
public class TaskQueryTool
{
  private final TaskStorage storage;
  private final TaskLockbox taskLockbox;
  private final TaskMaster taskMaster;

  @Inject
  public TaskQueryTool(TaskStorage storage, TaskLockbox taskLockbox, TaskMaster taskMaster)
  {
    this.storage = storage;
    this.taskLockbox = taskLockbox;
    this.taskMaster = taskMaster;
  }

  /**
   * @param lockFilterPolicies Requests for conflicing lock intervals for various datasources
   * @return Map from datasource to intervals locked by tasks that have a conflicting lock type that cannot be revoked
   */
  public Map<String, List<Interval>> getLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
  {
    return taskLockbox.getLockedIntervals(lockFilterPolicies);
  }

  /**
   * Gets a List of Intervals locked by higher priority tasks for each datasource.
   *
   * @param minTaskPriority Minimum task priority for each datasource. Only the
   *                        Intervals that are locked by Tasks with equal or
   *                        higher priority than this are returned. Locked intervals
   *                        for datasources that are not present in this Map are
   *                        not returned.
   * @return Map from Datasource to List of Intervals locked by Tasks that have
   * priority greater than or equal to the {@code minTaskPriority} for that datasource.
   */
  public Map<String, List<Interval>> getLockedIntervals(Map<String, Integer> minTaskPriority)
  {
    return taskLockbox.getLockedIntervals(minTaskPriority);
  }

  public List<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(@Nullable String dataSource)
  {
    return storage.getTaskInfos(
        TaskLookup.activeTasksOnly(),
        dataSource
    );
  }

  public List<TaskStatusPlus> getTaskStatusPlusList(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String dataSource
  )
  {
    return storage.getTaskStatusPlusList(taskLookups, dataSource);
  }

  public Optional<Task> getTask(final String taskId)
  {
    final Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      Optional<Task> activeTask = taskQueue.get().getActiveTask(taskId);
      if (activeTask.isPresent()) {
        return activeTask;
      }
    }
    return storage.getTask(taskId);
  }

  public Optional<TaskStatus> getStatus(final String taskId)
  {
    return storage.getStatus(taskId);
  }

  @Nullable
  public TaskInfo<Task, TaskStatus> getTaskInfo(String taskId)
  {
    return storage.getTaskInfo(taskId);
  }

}
