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

package org.apache.druid.indexing.scheduledbatch;

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.sql.http.SqlTaskStatus;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ScheduledBatchStatusTracker
{
  // Track supervisor -> tasks.
  private final ConcurrentHashMap<String, List<String>> supervisorToTaskIds = new ConcurrentHashMap<>();

  // Track task -> task status
  private final ConcurrentHashMap<String, TaskStatus> taskStatusMap = new ConcurrentHashMap<>();

  public void onTaskSubmitted(final String supervisorId, final SqlTaskStatus sqlTaskStatus)
  {
    final String taskId = sqlTaskStatus.getTaskId();
    final TaskStatus taskStatus = TaskStatus.fromCode(sqlTaskStatus.getTaskId(), sqlTaskStatus.getState());

    supervisorToTaskIds.computeIfAbsent(supervisorId, k -> new CopyOnWriteArrayList<>()).add(taskId);

    taskStatusMap.put(taskId, taskStatus);
  }

  public void onTaskCompleted(final String taskId, final TaskStatus taskStatus)
  {
    if (!taskStatusMap.containsKey(taskId)) {
      return;  // Task was not submitted by us
    }

    taskStatusMap.put(taskId, taskStatus);
  }

  public BatchSupervisorTaskStatus getSupervisorTasks(final String supervisorId)
  {
    final List<String> taskIds = supervisorToTaskIds.getOrDefault(supervisorId, Collections.emptyList());

    final Map<String, TaskStatus> submittedTasks = new HashMap<>();
    final Map<String, TaskStatus> completedTasks = new HashMap<>();

    for (String taskId : taskIds) {
      TaskStatus taskStatus = taskStatusMap.get(taskId);
      if (taskStatus != null) {
        if (taskStatus.isComplete()) {
          completedTasks.put(taskId, taskStatus);
        } else {
          submittedTasks.put(taskId, taskStatus);
        }
      }
    }

    return new BatchSupervisorTaskStatus(submittedTasks, completedTasks);
  }

  public static class BatchSupervisorTaskStatus
  {
    private final Map<String, TaskStatus> submittedTasks;
    private final Map<String, TaskStatus> completedTasks;

    public BatchSupervisorTaskStatus(
        final Map<String, TaskStatus> submittedTasks,
        final Map<String, TaskStatus> completedTasks
    )
    {
      this.submittedTasks = new HashMap<>(submittedTasks);
      this.completedTasks = new HashMap<>(completedTasks);
    }

    public Map<String, TaskStatus> getSubmittedTasks()
    {
      return submittedTasks;
    }

    public Map<String, TaskStatus> getCompletedTasks()
    {
      return completedTasks;
    }
  }
}
