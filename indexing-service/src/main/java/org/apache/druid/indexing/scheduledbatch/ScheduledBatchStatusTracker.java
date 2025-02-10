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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.http.SqlTaskStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks task statuses upon submission and completion for scheduled batch supervisors.
 * <p>
 * This class maintains per-supervisor mappings of submitted tasks and their statuses.
 * It also keeps track of total task counts (active, successful, failed) and ensures that
 * recently tracked task statuses are retained for a limited duration ({@link #MAX_STATUS_RETAIN_DURATION}).
 * </p>
 */
public class ScheduledBatchStatusTracker
{
  private static final Duration MAX_STATUS_RETAIN_DURATION = Duration.standardDays(2);

  /**
   * Track supervisor ID -> task IDs.
   */
  private final ConcurrentHashMap<String, List<String>> supervisorToTaskIds = new ConcurrentHashMap<>();

  /**
   * Track the task ID -> supervisor ID for reverse lookup to update the total counts.
   */
  private final ConcurrentHashMap<String, String> taskIdToSupervisorId = new ConcurrentHashMap<>();

  /**
   * Tracks the recent set of task ID -> task status for all tasks younger than {@link #MAX_STATUS_RETAIN_DURATION}.
   */
  private final ConcurrentHashMap<String, BatchSupervisorTaskStatus> recentTaskStatusMap = new ConcurrentHashMap<>();


  private final ConcurrentHashMap<String, AtomicInteger> supervisorTotalSubmittedTasks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicInteger> supervisorTotalSuccessfulTasks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicInteger> supervisorTotalFailedTasks = new ConcurrentHashMap<>();


  public void onTaskSubmitted(final String supervisorId, final SqlTaskStatus sqlTaskStatus)
  {
    final String taskId = sqlTaskStatus.getTaskId();

    supervisorToTaskIds.computeIfAbsent(supervisorId, k -> new CopyOnWriteArrayList<>()).add(taskId);
    taskIdToSupervisorId.put(taskId, supervisorId);
    recentTaskStatusMap.put(taskId, new BatchSupervisorTaskStatus(
        TaskStatus.fromCode(sqlTaskStatus.getTaskId(), sqlTaskStatus.getState()),
        DateTimes.nowUtc()
    ));
    supervisorTotalSubmittedTasks.computeIfAbsent(supervisorId, k -> new AtomicInteger(0)).incrementAndGet();

  }

  public void onTaskCompleted(final String taskId, final TaskStatus taskStatus)
  {
    if (!recentTaskStatusMap.containsKey(taskId)) {
      return;  // Task was not submitted by us
    }

    final String supervisorId = taskIdToSupervisorId.get(taskId);
    if (taskStatus.isSuccess()) {
      supervisorTotalSuccessfulTasks.computeIfAbsent(supervisorId, k -> new AtomicInteger(0)).incrementAndGet();
    } else {
      supervisorTotalFailedTasks.computeIfAbsent(supervisorId, k -> new AtomicInteger(0)).incrementAndGet();
    }

    recentTaskStatusMap.put(taskId, new BatchSupervisorTaskStatus(taskStatus, DateTimes.nowUtc()));
  }

  public BatchSupervisorTaskReport getSupervisorTaskStatus(final String supervisorId)
  {
    final List<String> taskIds = supervisorToTaskIds.getOrDefault(supervisorId, Collections.emptyList());

    final List<BatchSupervisorTaskStatus> recentActiveTasks = new ArrayList<>();
    final List<BatchSupervisorTaskStatus> recentSuccessfulTasks = new ArrayList<>();
    final List<BatchSupervisorTaskStatus> recentFailedTasks = new ArrayList<>();

    for (String taskId : taskIds) {
      BatchSupervisorTaskStatus taskStatus = recentTaskStatusMap.get(taskId);
      if (taskStatus != null) {
        TaskStatus status = taskStatus.getStatus();
        if (status.isComplete()) {
          if (status.isSuccess()) {
            recentSuccessfulTasks.add(taskStatus);
          } else {
            recentFailedTasks.add(taskStatus);
          }
        } else {
          recentActiveTasks.add(taskStatus);
        }
      }
    }

    return new BatchSupervisorTaskReport(
        supervisorTotalSubmittedTasks.getOrDefault(supervisorId, new AtomicInteger(0)).get(),
        supervisorTotalSuccessfulTasks.getOrDefault(supervisorId, new AtomicInteger(0)).get(),
        supervisorTotalFailedTasks.getOrDefault(supervisorId, new AtomicInteger(0)).get(),
        recentActiveTasks,
        recentSuccessfulTasks,
        recentFailedTasks
    );
  }

  public void cleanupStaleTaskStatuses()
  {
    final DateTime expiryThreshold = DateTimes.nowUtc().minus(MAX_STATUS_RETAIN_DURATION);

    final Set<String> staleTaskIds = new HashSet<>();

    // Remove expired tasks from taskStatusMap
    recentTaskStatusMap.forEach((taskId, status) -> {
      if (status.getUpdatedTime().isBefore(expiryThreshold)) {
        staleTaskIds.add(taskId);
      }
    });

    staleTaskIds.forEach(recentTaskStatusMap::remove);
  }
}
