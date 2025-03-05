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

import org.apache.druid.indexer.TaskState;
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
 * It also keeps track of summary total task counts (active, successful, failed) per supervisor and ensures that
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
   * Tracks supervisor ID -> aggregate task status summary.
   */
  private final ConcurrentHashMap<String, SupervisorStatusSummary> supervisorToStatusSummary = new ConcurrentHashMap<>();

  /**
   * Tracks the recent set of task ID -> task status for all tasks younger than {@link #MAX_STATUS_RETAIN_DURATION}.
   */
  private final ConcurrentHashMap<String, BatchSupervisorTaskStatus> recentTaskStatusMap = new ConcurrentHashMap<>();

  public void onTaskSubmitted(final String supervisorId, final SqlTaskStatus sqlTaskStatus)
  {
    final String taskId = sqlTaskStatus.getTaskId();

    supervisorToTaskIds.computeIfAbsent(supervisorId, k -> new CopyOnWriteArrayList<>()).add(taskId);
    recentTaskStatusMap.put(taskId, new BatchSupervisorTaskStatus(
        supervisorId,
        TaskStatus.fromCode(sqlTaskStatus.getTaskId(), sqlTaskStatus.getState()), DateTimes.nowUtc()
    ));
    supervisorToStatusSummary.computeIfAbsent(supervisorId, k -> new SupervisorStatusSummary())
                             .incrementCount(TaskState.RUNNING);
  }

  public void onTaskCompleted(final String taskId, final TaskStatus taskStatus)
  {
    final BatchSupervisorTaskStatus supervisorTaskStatus = recentTaskStatusMap.get(taskId);
    if (supervisorTaskStatus == null) {
      return;  // Task was not submitted by a batch supervisor
    }

    final String supervisorId = supervisorTaskStatus.getSupervisorId();
    recentTaskStatusMap.put(taskId, new BatchSupervisorTaskStatus(supervisorId, taskStatus, DateTimes.nowUtc()));
    supervisorToStatusSummary.computeIfAbsent(supervisorId, k -> new SupervisorStatusSummary())
                             .incrementCount(taskStatus.getStatusCode());
  }

  public BatchSupervisorTaskReport getSupervisorTaskReport(final String supervisorId)
  {
    final List<String> taskIds = supervisorToTaskIds.getOrDefault(supervisorId, Collections.emptyList());

    final List<BatchSupervisorTaskStatus> recentTasks = new ArrayList<>();

    for (String taskId : taskIds) {
      BatchSupervisorTaskStatus taskStatus = recentTaskStatusMap.get(taskId);
      if (taskStatus != null) {
        recentTasks.add(taskStatus);
      }
    }

    final SupervisorStatusSummary summary = supervisorToStatusSummary.getOrDefault(supervisorId, new SupervisorStatusSummary());
    return new BatchSupervisorTaskReport(
        summary.getTotalSubmitted(),
        summary.getTotalSuccessful(),
        summary.getTotalFailed(),
        recentTasks
    );
  }

  /**
   * Cleanup stale tasks that are older than {@link #MAX_STATUS_RETAIN_DURATION} associated with
   * the specified {@code supervisorId}.
   */
  public void cleanupStaleTaskStatuses(String supervisorId)
  {
    final List<String> taskIds = supervisorToTaskIds.get(supervisorId);
    if (taskIds == null) {
      return;
    }
    final Set<String> staleTaskIds = new HashSet<>();
    final DateTime expiryThreshold = DateTimes.nowUtc().minus(MAX_STATUS_RETAIN_DURATION);

    taskIds.forEach(taskId -> {
      final BatchSupervisorTaskStatus status = recentTaskStatusMap.get(taskId);
      if (status != null && status.getUpdatedTime().isBefore(expiryThreshold)) {
        staleTaskIds.add(taskId);
      }
    });

    staleTaskIds.forEach(recentTaskStatusMap::remove);
  }

  /**
   * Tracks the aggregate counts of submitted, successful and failed tasks for a batch supervisor.
   */
  private static class SupervisorStatusSummary
  {
    private final AtomicInteger totalSubmitted = new AtomicInteger(0);
    private final AtomicInteger totalSuccessful = new AtomicInteger(0);
    private final AtomicInteger totalFailed = new AtomicInteger(0);

    void incrementCount(TaskState state)
    {
      if (state.isRunnable()) {
        totalSubmitted.incrementAndGet();
      } else if (state.isSuccess()) {
        totalSuccessful.incrementAndGet();
      } else {
        totalFailed.incrementAndGet();
      }
    }

    int getTotalSubmitted()
    {
      return totalSubmitted.get();
    }

    int getTotalSuccessful()
    {
      return totalSuccessful.get();
    }

    int getTotalFailed()
    {
      return totalFailed.get();
    }
  }
}
