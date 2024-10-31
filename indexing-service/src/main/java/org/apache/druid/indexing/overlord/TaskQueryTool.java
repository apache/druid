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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.http.TaskStateLookup;
import org.apache.druid.indexing.overlord.http.TotalWorkerCapacityResponse;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides read-only methods to fetch information related to tasks.
 * This class may serve information that is cached in memory in {@link TaskQueue}
 * or {@link TaskLockbox}. If not present in memory, then the underlying
 * {@link TaskStorage} is queried.
 */
public class TaskQueryTool
{
  private static final Logger log = new Logger(TaskQueryTool.class);

  private final TaskStorage storage;
  private final TaskLockbox taskLockbox;
  private final TaskMaster taskMaster;
  private final Supplier<WorkerBehaviorConfig> workerBehaviorConfigSupplier;
  private final ProvisioningStrategy provisioningStrategy;

  @Inject
  public TaskQueryTool(
      TaskStorage storage,
      TaskLockbox taskLockbox,
      TaskMaster taskMaster,
      ProvisioningStrategy provisioningStrategy,
      Supplier<WorkerBehaviorConfig> workerBehaviorConfigSupplier
  )
  {
    this.storage = storage;
    this.taskLockbox = taskLockbox;
    this.taskMaster = taskMaster;
    this.workerBehaviorConfigSupplier = workerBehaviorConfigSupplier;
    this.provisioningStrategy = provisioningStrategy;
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
   * @param lockFilterPolicies Requests for active locks for various datasources
   * @return Map from datasource to conflicting lock infos
   */
  public Map<String, List<TaskLock>> getActiveLocks(List<LockFilterPolicy> lockFilterPolicies)
  {
    return taskLockbox.getActiveLocks(lockFilterPolicies);
  }

  public List<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(@Nullable String dataSource)
  {
    return storage.getTaskInfos(TaskLookup.activeTasksOnly(), dataSource);
  }

  public Map<String, TaskStatus> getMultipleTaskStatuses(Set<String> taskIds)
  {
    final Map<String, TaskStatus> result = Maps.newHashMapWithExpectedSize(taskIds.size());
    for (String taskId : taskIds) {
      final Optional<TaskStatus> optional = getTaskStatus(taskId);
      if (optional.isPresent()) {
        result.put(taskId, optional.get());
      }
    }

    return result;
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

  public Optional<TaskStatus> getTaskStatus(final String taskId)
  {
    final Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getTaskStatus(taskId);
    } else {
      return storage.getStatus(taskId);
    }
  }

  @Nullable
  public TaskInfo<Task, TaskStatus> getTaskInfo(String taskId)
  {
    return storage.getTaskInfo(taskId);
  }

  public List<TaskStatusPlus> getAllActiveTasks()
  {
    final Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      // Serve active task statuses from memory
      final List<TaskStatusPlus> taskStatusPlusList = new ArrayList<>();

      // Use a dummy created time as this is not used by the caller, just needs to be non-null
      final DateTime createdTime = DateTimes.nowUtc();

      final List<Task> activeTasks = taskQueue.get().getTasks();
      for (Task task : activeTasks) {
        final Optional<TaskStatus> statusOptional = taskQueue.get().getTaskStatus(task.getId());
        if (statusOptional.isPresent()) {
          final TaskStatus status = statusOptional.get();
          taskStatusPlusList.add(
              new TaskStatusPlus(
                  task.getId(),
                  task.getGroupId(),
                  task.getType(),
                  createdTime,
                  createdTime,
                  status.getStatusCode(),
                  null,
                  status.getDuration(),
                  status.getLocation(),
                  task.getDataSource(),
                  status.getErrorMsg()
              )
          );
        }
      }

      return taskStatusPlusList;
    } else {
      return getTaskStatusPlusList(TaskStateLookup.ALL, null, null, 0, null);
    }
  }

  public List<TaskStatusPlus> getTaskStatusPlusList(
      TaskStateLookup state,
      @Nullable String dataSource,
      @Nullable String createdTimeInterval,
      @Nullable Integer maxCompletedTasks,
      @Nullable String type
  )
  {
    Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (!taskRunnerOptional.isPresent()) {
      return Collections.emptyList();
    }
    final TaskRunner taskRunner = taskRunnerOptional.get();

    final Duration createdTimeDuration;
    if (createdTimeInterval != null) {
      final Interval theInterval = Intervals.of(StringUtils.replace(createdTimeInterval, "_", "/"));
      createdTimeDuration = theInterval.toDuration();
    } else {
      createdTimeDuration = null;
    }

    // Ideally, snapshotting in taskStorage and taskRunner should be done atomically,
    // but there is no way to do it today.
    // Instead, we first gets a snapshot from taskStorage and then one from taskRunner.
    // This way, we can use the snapshot from taskStorage as the source of truth for the set of tasks to process
    // and use the snapshot from taskRunner as a reference for potential task state updates happened
    // after the first snapshotting.
    Stream<TaskStatusPlus> taskStatusPlusStream = getTaskStatusPlusStream(
        state,
        dataSource,
        createdTimeDuration,
        maxCompletedTasks,
        type
    );
    final Map<String, ? extends TaskRunnerWorkItem> runnerWorkItems = getTaskRunnerWorkItems(
        taskRunner,
        state,
        dataSource,
        type
    );

    if (state == TaskStateLookup.PENDING || state == TaskStateLookup.RUNNING) {
      // We are interested in only those tasks which are in taskRunner.
      taskStatusPlusStream = taskStatusPlusStream
          .filter(statusPlus -> runnerWorkItems.containsKey(statusPlus.getId()));
    }
    final List<TaskStatusPlus> taskStatusPlusList = taskStatusPlusStream.collect(Collectors.toList());

    // Separate complete and active tasks from taskStorage.
    // Note that taskStorage can return only either complete tasks or active tasks per TaskLookupType.
    final List<TaskStatusPlus> completeTaskStatusPlusList = new ArrayList<>();
    final List<TaskStatusPlus> activeTaskStatusPlusList = new ArrayList<>();
    for (TaskStatusPlus statusPlus : taskStatusPlusList) {
      if (statusPlus.getStatusCode().isComplete()) {
        completeTaskStatusPlusList.add(statusPlus);
      } else {
        activeTaskStatusPlusList.add(statusPlus);
      }
    }

    final List<TaskStatusPlus> taskStatuses = new ArrayList<>(completeTaskStatusPlusList);

    activeTaskStatusPlusList.forEach(statusPlus -> {
      final TaskRunnerWorkItem runnerWorkItem = runnerWorkItems.get(statusPlus.getId());
      if (runnerWorkItem == null) {
        // a task is assumed to be a waiting task if it exists in taskStorage but not in taskRunner.
        if (state == TaskStateLookup.WAITING || state == TaskStateLookup.ALL) {
          taskStatuses.add(statusPlus);
        }
      } else {
        if (state == TaskStateLookup.PENDING || state == TaskStateLookup.RUNNING || state == TaskStateLookup.ALL) {
          taskStatuses.add(
              new TaskStatusPlus(
                  statusPlus.getId(),
                  statusPlus.getGroupId(),
                  statusPlus.getType(),
                  statusPlus.getCreatedTime(),
                  runnerWorkItem.getQueueInsertionTime(),
                  statusPlus.getStatusCode(),
                  taskRunner.getRunnerTaskState(statusPlus.getId()), // this is racy for remoteTaskRunner
                  statusPlus.getDuration(),
                  runnerWorkItem.getLocation(), // location in taskInfo is only updated after the task is done.
                  statusPlus.getDataSource(),
                  statusPlus.getErrorMsg()
              )
          );
        }
      }
    });

    return taskStatuses;
  }

  private Stream<TaskStatusPlus> getTaskStatusPlusStream(
      TaskStateLookup state,
      @Nullable String dataSource,
      Duration createdTimeDuration,
      @Nullable Integer maxCompletedTasks,
      @Nullable String type
  )
  {
    final Map<TaskLookupType, TaskLookup> taskLookups;
    switch (state) {
      case ALL:
        taskLookups = ImmutableMap.of(
            TaskLookupType.ACTIVE,
            TaskLookup.ActiveTaskLookup.getInstance(),
            TaskLookupType.COMPLETE,
            TaskLookup.CompleteTaskLookup.of(maxCompletedTasks, createdTimeDuration)
        );
        break;
      case COMPLETE:
        taskLookups = ImmutableMap.of(
            TaskLookupType.COMPLETE,
            TaskLookup.CompleteTaskLookup.of(maxCompletedTasks, createdTimeDuration)
        );
        break;
      case WAITING:
      case PENDING:
      case RUNNING:
        taskLookups = ImmutableMap.of(
            TaskLookupType.ACTIVE,
            TaskLookup.ActiveTaskLookup.getInstance()
        );
        break;
      default:
        throw new IAE("Unknown state: [%s]", state);
    }

    final Stream<TaskStatusPlus> taskStatusPlusStream
        = storage.getTaskStatusPlusList(taskLookups, dataSource).stream();
    if (type != null) {
      return taskStatusPlusStream.filter(
          statusPlus -> type.equals(statusPlus == null ? null : statusPlus.getType())
      );
    } else {
      return taskStatusPlusStream;
    }
  }

  private Map<String, ? extends TaskRunnerWorkItem> getTaskRunnerWorkItems(
      TaskRunner taskRunner,
      TaskStateLookup state,
      @Nullable String dataSource,
      @Nullable String type
  )
  {
    Stream<? extends TaskRunnerWorkItem> runnerWorkItemsStream;
    switch (state) {
      case ALL:
      case WAITING:
        // waiting tasks can be found by (all tasks in taskStorage - all tasks in taskRunner)
        runnerWorkItemsStream = taskRunner.getKnownTasks().stream();
        break;
      case PENDING:
        runnerWorkItemsStream = taskRunner.getPendingTasks().stream();
        break;
      case RUNNING:
        runnerWorkItemsStream = taskRunner.getRunningTasks().stream();
        break;
      case COMPLETE:
        runnerWorkItemsStream = Stream.empty();
        break;
      default:
        throw new IAE("Unknown state: [%s]", state);
    }
    if (dataSource != null) {
      runnerWorkItemsStream = runnerWorkItemsStream.filter(item -> dataSource.equals(item.getDataSource()));
    }
    if (type != null) {
      runnerWorkItemsStream = runnerWorkItemsStream.filter(item -> type.equals(item.getTaskType()));
    }
    return runnerWorkItemsStream
        .collect(Collectors.toMap(TaskRunnerWorkItem::getTaskId, item -> item));
  }

  public TotalWorkerCapacityResponse getTotalWorkerCapacity()
  {
    Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (!taskRunnerOptional.isPresent()) {
      return null;
    }
    TaskRunner taskRunner = taskRunnerOptional.get();

    return new TotalWorkerCapacityResponse(
        taskRunner.getTotalCapacity(),
        taskRunner.getMaximumCapacityWithAutoscale(),
        taskRunner.getUsedCapacity()
    );
  }

  public WorkerBehaviorConfig getLatestWorkerConfig()
  {
    return workerBehaviorConfigSupplier.get();
  }

}
