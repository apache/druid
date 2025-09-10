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
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.error.EntryAlreadyExists;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implements an in-heap TaskStorage facility, with no persistence across restarts. This class
 * is thread safe.
 */
public class HeapMemoryTaskStorage implements TaskStorage
{
  private final TaskStorageConfig config;

  private final ConcurrentHashMap<String, TaskInfo> tasks = new ConcurrentHashMap<>();

  @GuardedBy("itself")
  private final Multimap<String, TaskLock> taskLocks = HashMultimap.create();

  private static final Logger log = new Logger(HeapMemoryTaskStorage.class);

  @Inject
  public HeapMemoryTaskStorage(TaskStorageConfig config)
  {
    this.config = config;
  }

  @Override
  public TaskInfo insert(Task task, TaskStatus status)
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(
        task.getId().equals(status.getId()),
        "Task/Status ID mismatch[%s/%s]",
        task.getId(),
        status.getId()
    );

    final TaskInfo newTaskInfo = new TaskInfo(DateTimes.nowUtc(), status, task);
    final TaskInfo existingTaskInfo = tasks.putIfAbsent(task.getId(), newTaskInfo);
    if (existingTaskInfo != null) {
      throw EntryAlreadyExists.exception("Task[%s] already exists", task.getId());
    }

    log.info("Inserted task[%s] with status[%s]", task.getId(), status);
    return newTaskInfo;
  }

  @Override
  public Optional<Task> getTask(String taskid)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    TaskInfo taskInfo = tasks.get(taskid);
    if (taskInfo != null) {
      return Optional.of(taskInfo.getTask());
    } else {
      return Optional.absent();
    }
  }

  @Override
  public void setStatus(TaskStatus status)
  {
    Preconditions.checkNotNull(status, "status");
    final String taskid = status.getId();

    log.info("Updating task %s to status: %s", taskid, status);
    TaskInfo updated = tasks.computeIfPresent(taskid, (tid, taskInfo) -> {
      Preconditions.checkState(taskInfo.getStatus().isRunnable(), "Task must be runnable: %s", taskid);
      return taskInfo.withStatus(status);
    });
    Preconditions.checkNotNull(updated, "Task ID must already be present: %s", taskid);
  }

  @Override
  public Optional<TaskStatus> getStatus(String taskid)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    TaskInfo existing = tasks.get(taskid);
    if (existing != null) {
      return Optional.of(existing.getStatus());
    } else {
      return Optional.absent();
    }
  }

  @Nullable
  @Override
  public TaskInfo getTaskInfo(String taskId)
  {
    Preconditions.checkNotNull(taskId, "taskId");
    return tasks.get(taskId);
  }

  @Override
  public List<Task> getActiveTasks()
  {
    final ImmutableList.Builder<Task> listBuilder = ImmutableList.builder();
    for (final TaskInfo taskInfo : tasks.values()) {
      if (taskInfo.getStatus().isRunnable()) {
        listBuilder.add(taskInfo.getTask());
      }
    }
    return listBuilder.build();
  }

  @Override
  public List<TaskInfo> getActiveTaskInfos()
  {
    final ImmutableList.Builder<TaskInfo> listBuilder = ImmutableList.builder();
    for (final TaskInfo taskInfo : tasks.values()) {
      if (taskInfo.getStatus().isRunnable()) {
        listBuilder.add(taskInfo);
      }
    }
    return listBuilder.build();
  }

  @Override
  public List<Task> getActiveTasksByDatasource(String datasource)
  {
    final ImmutableList.Builder<Task> listBuilder = ImmutableList.builder();
    for (Map.Entry<String, TaskInfo> entry : tasks.entrySet()) {
      if (entry.getValue().getStatus().isRunnable() && entry.getValue().getDataSource().equals(datasource)) {
        listBuilder.add(entry.getValue().getTask());
      }
    }
    return listBuilder.build();
  }

  public List<TaskInfo> getActiveTaskInfo(@Nullable String dataSource)
  {
    final ImmutableList.Builder<TaskInfo> listBuilder = ImmutableList.builder();
    for (final TaskInfo taskInfo : tasks.values()) {
      if (taskInfo.getStatus().isRunnable()) {
        if (dataSource == null || dataSource.equals(taskInfo.getDataSource())) {
          listBuilder.add(taskInfo);
        }
      }
    }
    return listBuilder.build();
  }

  public List<TaskInfo> getRecentlyCreatedAlreadyFinishedTaskInfo(CompleteTaskLookup taskLookup)
  {
    final Ordering<TaskInfo> createdDateDesc = new Ordering<TaskInfo>()
    {
      @Override
      public int compare(TaskInfo a, TaskInfo b)
      {
        return a.getCreatedTime().compareTo(b.getCreatedTime());
      }
    }.reverse();

    return getRecentlyCreatedAlreadyFinishedTaskInfoSince(
        taskLookup.getTasksCreatedPriorTo(),
        taskLookup.getMaxTaskStatuses(),
        createdDateDesc
    );
  }

  /**
   * NOTE: This method is racy as it searches for complete tasks and active tasks separately outside a lock.
   * This method should be used only for testing.
   */
  @Override
  public List<TaskInfo> getTaskInfos(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String datasource
  )
  {
    final List<TaskInfo> tasks = new ArrayList<>();
    final Map<TaskLookupType, TaskLookup> processedTaskLookups =
        TaskStorageUtils.processTaskLookups(
            taskLookups,
            DateTimes.nowUtc().minus(config.getRecentlyFinishedThreshold())
        );

    processedTaskLookups.forEach((type, lookup) -> {
      if (type == TaskLookupType.COMPLETE) {
        tasks.addAll(getRecentlyCreatedAlreadyFinishedTaskInfo((CompleteTaskLookup) lookup));
      } else {
        tasks.addAll(getActiveTaskInfo(datasource));
      }
    });
    return tasks;
  }

  @Override
  public List<TaskStatusPlus> getTaskStatusPlusList(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String datasource
  )
  {
    return getTaskInfos(taskLookups, datasource).stream()
                                                .map(Task::toTaskIdentifierInfo)
                                                .map(TaskStatusPlus::fromTaskIdentifierInfo)
                                                .collect(Collectors.toList());
  }

  private List<TaskInfo> getRecentlyCreatedAlreadyFinishedTaskInfoSince(
      DateTime start,
      @Nullable Integer n,
      Ordering<TaskInfo> createdDateDesc
  )
  {
    Stream<TaskInfo> stream = tasks
        .values()
        .stream()
        .filter(taskInfo -> taskInfo.getStatus().isComplete() && taskInfo.getCreatedTime().isAfter(start))
        .sorted(createdDateDesc);
    if (n != null) {
      stream = stream.limit(n);
    }
    return stream.collect(Collectors.toUnmodifiableList());
  }

  @Override
  public void addLock(final String taskid, final TaskLock taskLock)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(taskLock, "taskLock");
    synchronized (taskLocks) {
      taskLocks.put(taskid, taskLock);
    }
  }

  @Override
  public void replaceLock(String taskid, TaskLock oldLock, TaskLock newLock)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    Preconditions.checkNotNull(oldLock, "oldLock");
    Preconditions.checkNotNull(newLock, "newLock");

    synchronized (taskLocks) {
      if (!taskLocks.remove(taskid, oldLock)) {
        log.warn("taskLock[%s] for replacement is not found for task[%s]", oldLock, taskid);
      }

      taskLocks.put(taskid, newLock);
    }
  }

  @Override
  public void removeLock(final String taskid, final TaskLock taskLock)
  {
    Preconditions.checkNotNull(taskLock, "taskLock");
    synchronized (taskLocks) {
      taskLocks.remove(taskid, taskLock);
    }
  }

  @Override
  public List<TaskLock> getLocks(final String taskid)
  {
    synchronized (taskLocks) {
      return ImmutableList.copyOf(taskLocks.get(taskid));
    }
  }

  @Override
  public void removeTasksOlderThan(final long timestamp)
  {
    // This is the only fn where both tasks & taskActions are modified for removal, they may
    // be added elsewhere.

    // It is possible that multiple calls here occur to removeTasksOlderThan() concurrently.
    // It is then possible that the same task will be queued for removal twice. Whilst not ideal,
    // it will not cause any problems.
    List<String> taskIds = tasks.entrySet().stream()
                                .filter(entry -> entry.getValue().getStatus().isComplete()
                                                 && entry.getValue().getCreatedTime().isBefore(timestamp))
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toList());

    taskIds.forEach(tasks::remove);
  }
}
