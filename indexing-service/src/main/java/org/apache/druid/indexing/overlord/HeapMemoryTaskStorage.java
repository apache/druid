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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
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

  private final ConcurrentHashMap<String, TaskStuff> tasks = new ConcurrentHashMap<>();

  @GuardedBy("itself")
  private final Multimap<String, TaskLock> taskLocks = HashMultimap.create();
  @GuardedBy("itself")
  private final Multimap<String, TaskAction> taskActions = ArrayListMultimap.create();

  private static final Logger log = new Logger(HeapMemoryTaskStorage.class);

  @Inject
  public HeapMemoryTaskStorage(TaskStorageConfig config)
  {
    this.config = config;
  }

  @Override
  public void insert(Task task, TaskStatus status) throws EntryExistsException
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(status, "status");
    Preconditions.checkArgument(
        task.getId().equals(status.getId()),
        "Task/Status ID mismatch[%s/%s]",
        task.getId(),
        status.getId()
    );

    TaskStuff newTaskStuff = new TaskStuff(task, status, DateTimes.nowUtc(), task.getDataSource());
    TaskStuff alreadyExisted = tasks.putIfAbsent(task.getId(), newTaskStuff);
    if (alreadyExisted != null) {
      throw new EntryExistsException(task.getId());
    }

    log.info("Inserted task %s with status: %s", task.getId(), status);
  }

  @Override
  public Optional<Task> getTask(String taskid)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    TaskStuff taskStuff = tasks.get(taskid);
    if (taskStuff != null) {
      return Optional.of(taskStuff.getTask());
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
    TaskStuff updated = tasks.computeIfPresent(taskid, (tid, taskStuff) -> {
      Preconditions.checkState(taskStuff.getStatus().isRunnable(), "Task must be runnable: %s", taskid);
      return taskStuff.withStatus(status);
    });
    Preconditions.checkNotNull(updated, "Task ID must already be present: %s", taskid);
  }

  @Override
  public Optional<TaskStatus> getStatus(String taskid)
  {
    Preconditions.checkNotNull(taskid, "taskid");
    TaskStuff existing = tasks.get(taskid);
    if (existing != null) {
      return Optional.of(existing.getStatus());
    } else {
      return Optional.absent();
    }
  }

  @Nullable
  @Override
  public TaskInfo<Task, TaskStatus> getTaskInfo(String taskId)
  {
    Preconditions.checkNotNull(taskId, "taskId");
    final TaskStuff taskStuff = tasks.get(taskId);
    if (taskStuff != null) {
      return TaskStuff.toTaskInfo(taskStuff);
    } else {
      return null;
    }
  }

  @Override
  public List<Task> getActiveTasks()
  {
    final ImmutableList.Builder<Task> listBuilder = ImmutableList.builder();
    for (final TaskStuff taskStuff : tasks.values()) {
      if (taskStuff.getStatus().isRunnable()) {
        listBuilder.add(taskStuff.getTask());
      }
    }
    return listBuilder.build();
  }

  @Override
  public List<Task> getActiveTasksByDatasource(String datasource)
  {
    final ImmutableList.Builder<Task> listBuilder = ImmutableList.builder();
    for (Map.Entry<String, TaskStuff> entry : tasks.entrySet()) {
      if (entry.getValue().getStatus().isRunnable() && entry.getValue().getDataSource().equals(datasource)) {
        listBuilder.add(entry.getValue().getTask());
      }
    }
    return listBuilder.build();
  }

  public List<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(@Nullable String dataSource)
  {
    final ImmutableList.Builder<TaskInfo<Task, TaskStatus>> listBuilder = ImmutableList.builder();
    for (final TaskStuff taskStuff : tasks.values()) {
      if (taskStuff.getStatus().isRunnable()) {
        if (dataSource == null || dataSource.equals(taskStuff.getDataSource())) {
          listBuilder.add(TaskStuff.toTaskInfo(taskStuff));
        }
      }
    }
    return listBuilder.build();
  }

  public List<TaskInfo<Task, TaskStatus>> getRecentlyCreatedAlreadyFinishedTaskInfo(
      CompleteTaskLookup taskLookup,
      @Nullable String datasource
  )
  {
    final Ordering<TaskStuff> createdDateDesc = new Ordering<TaskStuff>()
    {
      @Override
      public int compare(TaskStuff a, TaskStuff b)
      {
        return a.getCreatedDate().compareTo(b.getCreatedDate());
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
  public List<TaskInfo<Task, TaskStatus>> getTaskInfos(
      Map<TaskLookupType, TaskLookup> taskLookups,
      @Nullable String datasource
  )
  {
    final List<TaskInfo<Task, TaskStatus>> tasks = new ArrayList<>();
    taskLookups.forEach((type, lookup) -> {
      if (type == TaskLookupType.COMPLETE) {
        CompleteTaskLookup completeTaskLookup = (CompleteTaskLookup) lookup;
        tasks.addAll(
            getRecentlyCreatedAlreadyFinishedTaskInfo(
                completeTaskLookup.hasTaskCreatedTimeFilter()
                ? completeTaskLookup
                : completeTaskLookup.withDurationBeforeNow(config.getRecentlyFinishedThreshold()),
                datasource
            )
        );
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

  private List<TaskInfo<Task, TaskStatus>> getRecentlyCreatedAlreadyFinishedTaskInfoSince(
      DateTime start,
      @Nullable Integer n,
      Ordering<TaskStuff> createdDateDesc
  )
  {
    Stream<TaskStuff> stream = tasks
        .values()
        .stream()
        .filter(taskStuff -> taskStuff.getStatus().isComplete() && taskStuff.getCreatedDate().isAfter(start))
        .sorted(createdDateDesc);
    if (n != null) {
      stream = stream.limit(n);
    }
    List<TaskInfo<Task, TaskStatus>> list = stream
        .map(TaskStuff::toTaskInfo)
        .collect(Collectors.toList());
    return Collections.unmodifiableList(list);
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
                          && entry.getValue().getCreatedDate().isBefore(timestamp))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());

    taskIds.forEach(tasks::remove);
    synchronized (taskActions) {
      taskIds.forEach(taskActions::removeAll);
    }
  }

  @Deprecated
  @Override
  public <T> void addAuditLog(Task task, TaskAction<T> taskAction)
  {
    synchronized (taskActions) {
      taskActions.put(task.getId(), taskAction);
    }
  }

  @Deprecated
  @Override
  public List<TaskAction> getAuditLogs(String taskid)
  {
    synchronized (taskActions) {
      return ImmutableList.copyOf(taskActions.get(taskid));
    }
  }

  private static class TaskStuff
  {
    final Task task;
    final TaskStatus status;
    final DateTime createdDate;
    final String dataSource;

    private TaskStuff(Task task, TaskStatus status, DateTime createdDate, String dataSource)
    {
      Preconditions.checkArgument(task.getId().equals(status.getId()));

      this.task = Preconditions.checkNotNull(task, "task");
      this.status = Preconditions.checkNotNull(status, "status");
      this.createdDate = Preconditions.checkNotNull(createdDate, "createdDate");
      this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    }

    public Task getTask()
    {
      return task;
    }

    public TaskStatus getStatus()
    {
      return status;
    }

    public DateTime getCreatedDate()
    {
      return createdDate;
    }

    public String getDataSource()
    {
      return dataSource;
    }

    private TaskStuff withStatus(TaskStatus _status)
    {
      return new TaskStuff(task, _status, createdDate, dataSource);
    }

    static TaskInfo<Task, TaskStatus> toTaskInfo(TaskStuff taskStuff)
    {
      return new TaskInfo<>(
        taskStuff.getTask().getId(),
        taskStuff.getCreatedDate(),
        taskStuff.getStatus(),
        taskStuff.getDataSource(),
        taskStuff.getTask()
      );
    }
  }
}
