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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

/**
 * Base class for {@link ForkingTaskRunner} and {@link ThreadingTaskRunner} which support task restoration.
 */
public abstract class BaseRestorableTaskRunner<WorkItemType extends TaskRunnerWorkItem> implements TaskRunner
{
  protected static final EmittingLogger LOG = new EmittingLogger(BaseRestorableTaskRunner.class);
  protected static final String TASK_RESTORE_FILENAME = "restore.json";

  protected final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  /**
   * Writes must be synchronized. This is only a ConcurrentMap so "informational" reads can occur without waiting.
   */
  protected final ConcurrentHashMap<String, WorkItemType> tasks = new ConcurrentHashMap<>();
  protected final ObjectMapper jsonMapper;
  protected final TaskConfig taskConfig;

  public BaseRestorableTaskRunner(
      ObjectMapper jsonMapper,
      TaskConfig taskConfig
  )
  {
    this.jsonMapper = jsonMapper;
    this.taskConfig = taskConfig;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    final File restoreFile = getRestoreFile();
    final TaskRestoreInfo taskRestoreInfo;
    if (restoreFile.exists()) {
      try {
        taskRestoreInfo = jsonMapper.readValue(restoreFile, TaskRestoreInfo.class);
      }
      catch (Exception e) {
        LOG.error(e, "Failed to read restorable tasks from file[%s]. Skipping restore.", restoreFile);
        return ImmutableList.of();
      }
    } else {
      return ImmutableList.of();
    }

    final List<Pair<Task, ListenableFuture<TaskStatus>>> retVal = new ArrayList<>();
    for (final String taskId : taskRestoreInfo.getRunningTasks()) {
      try {
        final File taskFile = new File(taskConfig.getTaskDir(taskId), "task.json");
        final Task task = jsonMapper.readValue(taskFile, Task.class);

        if (!task.getId().equals(taskId)) {
          throw new ISE("WTF?! Task[%s] restore file had wrong id[%s].", taskId, task.getId());
        }

        if (taskConfig.isRestoreTasksOnRestart() && task.canRestore()) {
          LOG.info("Restoring task[%s].", task.getId());
          retVal.add(Pair.of(task, run(task)));
        }
      }
      catch (Exception e) {
        LOG.warn(e, "Failed to restore task[%s]. Trying to restore other tasks.", taskId);
      }
    }

    if (!retVal.isEmpty()) {
      LOG.info("Restored %,d tasks: %s", retVal.size(), Joiner.on(", ").join(retVal));
    }

    return retVal;
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listener.getListenerId())) {
        throw new ISE("Listener [%s] already registered", listener.getListenerId());
      }
    }

    final Pair<TaskRunnerListener, Executor> listenerPair = Pair.of(listener, executor);

    synchronized (tasks) {
      for (TaskRunnerWorkItem item : tasks.values()) {
        TaskRunnerUtils.notifyLocationChanged(ImmutableList.of(listenerPair), item.getTaskId(), item.getLocation());
      }

      listeners.add(listenerPair);
      LOG.debug("Registered listener [%s]", listener.getListenerId());
    }
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listenerId)) {
        listeners.remove(pair);
        LOG.debug("Unregistered listener [%s]", listenerId);
        return;
      }
    }
  }

  @Override
  public abstract Collection<TaskRunnerWorkItem> getRunningTasks();

  @Override
  public abstract Collection<TaskRunnerWorkItem> getPendingTasks();

  @Nullable
  @Override
  public abstract RunnerTaskState getRunnerTaskState(String taskId);

  @Override
  public Collection<TaskRunnerWorkItem> getKnownTasks()
  {
    synchronized (tasks) {
      return Lists.newArrayList(tasks.values());
    }
  }

  /**
   * Save running tasks to a file, so they can potentially be restored on next startup. Suppresses exceptions that
   * occur while saving.
   */
  @GuardedBy("tasks")
  protected void saveRunningTasks()
  {
    final File restoreFile = getRestoreFile();
    final List<String> theTasks = new ArrayList<>();
    for (TaskRunnerWorkItem forkingTaskRunnerWorkItem : tasks.values()) {
      theTasks.add(forkingTaskRunnerWorkItem.getTaskId());
    }

    try {
      Files.createParentDirs(restoreFile);
      jsonMapper.writeValue(restoreFile, new TaskRestoreInfo(theTasks));
    }
    catch (Exception e) {
      LOG.warn(e, "Failed to save tasks to restore file[%s]. Skipping this save.", restoreFile);
    }
  }

  protected File getRestoreFile()
  {
    return new File(taskConfig.getBaseTaskDir(), TASK_RESTORE_FILENAME);
  }

  protected static class TaskRestoreInfo
  {
    @JsonProperty
    private final List<String> runningTasks;

    @JsonCreator
    public TaskRestoreInfo(
        @JsonProperty("runningTasks") List<String> runningTasks
    )
    {
      this.runningTasks = runningTasks;
    }

    public List<String> getRunningTasks()
    {
      return runningTasks;
    }
  }
}
