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
import org.apache.druid.indexing.common.TaskStorageDirTracker;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  protected final TaskStorageDirTracker dirTracker;

  public BaseRestorableTaskRunner(
      ObjectMapper jsonMapper,
      TaskConfig taskConfig,
      TaskStorageDirTracker dirTracker
  )
  {
    this.jsonMapper = jsonMapper;
    this.taskConfig = taskConfig;
    this.dirTracker = dirTracker;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    final Map<File, TaskRestoreInfo> taskRestoreInfos = new HashMap<>();
    for (File baseDir : dirTracker.getBaseTaskDirs()) {
      File restoreFile = new File(baseDir, TASK_RESTORE_FILENAME);
      if (restoreFile.exists()) {
        try {
          taskRestoreInfos.put(baseDir, jsonMapper.readValue(restoreFile, TaskRestoreInfo.class));
        }
        catch (Exception e) {
          LOG.error(e, "Failed to read restorable tasks from file[%s]. Skipping restore.", restoreFile);
        }
      }
    }

    final List<Pair<Task, ListenableFuture<TaskStatus>>> retVal = new ArrayList<>();
    for (Map.Entry<File, TaskRestoreInfo> entry : taskRestoreInfos.entrySet()) {
      final File baseDir = entry.getKey();
      final TaskRestoreInfo taskRestoreInfo = entry.getValue();
      for (final String taskId : taskRestoreInfo.getRunningTasks()) {
        try {
          dirTracker.addTask(taskId, baseDir);
          final File taskFile = new File(dirTracker.getTaskDir(taskId), "task.json");
          final Task task = jsonMapper.readValue(taskFile, Task.class);

          if (!task.getId().equals(taskId)) {
            throw new ISE("Task[%s] restore file had wrong id[%s]", taskId, task.getId());
          }

          if (taskConfig.isRestoreTasksOnRestart() && task.canRestore()) {
            LOG.info("Restoring task[%s] from location[%s].", task.getId(), baseDir);
            retVal.add(Pair.of(task, run(task)));
          } else {
            dirTracker.removeTask(taskId);
          }
        }
        catch (Exception e) {
          LOG.warn(e, "Failed to restore task[%s] from path[%s]. Trying to restore other tasks.",
                   taskId, baseDir);
          dirTracker.removeTask(taskId);
        }
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
    final Map<File, List<String>> theTasks = new HashMap<>();
    for (TaskRunnerWorkItem forkingTaskRunnerWorkItem : tasks.values()) {
      final String taskId = forkingTaskRunnerWorkItem.getTaskId();
      final File restoreFile = getRestoreFile(taskId);
      theTasks.computeIfAbsent(restoreFile, k -> new ArrayList<>())
              .add(taskId);
    }

    for (Map.Entry<File, List<String>> entry : theTasks.entrySet()) {
      final File restoreFile = entry.getKey();
      final TaskRestoreInfo taskRestoreInfo = new TaskRestoreInfo(entry.getValue());
      try {
        Files.createParentDirs(restoreFile);
        jsonMapper.writeValue(restoreFile, taskRestoreInfo);
        LOG.debug("Save restore file at [%s] for tasks [%s]",
                 restoreFile.getAbsoluteFile(), Arrays.toString(theTasks.get(restoreFile).toArray()));
      }
      catch (Exception e) {
        LOG.warn(e, "Failed to save tasks to restore file[%s]. Skipping this save.", restoreFile);
      }
    }
  }

  @Override
  public void stop()
  {
    if (!taskConfig.isRestoreTasksOnRestart()) {
      return;
    }

    for (File baseDir : dirTracker.getBaseTaskDirs()) {
      File restoreFile = new File(baseDir, TASK_RESTORE_FILENAME);
      if (restoreFile.exists()) {
        try {
          TaskRestoreInfo taskRestoreInfo = jsonMapper.readValue(restoreFile, TaskRestoreInfo.class);
          LOG.info("Path[%s] contains restore data for tasks[%s] on restart",
                   baseDir, taskRestoreInfo.getRunningTasks());
        }
        catch (Exception e) {
          LOG.error(e, "Failed to read task restore info from file[%s].", restoreFile);
        }
      }
    }
  }

  private File getRestoreFile(String taskId)
  {
    return new File(dirTracker.getBaseTaskDir(taskId), TASK_RESTORE_FILENAME);
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
