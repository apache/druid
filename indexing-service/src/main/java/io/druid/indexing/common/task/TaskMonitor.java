/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.common.task;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.TaskStatusResponse;
import io.druid.indexer.TaskState;
import io.druid.indexer.TaskStatusPlus;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for submitting tasks, monitoring task statuses, resubmitting failed tasks, and returning the final task
 * status.
 */
public class TaskMonitor<T extends Task>
{
  private static final Logger log = new Logger(TaskMonitor.class);

  private final ScheduledExecutorService taskStatusChecker = Execs.scheduledSingleThreaded(("task-monitor-%d"));

  // taskId -> monitorEntry
  private final ConcurrentMap<String, MonitorEntry> runningTasks = new ConcurrentHashMap<>();

  // overlord client
  private final AtomicInteger numRunningTasks = new AtomicInteger();

  private final IndexingServiceClient indexingServiceClient;
  private final int maxRetry;

  private volatile boolean running = false;

  TaskMonitor(IndexingServiceClient indexingServiceClient, int maxRetry)
  {
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
    this.maxRetry = maxRetry;
  }

  public void start(long taskStatusCheckingPeriod)
  {
    running = true;
    log.info("Starting taskMonitor");
    // NOTE: This polling can be improved to event-driven processing by registering TaskRunnerListener to TaskRunner.
    // That listener should be able to send the events reported to TaskRunner to this TaskMonitor.
    taskStatusChecker.scheduleAtFixedRate(
        () -> {
          try {
            final Iterator<Entry<String, MonitorEntry>> iterator = runningTasks.entrySet().iterator();
            while (iterator.hasNext()) {
              final Entry<String, MonitorEntry> entry = iterator.next();
              final String taskId = entry.getKey();
              final MonitorEntry monitorEntry = entry.getValue();
              final TaskStatusResponse taskStatusResponse = indexingServiceClient.getTaskStatus(taskId);
              if (taskStatusResponse != null) {
                final TaskStatusPlus taskStatus = taskStatusResponse.getStatus();
                switch (taskStatus.getState()) {
                  case SUCCESS:
                    numRunningTasks.decrementAndGet();
                    iterator.remove();

                    monitorEntry.setLastStatus(taskStatus);
                    break;
                  case FAILED:
                    numRunningTasks.decrementAndGet();
                    iterator.remove();

                    log.warn("task[%s] failed!", taskId);
                    if (monitorEntry.numTries() < maxRetry) {
                      log.info(
                          "We still have chnaces[%d/%d] to complete.",
                          monitorEntry.numTries(),
                          maxRetry
                      );
                      retry(monitorEntry, taskStatus);
                    } else {
                      log.error(
                          "spec[%s] failed after [%d] tries",
                          monitorEntry.spec.getId(),
                          monitorEntry.numTries()
                      );
                      monitorEntry.setLastStatus(taskStatus);
                    }
                    break;
                  case RUNNING:
                    // do nothing
                    break;
                  default:
                    throw new ISE("Unknown taskStatus[%s] for task[%s[", taskStatus.getState(), taskId);
                }
              }
            }
          }
          catch (Throwable t) {
            log.error(t, "Error while monitoring");
          }
        },
        taskStatusCheckingPeriod,
        taskStatusCheckingPeriod,
        TimeUnit.MILLISECONDS
    );
  }

  public void stop()
  {
    running = false;
    taskStatusChecker.shutdownNow();
    log.info("Stopped taskMonitor");
  }

  public ListenableFuture<SubTaskCompleteEvent<T>> submit(SubTaskSpec<T> spec)
  {
    if (!running) {
      return Futures.immediateFailedFuture(new ISE("TaskMonitore is not running"));
    }
    final T task = spec.newSubTask(0);
    log.info("Submitting a new task[%s]", task.getId());
    indexingServiceClient.runTask(task);
    numRunningTasks.incrementAndGet();

    final SettableFuture<SubTaskCompleteEvent<T>> taskFuture = SettableFuture.create();
    runningTasks.put(task.getId(), new MonitorEntry(spec, task, taskFuture));

    return taskFuture;
  }

  private void retry(MonitorEntry monitorEntry, TaskStatusPlus lastFailedTaskStatus)
  {
    if (running) {
      final SubTaskSpec<T> spec = monitorEntry.spec;
      final T task = spec.newSubTask(monitorEntry.taskHistory.size() + 1);
      log.info("Submitting a new task[%s] for retrying spec[%s]", task.getId(), spec.getId());
      indexingServiceClient.runTask(task);
      numRunningTasks.incrementAndGet();

      runningTasks.put(
          task.getId(),
          monitorEntry.withNewRunningTask(task, lastFailedTaskStatus)
      );
    }
  }

  public void killAll()
  {
    runningTasks.keySet().forEach(taskId -> {
      log.info("Request to kill subtask[%s]", taskId);
      indexingServiceClient.killTask(taskId);
    });
    runningTasks.clear();
  }

  public int getNumRunningTasks()
  {
    return numRunningTasks.intValue();
  }

  private class MonitorEntry
  {
    private final SubTaskSpec<T> spec;
    private final T runningTask;
    private final List<TaskStatusPlus> taskHistory;
    private final SettableFuture<SubTaskCompleteEvent<T>> completeEventFuture;

    MonitorEntry(
        SubTaskSpec<T> spec,
        T runningTask,
        SettableFuture<SubTaskCompleteEvent<T>> completeEventFuture
    )
    {
      this(spec, runningTask, new ArrayList<>(), completeEventFuture);
    }

    private MonitorEntry(
        SubTaskSpec<T> spec,
        T runningTask,
        List<TaskStatusPlus> taskHistory,
        SettableFuture<SubTaskCompleteEvent<T>> completeEventFuture
    )
    {
      this.spec = spec;
      this.runningTask = runningTask;
      this.taskHistory = taskHistory;
      this.completeEventFuture = completeEventFuture;
    }

    MonitorEntry withNewRunningTask(T newTask, TaskStatusPlus statusOfLastTask)
    {
      taskHistory.add(statusOfLastTask);
      return new MonitorEntry(
          spec,
          newTask,
          taskHistory,
          completeEventFuture
      );
    }

    int numTries()
    {
      return taskHistory.size() + 1; // count runningTask. this is valid only until setLastStatus() is called
    }

    void setLastStatus(TaskStatusPlus lastStatus)
    {
      if (!runningTask.getId().equals(lastStatus.getId())) {
        throw new ISE(
            "Task id[%s] of lastStatus is different from the running task[%s]",
            lastStatus.getId(),
            runningTask.getId()
        );
      }

      taskHistory.add(lastStatus);
      completeEventFuture.set(new SubTaskCompleteEvent<>(spec, lastStatus.getState(), taskHistory));
    }
  }

  static class SubTaskCompleteEvent<T extends Task>
  {
    private final SubTaskSpec<T> spec;
    private final TaskState lastState;
    @Nullable
    private final List<TaskStatusPlus> attemptHistory;

    SubTaskCompleteEvent(
        SubTaskSpec<T> spec,
        TaskState lastState,
        @Nullable List<TaskStatusPlus> attemptHistory
    )
    {
      this.spec = Preconditions.checkNotNull(spec, "spec");
      this.lastState = Preconditions.checkNotNull(lastState, "lastState");
      this.attemptHistory = attemptHistory;
    }

    SubTaskSpec<T> getSpec()
    {
      return spec;
    }

    TaskState getLastState()
    {
      return lastState;
    }

    @Nullable
    List<TaskStatusPlus> getAttemptHistory()
    {
      return attemptHistory;
    }

    @Nullable
    TaskStatusPlus getLastStatus()
    {
      return attemptHistory == null ? null : attemptHistory.get(attemptHistory.size() - 1);
    }
  }
}
