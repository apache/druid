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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Responsible for submitting tasks, monitoring task statuses, resubmitting failed tasks, and returning the final task
 * status.
 */
public class TaskMonitor<T extends Task>
{
  private static final Logger log = new Logger(TaskMonitor.class);

  private final ScheduledExecutorService taskStatusChecker = Execs.scheduledSingleThreaded(("task-monitor-%d"));

  /**
   * A map of subtaskId to {@link MonitorEntry}. This map stores the state of running {@link SubTaskSpec}s. This is
   * read in {@link java.util.concurrent.Callable} executed by {@link #taskStatusChecker} and updated in {@link #submit}
   * and {@link #retry}. This can also be read by calling {@link #getRunningTaskMonitorEntry},
   * {@link #getRunningTaskIds}, and {@link #getRunningSubTaskSpecs}.
   */
  private final ConcurrentMap<String, MonitorEntry> runningTasks = new ConcurrentHashMap<>();

  /**
   * A map of subTaskSpecId to {@link TaskHistory}. This map stores the history of complete {@link SubTaskSpec}s
   * whether their final state is succeeded or failed. This is updated in {@link MonitorEntry#setLastStatus} which is
   * called by the {@link java.util.concurrent.Callable} executed by {@link #taskStatusChecker} and can be
   * read by outside of this class.
   */
  private final ConcurrentMap<String, TaskHistory<T>> taskHistories = new ConcurrentHashMap<>();

  // lock for updating numRunningTasks, numSucceededTasks, and numFailedTasks
  private final Object taskCountLock = new Object();

  // lock for updating running state
  private final Object startStopLock = new Object();

  // overlord client
  private final IndexingServiceClient indexingServiceClient;
  private final int maxSubtaskRetries;
  private final int estimatedNumSucceededTasks;

  @GuardedBy("taskCountLock")
  private int numRunningTasks;
  @GuardedBy("taskCountLock")
  private int numSucceededTasks;
  @GuardedBy("taskCountLock")
  private int numFailedTasks;
  /**
   * This metric is used only for unit tests because the current task status system doesn't track the canceled task
   * status. Currently, this metric only represents the number of canceled tasks by {@link ParallelIndexTaskRunner}.
   * See {@link #stop()}, {@link ParallelIndexPhaseRunner#run()}, and
   * {@link ParallelIndexPhaseRunner#stopGracefully()}.
   */
  private int numCanceledTasks;

  @GuardedBy("startStopLock")
  private boolean running = false;

  TaskMonitor(IndexingServiceClient indexingServiceClient, int maxSubtaskRetries, int estimatedNumSucceededTasks)
  {
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
    this.maxSubtaskRetries = maxSubtaskRetries;
    this.estimatedNumSucceededTasks = estimatedNumSucceededTasks;

    log.info("TaskMonitor is initialized with estimatedNumSucceededTasks[%d]", estimatedNumSucceededTasks);
  }

  public void start(long taskStatusCheckPeriodMs, long liveReportTimeoutMs)
  {
    final long liveReportTimeoutNs = TimeUnit.MILLISECONDS.toNanos(liveReportTimeoutMs);
    synchronized (startStopLock) {
      running = true;
      log.info("Starting taskMonitor");
      // In Parallel task, subtasks periodically report their states with metrics. However, this could not be
      // enough for monitoring subtask status because the report can be missing or even wrong for various reasons
      // in distributed systems. TaskMonitor always checks the final status of subtask with the Overlord where
      // is the source of truth for task statuses.
      taskStatusChecker.scheduleAtFixedRate(
          () -> {
            try {
              final Iterator<Entry<String, MonitorEntry>> iterator = runningTasks.entrySet().iterator();
              while (iterator.hasNext()) {
                final Entry<String, MonitorEntry> entry = iterator.next();
                final String taskId = entry.getKey();
                final MonitorEntry monitorEntry = entry.getValue();

                // We here measure the current time for individual subtask because it could take long time to talk to
                // the Overlord.
                if (monitorEntry.needStatusCheck(System.nanoTime(), liveReportTimeoutNs)) {
                  final TaskStatusResponse taskStatusResponse = indexingServiceClient.getTaskStatus(taskId);
                  final TaskStatusPlus taskStatus = taskStatusResponse.getStatus();
                  if (taskStatus != null) {
                    switch (Preconditions.checkNotNull(taskStatus.getStatusCode(), "taskState")) {
                      case SUCCESS:
                        incrementNumSucceededTasks();

                        // Remove the current entry after updating taskHistories to make sure that task history
                        // exists either runningTasks or taskHistories.
                        monitorEntry.setLastStatus(taskStatus);
                        iterator.remove();
                        break;
                      case FAILED:
                        incrementNumFailedTasks();

                        log.warn("task[%s] failed!", taskId);
                        if (monitorEntry.numTries() < maxSubtaskRetries) {
                          log.info(
                              "We still have more chances[%d/%d] to process the spec[%s].",
                              monitorEntry.numTries(),
                              maxSubtaskRetries,
                              monitorEntry.spec.getId()
                          );
                          retry(monitorEntry, taskStatus);
                        } else {
                          log.error(
                              "spec[%s] failed after [%d] tries",
                              monitorEntry.spec.getId(),
                              monitorEntry.numTries()
                          );
                          // Remove the current entry after updating taskHistories to make sure that task history
                          // exists either runningTasks or taskHistories.
                          monitorEntry.setLastStatus(taskStatus);
                          iterator.remove();
                        }
                        break;
                      case RUNNING:
                        monitorEntry.updateRunningStatus(taskStatus);
                        break;
                      default:
                        throw new ISE("Unknown taskStatus[%s] for task[%s[", taskStatus.getStatusCode(), taskId);
                    }
                  }
                } else {
                  // If we recently received a report from a subtask, it would be likely still running. We don't
                  // have to be aggresive for checking its status.
                  log.info("Skipping status check for subtask[%s] because it recently sent a report", taskId);
                }
              }
            }
            catch (Throwable t) {
              // Note that we only log the message here so that task monitoring continues to happen or else
              // the task which created this monitor will keep on waiting endlessly assuming monitored tasks
              // are still running.
              log.error(t, "Error while monitoring");
            }
          },
          taskStatusCheckPeriodMs,
          taskStatusCheckPeriodMs,
          TimeUnit.MILLISECONDS
      );
    }
  }

  /**
   * Stop task monitoring and cancel all running tasks.
   */
  public void stop()
  {
    synchronized (startStopLock) {
      if (running) {
        running = false;
        taskStatusChecker.shutdownNow();

        synchronized (taskCountLock) {
          if (numRunningTasks > 0) {
            final Iterator<MonitorEntry> iterator = runningTasks.values().iterator();
            while (iterator.hasNext()) {
              final MonitorEntry entry = iterator.next();
              iterator.remove();
              final String taskId = entry.runningTask.getId();
              log.info("Request to cancel subtask[%s]", taskId);
              indexingServiceClient.cancelTask(taskId);
              numRunningTasks--;
              numCanceledTasks++;
            }

            if (numRunningTasks > 0) {
              log.warn(
                  "Inconsistent state: numRunningTasks[%d] is still not zero after trying to cancel all running tasks.",
                  numRunningTasks
              );
            }
          }
        }

        log.info("Stopped taskMonitor");
      }
    }
  }

  /**
   * This method is called when a subtask reports its state. When TaskMonitor receives a {@link TaskState#RUNNING}
   * report from a subtask, it will pause the status check for the subtask for a while but assume that the subtask
   * is still running. When the reported state is {@link TaskState#SUCCESS} or {@link TaskState#FAILED}, TaskMonitor
   * will double-check the last status of the task with Overlord.
   */
  public void statusReport(String subtaskId, TaskState state)
  {
    final MonitorEntry monitorEntry = runningTasks.get(subtaskId);
    if (monitorEntry == null) {
      // This shouldn't usually happen, but even if it happens, we can simply ignore the reports from unknown subtasks.
      log.warn("Got a status report[%s] from an unknown subtask[%s]", state, subtaskId);
    } else {
      monitorEntry.updateStatusCheckState(System.nanoTime(), state.isComplete());
    }
  }

  /**
   * Submits a {@link SubTaskSpec} to process to this TaskMonitor. TaskMonitor can issue one or more tasks to process
   * the given spec. The returned future is done when
   * 1) a sub task successfully processed the given spec or
   * 2) the last sub task for the spec failed after all retries were exhausted.
   */
  public ListenableFuture<SubTaskCompleteEvent<T>> submit(SubTaskSpec<T> spec)
  {
    synchronized (startStopLock) {
      if (!running) {
        return Futures.immediateFailedFuture(new ISE("TaskMonitor is not running"));
      }
      final T task = submitTask(spec, 0);
      log.info("Submitted a new task[%s] for spec[%s]", task.getId(), spec.getId());
      incrementNumRunningTasks();

      final SettableFuture<SubTaskCompleteEvent<T>> taskFuture = SettableFuture.create();
      runningTasks.put(task.getId(), new MonitorEntry(spec, task, taskFuture));

      return taskFuture;
    }
  }

  /**
   * Submit a retry task for a failed spec. This method should be called inside of the
   * {@link java.util.concurrent.Callable} executed by {@link #taskStatusChecker}.
   */
  private void retry(MonitorEntry monitorEntry, TaskStatusPlus lastFailedTaskStatus)
  {
    synchronized (startStopLock) {
      if (running) {
        final SubTaskSpec<T> spec = monitorEntry.spec;
        final T task = submitTask(spec, monitorEntry.taskHistory.size() + 1);
        log.info("Submitted a new task[%s] for retrying spec[%s]", task.getId(), spec.getId());
        incrementNumRunningTasks();

        runningTasks.put(
            task.getId(),
            monitorEntry.withNewRunningTask(task, lastFailedTaskStatus)
        );
      }
    }
  }

  private T submitTask(SubTaskSpec<T> spec, int numAttempts)
  {
    T task = spec.newSubTask(numAttempts);
    try {
      indexingServiceClient.runTask(task.getId(), task);
    }
    catch (Exception e) {
      if (isUnknownTypeIdException(e)) {
        log.warn(e, "Got an unknown type id error. Retrying with a backward compatible type.");
        task = spec.newSubTaskWithBackwardCompatibleType(numAttempts);
        indexingServiceClient.runTask(task.getId(), task);
      } else {
        throw e;
      }
    }
    return task;
  }

  private boolean isUnknownTypeIdException(Throwable e)
  {
    if (e instanceof IllegalStateException) {
      if (e.getMessage() != null && e.getMessage().contains("Could not resolve type id")) {
        return true;
      }
    }
    if (e.getCause() != null) {
      return isUnknownTypeIdException(e.getCause());
    } else {
      return false;
    }
  }

  private void incrementNumRunningTasks()
  {
    synchronized (taskCountLock) {
      numRunningTasks++;
    }
  }

  private void incrementNumSucceededTasks()
  {
    synchronized (taskCountLock) {
      numRunningTasks--;
      numSucceededTasks++;
      log.info("[%d/%d] tasks succeeded", numSucceededTasks, estimatedNumSucceededTasks);
    }
  }

  private void incrementNumFailedTasks()
  {
    synchronized (taskCountLock) {
      numRunningTasks--;
      numFailedTasks++;
    }
  }

  int getNumSucceededTasks()
  {
    synchronized (taskCountLock) {
      return numSucceededTasks;
    }
  }

  int getNumRunningTasks()
  {
    synchronized (taskCountLock) {
      return numRunningTasks;
    }
  }

  @VisibleForTesting
  int getNumCanceledTasks()
  {
    return numCanceledTasks;
  }

  ParallelIndexingPhaseProgress getProgress()
  {
    synchronized (taskCountLock) {
      return new ParallelIndexingPhaseProgress(
          numRunningTasks,
          numSucceededTasks,
          numFailedTasks,
          numSucceededTasks + numFailedTasks,
          numRunningTasks + numSucceededTasks + numFailedTasks,
          estimatedNumSucceededTasks
      );
    }
  }

  Set<String> getRunningTaskIds()
  {
    return runningTasks.keySet();
  }

  List<SubTaskSpec<T>> getRunningSubTaskSpecs()
  {
    return runningTasks.values().stream().map(monitorEntry -> monitorEntry.spec).collect(Collectors.toList());
  }

  @Nullable
  MonitorEntry getRunningTaskMonitorEntry(String subTaskSpecId)
  {
    return runningTasks.values()
                       .stream()
                       .filter(monitorEntry -> monitorEntry.spec.getId().equals(subTaskSpecId))
                       .findFirst()
                       .orElse(null);
  }

  List<SubTaskSpec<T>> getCompleteSubTaskSpecs()
  {
    return taskHistories.values().stream().map(TaskHistory::getSpec).collect(Collectors.toList());
  }

  @Nullable
  TaskHistory<T> getCompleteSubTaskSpecHistory(String subTaskSpecId)
  {
    return taskHistories.get(subTaskSpecId);
  }

  class MonitorEntry
  {
    private static final long UNKNOWN_UPDATE_TIME = -1;

    private final SubTaskSpec<T> spec;
    private final T runningTask;
    // old tasks to recent tasks. running task is not included
    private final CopyOnWriteArrayList<TaskStatusPlus> taskHistory;
    private final SettableFuture<SubTaskCompleteEvent<T>> completeEventFuture;

    private final Object statusCheckLock = new Object();

    /**
     * Last time when {@link #runningTask} sent a report.
     */
    @GuardedBy("statusCheckLock")
    private long lastStatusUpdateTimeNs = UNKNOWN_UPDATE_TIME;
    /**
     * A flag indicating that the task status should be checked regardless of {@link #lastStatusUpdateTimeNs}. This is
     * usually set when {@link #runningTask} is finished.
     */
    @GuardedBy("statusCheckLock")
    private boolean needStatusCheck = false;

    private MonitorEntry(
        SubTaskSpec<T> spec,
        T runningTask,
        SettableFuture<SubTaskCompleteEvent<T>> completeEventFuture
    )
    {
      this(spec, runningTask, new CopyOnWriteArrayList<>(), completeEventFuture);
    }

    private MonitorEntry(
        SubTaskSpec<T> spec,
        T runningTask,
        CopyOnWriteArrayList<TaskStatusPlus> taskHistory,
        SettableFuture<SubTaskCompleteEvent<T>> completeEventFuture
    )
    {
      this.spec = spec;
      this.runningTask = runningTask;
      this.taskHistory = taskHistory;
      this.completeEventFuture = completeEventFuture;
    }

    private void updateStatusCheckState(long lastStatusUpdateTimeNs, boolean needStatusCheck)
    {
      synchronized (statusCheckLock) {
        this.lastStatusUpdateTimeNs = lastStatusUpdateTimeNs;
        this.needStatusCheck = needStatusCheck;
      }
    }

    boolean needStatusCheck(long currentTimeNs, long statusCheckTimeoutNs)
    {
      synchronized (statusCheckLock) {
        return needStatusCheck || currentTimeNs > lastStatusUpdateTimeNs + statusCheckTimeoutNs;
      }
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
      return taskHistory.size() + 1; // count runningTask as well
    }

    void updateRunningStatus(TaskStatusPlus statusPlus)
    {
      if (!runningTask.getId().equals(statusPlus.getId())) {
        throw new ISE(
            "Task id[%s] of lastStatus is different from the running task[%s]",
            statusPlus.getId(),
            runningTask.getId()
        );
      }
      updateStatusCheckState(System.nanoTime(), false);
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

      // We don't have to update the lastStatuUpdateTime since this MonitorEntry will be removed from runningTasks
      // after this method call.
      taskHistory.add(lastStatus);
      taskHistories.put(spec.getId(), new TaskHistory<>(spec, taskHistory));
      completeEventFuture.set(SubTaskCompleteEvent.success(spec, lastStatus));
    }

    SubTaskSpec<T> getSpec()
    {
      return spec;
    }

    T getRunningTask()
    {
      return runningTask;
    }

    List<TaskStatusPlus> getTaskHistory()
    {
      return taskHistory;
    }
  }

  static class SubTaskCompleteEvent<T extends Task>
  {
    private final SubTaskSpec<T> spec;
    @Nullable
    private final TaskStatusPlus lastStatus;
    @Nullable
    private final Throwable throwable;

    static <T extends Task> SubTaskCompleteEvent<T> success(SubTaskSpec<T> spec, TaskStatusPlus lastStatus)
    {
      return new SubTaskCompleteEvent<>(spec, Preconditions.checkNotNull(lastStatus, "lastStatus"), null);
    }

    static <T extends Task> SubTaskCompleteEvent<T> fail(SubTaskSpec<T> spec, Throwable t)
    {
      return new SubTaskCompleteEvent<>(spec, null, t);
    }

    private SubTaskCompleteEvent(
        SubTaskSpec<T> spec,
        @Nullable TaskStatusPlus lastStatus,
        @Nullable Throwable throwable
    )
    {
      this.spec = Preconditions.checkNotNull(spec, "spec");
      this.lastStatus = lastStatus;
      this.throwable = throwable;
    }

    SubTaskSpec<T> getSpec()
    {
      return spec;
    }

    TaskState getLastState()
    {
      return lastStatus == null ? TaskState.FAILED : lastStatus.getStatusCode();
    }

    @Nullable
    TaskStatusPlus getLastStatus()
    {
      return lastStatus;
    }

    @Nullable
    Throwable getThrowable()
    {
      return throwable;
    }
  }
}
