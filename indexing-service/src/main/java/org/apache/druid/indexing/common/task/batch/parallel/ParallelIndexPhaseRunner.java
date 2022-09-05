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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.MonitorEntry;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.SubTaskCompleteEvent;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Base class for different implementations of {@link ParallelIndexTaskRunner}.
 * It creates sub tasks, schedule them, and monitor their status.
 */
public abstract class ParallelIndexPhaseRunner<SubTaskType extends Task, SubTaskReportType extends SubTaskReport>
    implements ParallelIndexTaskRunner<SubTaskType, SubTaskReportType>
{
  private static final Logger LOG = new Logger(ParallelIndexPhaseRunner.class);

  private final TaskToolbox toolbox;
  private final String taskId;
  private final String groupId;
  private final String baseSubtaskSpecName;
  private final ParallelIndexTuningConfig tuningConfig;
  private final Map<String, Object> context;

  /**
   * Max number of sub tasks which can be executed concurrently at the same time.
   */
  private final int maxNumConcurrentSubTasks;

  private final BlockingQueue<SubTaskCompleteEvent<SubTaskType>> taskCompleteEvents = new LinkedBlockingDeque<>();

  private volatile boolean subTaskScheduleAndMonitorStopped;
  private volatile TaskMonitor<SubTaskType, SubTaskReportType> taskMonitor;
  private volatile String stopReason;

  private int nextSpecId = 0;

  ParallelIndexPhaseRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      String baseSubtaskSpecName,
      ParallelIndexTuningConfig tuningConfig,
      Map<String, Object> context
  )
  {
    this.toolbox = toolbox;
    this.taskId = taskId;
    this.groupId = groupId;
    this.baseSubtaskSpecName = baseSubtaskSpecName;
    this.tuningConfig = tuningConfig;
    this.context = context;
    this.maxNumConcurrentSubTasks = tuningConfig.getMaxNumConcurrentSubTasks();
  }

  /**
   * Returns an iterator for {@link SubTaskSpec}s of this phase.
   */
  abstract Iterator<SubTaskSpec<SubTaskType>> subTaskSpecIterator() throws IOException;

  /**
   * Returns the total number of sub tasks required to execute this phase.
   */
  abstract int estimateTotalNumSubTasks() throws IOException;

  public Runnable getSubtaskCompletionCallback(SubTaskCompleteEvent<?> event)
  {
    return () -> {};
  }

  @Override
  public TaskState run() throws Exception
  {
    final CountingSubTaskSpecIterator subTaskSpecIterator = new CountingSubTaskSpecIterator(subTaskSpecIterator());
    if (!subTaskSpecIterator.hasNext()) {
      LOG.warn("There's no input split to process");
      return TaskState.SUCCESS;
    }

    final long taskStatusCheckingPeriod = tuningConfig.getTaskStatusCheckPeriodMs();

    taskMonitor = new TaskMonitor<>(
        toolbox.getOverlordClient(),
        tuningConfig.getMaxRetry(),
        estimateTotalNumSubTasks()
    );
    TaskState state = TaskState.RUNNING;

    taskMonitor.start(taskStatusCheckingPeriod);

    try {
      LOG.info("Submitting initial tasks");
      // Submit initial tasks
      while (isRunning() && subTaskSpecIterator.hasNext() && taskMonitor.getNumRunningTasks() < maxNumConcurrentSubTasks) {
        submitNewTask(taskMonitor, subTaskSpecIterator.next());
      }

      LOG.info("Waiting for subTasks to be completed");
      while (isRunning()) {
        final SubTaskCompleteEvent<SubTaskType> taskCompleteEvent = taskCompleteEvents.poll(
            taskStatusCheckingPeriod,
            TimeUnit.MILLISECONDS
        );

        if (taskCompleteEvent != null) {
          final TaskState completeState = taskCompleteEvent.getLastState();
          getSubtaskCompletionCallback(taskCompleteEvent).run();
          switch (completeState) {
            case SUCCESS:
              final TaskStatusPlus completeStatus = taskCompleteEvent.getLastStatus();
              if (completeStatus == null) {
                throw new ISE("Last status of complete task is missing!");
              }
              if (!subTaskSpecIterator.hasNext()) {
                // We have no more subTasks to run
                if (taskMonitor.getNumRunningTasks() == 0 && taskCompleteEvents.isEmpty()) {
                  subTaskScheduleAndMonitorStopped = true;
                  if (subTaskSpecIterator.count == taskMonitor.getNumSucceededTasks()) {
                    // Succeeded
                    state = TaskState.SUCCESS;
                  } else {
                    // Failed
                    final ParallelIndexingPhaseProgress monitorStatus = taskMonitor.getProgress();
                    throw new ISE(
                        "Expected [%d] tasks to succeed, but we got [%d] succeeded tasks and [%d] failed tasks",
                        subTaskSpecIterator.count,
                        monitorStatus.getSucceeded(),
                        monitorStatus.getFailed()
                    );
                  }
                }
              } else if (taskMonitor.getNumRunningTasks() < maxNumConcurrentSubTasks) {
                // We have more subTasks to run
                submitNewTask(taskMonitor, subTaskSpecIterator.next());
              } else {
                // We have more subTasks to run, but don't have enough available task slots
                // do nothing
              }
              break;
            case FAILED:
              // TaskMonitor already tried everything it can do for failed tasks. We failed.
              state = TaskState.FAILED;
              subTaskScheduleAndMonitorStopped = true;
              final TaskStatusPlus lastStatus = taskCompleteEvent.getLastStatus();
              if (lastStatus != null) {
                LOG.error("Failed because of the failed sub task[%s]", lastStatus.getId());
              } else {
                final SubTaskSpec<?> spec = taskCompleteEvent.getSpec();
                LOG.error("Failed to process spec[%s] with an unknown last status", spec.getId());
              }
              break;
            default:
              throw new ISE("spec[%s] is in an invalid state[%s]", taskCompleteEvent.getSpec().getId(), completeState);
          }
        }
      }
    }
    finally {
      stopInternal();
      if (!state.isComplete()) {
        state = TaskState.FAILED;
      }
    }

    return state;
  }

  private class CountingSubTaskSpecIterator implements Iterator<SubTaskSpec<SubTaskType>>
  {
    private final Iterator<SubTaskSpec<SubTaskType>> delegate;
    private int count;

    private CountingSubTaskSpecIterator(Iterator<SubTaskSpec<SubTaskType>> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext()
    {
      return delegate.hasNext();
    }

    @Override
    public SubTaskSpec<SubTaskType> next()
    {
      if (!delegate.hasNext()) {
        throw new NoSuchElementException();
      }
      count++;
      return delegate.next();
    }
  }

  private boolean isRunning()
  {
    return !subTaskScheduleAndMonitorStopped && !Thread.currentThread().isInterrupted();
  }

  private void submitNewTask(
      TaskMonitor<SubTaskType, SubTaskReportType> taskMonitor,
      SubTaskSpec<SubTaskType> spec
  )
  {
    LOG.info("Submit a new task for spec[%s]", spec.getId());
    final ListenableFuture<SubTaskCompleteEvent<SubTaskType>> future = taskMonitor.submit(spec);
    Futures.addCallback(
        future,
        new FutureCallback<SubTaskCompleteEvent<SubTaskType>>()
        {
          @Override
          public void onSuccess(SubTaskCompleteEvent<SubTaskType> completeEvent)
          {
            // this callback is called if a task completed whether it succeeded or not.
            taskCompleteEvents.offer(completeEvent);
          }

          @Override
          public void onFailure(Throwable t)
          {
            // this callback is called only when there were some problems in TaskMonitor.
            LOG.error(t, "Error while running a task for spec[%s]", spec.getId());
            taskCompleteEvents.offer(SubTaskCompleteEvent.fail(spec, t));
          }
        }
    );
  }

  @Override
  public void stopGracefully(String stopReason)
  {
    subTaskScheduleAndMonitorStopped = true;
    this.stopReason = stopReason;
    stopInternal();
  }

  /**
   * Stop task scheduling and monitoring, and kill all running tasks.
   * This method is thread-safe.
   */
  private void stopInternal()
  {
    LOG.info("Cleaning up resources");

    taskCompleteEvents.clear();
    if (taskMonitor != null) {
      taskMonitor.stop();
    }
  }

  @Override
  public void collectReport(SubTaskReportType report)
  {
    // This method is only called when there is a subtask sending its report.
    // Since TaskMonitor is responsible for spawning subtasks, the taskMonitor cannot be null if we have subtask sending report
    // This null check is to ensure that the contract mentioned above is not broken
    assert taskMonitor != null;
    taskMonitor.collectReport(report);
  }

  @Override
  public Map<String, SubTaskReportType> getReports()
  {
    return taskMonitor == null ? Collections.emptyMap() : taskMonitor.getReports();
  }

  @Override
  public ParallelIndexingPhaseProgress getProgress()
  {
    return taskMonitor == null ? ParallelIndexingPhaseProgress.notRunning() : taskMonitor.getProgress();
  }

  @Override
  public Set<String> getRunningTaskIds()
  {
    return taskMonitor == null ? Collections.emptySet() : taskMonitor.getRunningTaskIds();
  }

  @Override
  public List<SubTaskSpec<SubTaskType>> getSubTaskSpecs()
  {
    if (taskMonitor != null) {
      final List<SubTaskSpec<SubTaskType>> runningSubTaskSpecs = taskMonitor.getRunningSubTaskSpecs();
      final List<SubTaskSpec<SubTaskType>> completeSubTaskSpecs = taskMonitor
          .getCompleteSubTaskSpecs();
      // Deduplicate subTaskSpecs because some subTaskSpec might exist both in runningSubTaskSpecs and
      // completeSubTaskSpecs.
      final Map<String, SubTaskSpec<SubTaskType>> subTaskSpecMap = Maps.newHashMapWithExpectedSize(
          runningSubTaskSpecs.size() + completeSubTaskSpecs.size()
      );
      runningSubTaskSpecs.forEach(spec -> subTaskSpecMap.put(spec.getId(), spec));
      completeSubTaskSpecs.forEach(spec -> subTaskSpecMap.put(spec.getId(), spec));
      return new ArrayList<>(subTaskSpecMap.values());
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public List<SubTaskSpec<SubTaskType>> getRunningSubTaskSpecs()
  {
    return taskMonitor == null ? Collections.emptyList() : taskMonitor.getRunningSubTaskSpecs();
  }

  @Override
  public List<SubTaskSpec<SubTaskType>> getCompleteSubTaskSpecs()
  {
    return taskMonitor == null ? Collections.emptyList() : taskMonitor.getCompleteSubTaskSpecs();
  }

  @Nullable
  @Override
  public SubTaskSpec<SubTaskType> getSubTaskSpec(String subTaskSpecId)
  {
    if (taskMonitor != null) {
      // Running tasks should be checked first because, in taskMonitor, subTaskSpecs are removed from runningTasks after
      // adding them to taskHistory.
      final MonitorEntry monitorEntry = taskMonitor.getRunningTaskMonitorEntry(subTaskSpecId);
      final TaskHistory<SubTaskType> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(subTaskSpecId);
      final SubTaskSpec<SubTaskType> subTaskSpec;

      if (monitorEntry != null) {
        subTaskSpec = monitorEntry.getSpec();
      } else {
        if (taskHistory != null) {
          subTaskSpec = taskHistory.getSpec();
        } else {
          subTaskSpec = null;
        }
      }

      return subTaskSpec;
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public SubTaskSpecStatus getSubTaskState(String subTaskSpecId)
  {
    if (taskMonitor == null) {
      return null;
    } else {
      // Running tasks should be checked first because, in taskMonitor, subTaskSpecs are removed from runningTasks after
      // adding them to taskHistory.
      final MonitorEntry monitorEntry = taskMonitor.getRunningTaskMonitorEntry(subTaskSpecId);
      final TaskHistory<SubTaskType> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(subTaskSpecId);

      final SubTaskSpecStatus subTaskSpecStatus;

      if (monitorEntry != null) {
        subTaskSpecStatus = new SubTaskSpecStatus(
            (SinglePhaseSubTaskSpec) monitorEntry.getSpec(),
            monitorEntry.getRunningStatus(),
            monitorEntry.getTaskHistory()
        );
      } else {
        if (taskHistory != null && !taskHistory.isEmpty()) {
          subTaskSpecStatus = new SubTaskSpecStatus(
              (SinglePhaseSubTaskSpec) taskHistory.getSpec(),
              null,
              taskHistory.getAttemptHistory()
          );
        } else {
          subTaskSpecStatus = null;
        }
      }

      return subTaskSpecStatus;
    }
  }

  @Nullable
  @Override
  public TaskHistory<SubTaskType> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId)
  {
    if (taskMonitor == null) {
      return null;
    } else {
      return taskMonitor.getCompleteSubTaskSpecHistory(subTaskSpecId);
    }
  }

  @Override
  public String getStopReason()
  {
    return stopReason;
  }

  String getTaskId()
  {
    return taskId;
  }

  String getGroupId()
  {
    return groupId;
  }

  String getBaseSubtaskSpecName()
  {
    return baseSubtaskSpecName;
  }

  Map<String, Object> getContext()
  {
    return context;
  }

  ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  TaskToolbox getToolbox()
  {
    return toolbox;
  }

  @VisibleForTesting
  @Nullable
  TaskMonitor<SubTaskType, SubTaskReportType> getTaskMonitor()
  {
    return taskMonitor;
  }

  @VisibleForTesting
  int getAndIncrementNextSpecId()
  {
    return nextSpecId++;
  }
}
