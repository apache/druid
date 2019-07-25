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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.MonitorEntry;
import org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor.SubTaskCompleteEvent;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.appenderator.UsedSegmentChecker;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of {@link ParallelIndexTaskRunner} to support best-effort roll-up. This runner can submit and
 * monitor multiple {@link ParallelIndexSubTask}s.
 * <p>
 * As its name indicates, distributed indexing is done in a single phase, i.e., without shuffling intermediate data. As
 * a result, this task can't be used for perfect rollup.
 */
public class SinglePhaseParallelIndexTaskRunner implements ParallelIndexTaskRunner<ParallelIndexSubTask>
{
  private static final Logger log = new Logger(SinglePhaseParallelIndexTaskRunner.class);

  private final TaskToolbox toolbox;
  private final String taskId;
  private final String groupId;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final Map<String, Object> context;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;
  private final int maxNumTasks;
  private final IndexingServiceClient indexingServiceClient;

  private final BlockingQueue<SubTaskCompleteEvent<ParallelIndexSubTask>> taskCompleteEvents =
      new LinkedBlockingDeque<>();

  /**
   * subTaskId -> report
   */
  private final ConcurrentHashMap<String, PushedSegmentsReport> segmentsMap = new ConcurrentHashMap<>();

  private volatile boolean subTaskScheduleAndMonitorStopped;
  private volatile TaskMonitor<ParallelIndexSubTask> taskMonitor;

  private int nextSpecId = 0;

  SinglePhaseParallelIndexTaskRunner(
      TaskToolbox toolbox,
      String taskId,
      String groupId,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient
  )
  {
    this.toolbox = toolbox;
    this.taskId = taskId;
    this.groupId = groupId;
    this.ingestionSchema = ingestionSchema;
    this.context = context;
    this.baseFirehoseFactory = (FiniteFirehoseFactory) ingestionSchema.getIOConfig().getFirehoseFactory();
    this.maxNumTasks = ingestionSchema.getTuningConfig().getMaxNumSubTasks();
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
  }

  @Override
  public TaskState run() throws Exception
  {
    if (baseFirehoseFactory.getNumSplits() == 0) {
      log.warn("There's no input split to process");
      return TaskState.SUCCESS;
    }

    final Iterator<ParallelIndexSubTaskSpec> subTaskSpecIterator = subTaskSpecIterator().iterator();
    final long taskStatusCheckingPeriod = ingestionSchema.getTuningConfig().getTaskStatusCheckPeriodMs();

    taskMonitor = new TaskMonitor<>(
        Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient"),
        ingestionSchema.getTuningConfig().getMaxRetry(),
        baseFirehoseFactory.getNumSplits()
    );
    TaskState state = TaskState.RUNNING;

    taskMonitor.start(taskStatusCheckingPeriod);

    try {
      log.info("Submitting initial tasks");
      // Submit initial tasks
      while (isRunning() && subTaskSpecIterator.hasNext() && taskMonitor.getNumRunningTasks() < maxNumTasks) {
        submitNewTask(taskMonitor, subTaskSpecIterator.next());
      }

      log.info("Waiting for subTasks to be completed");
      while (isRunning()) {
        final SubTaskCompleteEvent<ParallelIndexSubTask> taskCompleteEvent = taskCompleteEvents.poll(
            taskStatusCheckingPeriod,
            TimeUnit.MILLISECONDS
        );

        if (taskCompleteEvent != null) {
          final TaskState completeState = taskCompleteEvent.getLastState();
          switch (completeState) {
            case SUCCESS:
              final TaskStatusPlus completeStatus = taskCompleteEvent.getLastStatus();
              if (completeStatus == null) {
                throw new ISE("Last status of complete task is missing!");
              }
              // Pushed segments of complete tasks are supposed to be already reported.
              if (!segmentsMap.containsKey(completeStatus.getId())) {
                throw new ISE("Missing reports from task[%s]!", completeStatus.getId());
              }

              if (!subTaskSpecIterator.hasNext()) {
                // We have no more subTasks to run
                if (taskMonitor.getNumRunningTasks() == 0 && taskCompleteEvents.size() == 0) {
                  subTaskScheduleAndMonitorStopped = true;
                  if (taskMonitor.isSucceeded()) {
                    // Publishing all segments reported so far
                    publish(toolbox);

                    // Succeeded
                    state = TaskState.SUCCESS;
                  } else {
                    // Failed
                    final SinglePhaseParallelIndexingProgress monitorStatus = taskMonitor.getProgress();
                    throw new ISE(
                        "Expected for [%d] tasks to succeed, but we got [%d] succeeded tasks and [%d] failed tasks",
                        monitorStatus.getExpectedSucceeded(),
                        monitorStatus.getSucceeded(),
                        monitorStatus.getFailed()
                    );
                  }
                }
              } else if (taskMonitor.getNumRunningTasks() < maxNumTasks) {
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
                log.error("Failed because of the failed sub task[%s]", lastStatus.getId());
              } else {
                final ParallelIndexSubTaskSpec spec =
                    (ParallelIndexSubTaskSpec) taskCompleteEvent.getSpec();
                log.error(
                    "Failed to run sub tasks for inputSplits[%s]",
                    getSplitsIfSplittable(spec.getIngestionSpec().getIOConfig().getFirehoseFactory())
                );
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

  @Override
  public void stopGracefully()
  {
    subTaskScheduleAndMonitorStopped = true;
    stopInternal();
  }

  /**
   * Stop task scheduling and monitoring, and kill all running tasks.
   * This method is thread-safe.
   */
  private void stopInternal()
  {
    log.info("Cleaning up resources");

    taskCompleteEvents.clear();
    if (taskMonitor != null) {
      taskMonitor.stop();
    }
  }

  private boolean isRunning()
  {
    return !subTaskScheduleAndMonitorStopped && !Thread.currentThread().isInterrupted();
  }

  @VisibleForTesting
  TaskToolbox getToolbox()
  {
    return toolbox;
  }

  @VisibleForTesting
  ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @VisibleForTesting
  @Nullable
  TaskMonitor<ParallelIndexSubTask> getTaskMonitor()
  {
    return taskMonitor;
  }

  @Override
  public void collectReport(PushedSegmentsReport report)
  {
    // subTasks might send their reports multiple times because of the HTTP retry.
    // Here, we simply make sure the current report is exactly same with the previous one.
    segmentsMap.compute(report.getTaskId(), (taskId, prevReport) -> {
      if (prevReport != null) {
        Preconditions.checkState(
            prevReport.getNewSegments().equals(report.getNewSegments()),
            "task[%s] sent two or more reports and previous report[%s] is different from the current one[%s]",
            taskId,
            prevReport,
            report
        );
      }
      return report;
    });
  }

  @Override
  public SinglePhaseParallelIndexingProgress getProgress()
  {
    return taskMonitor == null ? SinglePhaseParallelIndexingProgress.notRunning() : taskMonitor.getProgress();
  }

  @Override
  public Set<String> getRunningTaskIds()
  {
    return taskMonitor == null ? Collections.emptySet() : taskMonitor.getRunningTaskIds();
  }

  @Override
  public List<SubTaskSpec<ParallelIndexSubTask>> getSubTaskSpecs()
  {
    if (taskMonitor != null) {
      final List<SubTaskSpec<ParallelIndexSubTask>> runningSubTaskSpecs = taskMonitor.getRunningSubTaskSpecs();
      final List<SubTaskSpec<ParallelIndexSubTask>> completeSubTaskSpecs = taskMonitor
          .getCompleteSubTaskSpecs();
      // Deduplicate subTaskSpecs because some subTaskSpec might exist both in runningSubTaskSpecs and
      // completeSubTaskSpecs.
      final Map<String, SubTaskSpec<ParallelIndexSubTask>> subTaskSpecMap = new HashMap<>(
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
  public List<SubTaskSpec<ParallelIndexSubTask>> getRunningSubTaskSpecs()
  {
    return taskMonitor == null ? Collections.emptyList() : taskMonitor.getRunningSubTaskSpecs();
  }

  @Override
  public List<SubTaskSpec<ParallelIndexSubTask>> getCompleteSubTaskSpecs()
  {
    return taskMonitor == null ? Collections.emptyList() : taskMonitor.getCompleteSubTaskSpecs();
  }

  @Nullable
  @Override
  public SubTaskSpec<ParallelIndexSubTask> getSubTaskSpec(String subTaskSpecId)
  {
    if (taskMonitor != null) {
      // Running tasks should be checked first because, in taskMonitor, subTaskSpecs are removed from runningTasks after
      // adding them to taskHistory.
      final MonitorEntry monitorEntry = taskMonitor.getRunningTaskMonitorEntry(subTaskSpecId);
      final TaskHistory<ParallelIndexSubTask> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(subTaskSpecId);
      final SubTaskSpec<ParallelIndexSubTask> subTaskSpec;

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
      final TaskHistory<ParallelIndexSubTask> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(subTaskSpecId);

      final SubTaskSpecStatus subTaskSpecStatus;

      if (monitorEntry != null) {
        subTaskSpecStatus = new SubTaskSpecStatus(
            (ParallelIndexSubTaskSpec) monitorEntry.getSpec(),
            monitorEntry.getRunningStatus(),
            monitorEntry.getTaskHistory()
        );
      } else {
        if (taskHistory != null && !taskHistory.isEmpty()) {
          subTaskSpecStatus = new SubTaskSpecStatus(
              (ParallelIndexSubTaskSpec) taskHistory.getSpec(),
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
  public TaskHistory<ParallelIndexSubTask> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId)
  {
    if (taskMonitor == null) {
      return null;
    } else {
      return taskMonitor.getCompleteSubTaskSpecHistory(subTaskSpecId);
    }
  }

  private void publish(TaskToolbox toolbox) throws IOException
  {
    final UsedSegmentChecker usedSegmentChecker = new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient());
    final Set<DataSegment> oldSegments = new HashSet<>();
    final Set<DataSegment> newSegments = new HashSet<>();
    segmentsMap
        .values()
        .forEach(report -> {
          oldSegments.addAll(report.getOldSegments());
          newSegments.addAll(report.getNewSegments());
        });
    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient().submit(
            SegmentTransactionalInsertAction.overwriteAction(segmentsToBeOverwritten, segmentsToPublish)
        );
    final boolean published = newSegments.isEmpty()
                              || publisher.publishSegments(oldSegments, newSegments, null).isSuccess();

    if (published) {
      log.info("Published [%d] segments", newSegments.size());
    } else {
      log.info("Transaction failure while publishing segments, checking if someone else beat us to it.");
      final Set<SegmentIdWithShardSpec> segmentsIdentifiers = segmentsMap
          .values()
          .stream()
          .flatMap(report -> report.getNewSegments().stream())
          .map(SegmentIdWithShardSpec::fromDataSegment)
          .collect(Collectors.toSet());
      if (usedSegmentChecker.findUsedSegments(segmentsIdentifiers)
                            .equals(newSegments)) {
        log.info("Our segments really do exist, awaiting handoff.");
      } else {
        throw new ISE("Failed to publish segments[%s]", newSegments);
      }
    }
  }

  private void submitNewTask(
      TaskMonitor<ParallelIndexSubTask> taskMonitor,
      ParallelIndexSubTaskSpec spec
  )
  {
    log.info("Submit a new task for spec[%s] and inputSplit[%s]", spec.getId(), spec.getInputSplit());
    final ListenableFuture<SubTaskCompleteEvent<ParallelIndexSubTask>> future = taskMonitor.submit(spec);
    Futures.addCallback(
        future,
        new FutureCallback<SubTaskCompleteEvent<ParallelIndexSubTask>>()
        {
          @Override
          public void onSuccess(SubTaskCompleteEvent<ParallelIndexSubTask> completeEvent)
          {
            // this callback is called if a task completed wheter it succeeded or not.
            taskCompleteEvents.offer(completeEvent);
          }

          @Override
          public void onFailure(Throwable t)
          {
            // this callback is called only when there were some problems in TaskMonitor.
            log.error(t, "Error while running a task for subTaskSpec[%s]", spec);
            taskCompleteEvents.offer(SubTaskCompleteEvent.fail(spec, t));
          }
        }
    );
  }

  @VisibleForTesting
  int getAndIncrementNextSpecId()
  {
    return nextSpecId++;
  }

  @VisibleForTesting
  Stream<ParallelIndexSubTaskSpec> subTaskSpecIterator() throws IOException
  {
    return baseFirehoseFactory.getSplits().map(this::newTaskSpec);
  }

  @VisibleForTesting
  ParallelIndexSubTaskSpec newTaskSpec(InputSplit split)
  {
    return new ParallelIndexSubTaskSpec(
        taskId + "_" + getAndIncrementNextSpecId(),
        groupId,
        taskId,
        new ParallelIndexIngestionSpec(
            ingestionSchema.getDataSchema(),
            new ParallelIndexIOConfig(
                baseFirehoseFactory.withSplit(split),
                ingestionSchema.getIOConfig().isAppendToExisting()
            ),
            ingestionSchema.getTuningConfig()
        ),
        context,
        split
    );
  }

  private static List<InputSplit> getSplitsIfSplittable(FirehoseFactory firehoseFactory) throws IOException
  {
    if (firehoseFactory instanceof FiniteFirehoseFactory) {
      final FiniteFirehoseFactory<?, ?> finiteFirehoseFactory = (FiniteFirehoseFactory) firehoseFactory;
      return finiteFirehoseFactory.getSplits().collect(Collectors.toList());
    } else {
      throw new ISE("firehoseFactory[%s] is not splittable", firehoseFactory.getClass().getSimpleName());
    }
  }
}
