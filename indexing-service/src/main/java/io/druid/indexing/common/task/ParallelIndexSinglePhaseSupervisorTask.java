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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.TaskStatus;
import io.druid.data.input.FiniteFirehoseFactory;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputSplit;
import io.druid.indexer.TaskState;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIOConfig;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.appenderator.UsedSegmentChecker;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ParallelIndexSinglePhaseSupervisorTask is capable of running multiple subTasks for parallel indexing. This is
 * applicable if the input {@link FiniteFirehoseFactory} is splittable. While this task is running, it can submit
 * multiple child tasks to overlords. This task succeeds only when all its child tasks succeed; otherwise it fails.
 *
 * As its name indicates, distributed indexing is done in a single phase, i.e., without shuffling intermediate data. As
 * a result, this task can't be used for perfect rollup.
 */
public class ParallelIndexSinglePhaseSupervisorTask extends AbstractTask
{
  private static final Logger log = new Logger(ParallelIndexSinglePhaseSupervisorTask.class);
  private static final String TYPE = "parallelIndexSinglePhase";

  private final ParallelIndexSinglePhaseIngestionSpec ingestionSchema;
  private final FiniteFirehoseFactory baseFirehoseFactory;
  private final int maxNumTasks;
  private final TaskMonitor taskMonitor;

  private final BlockingQueue<TaskStatus> taskCompleteEvents = new LinkedBlockingDeque<>();
  private final List<DataSegment> segments = new ArrayList<>();

  private volatile boolean stopped;

  @JsonCreator
  public ParallelIndexSinglePhaseSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") ParallelIndexSinglePhaseIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        null,
        taskResource,
        ingestionSchema.getDataSchema().getDataSource(),
        context
    );

    this.ingestionSchema = ingestionSchema;

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    if (!(firehoseFactory instanceof FiniteFirehoseFactory)) {
      throw new IAE("[%s] should implement FiniteFirehoseFactory", firehoseFactory.getClass().getSimpleName());
    }

    this.baseFirehoseFactory = (FiniteFirehoseFactory) firehoseFactory;
    this.maxNumTasks = ingestionSchema.getTuningConfig().getMaxNumBatchTasks();
    this.taskMonitor = new TaskMonitor(indexingServiceClient, ingestionSchema.getTuningConfig().getMaxRetry());
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_BATCH_INDEX_TASK_PRIORITY);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty("spec")
  ParallelIndexSinglePhaseIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final Optional<SortedSet<Interval>> intervals = ingestionSchema.getDataSchema()
                                                                   .getGranularitySpec()
                                                                   .bucketIntervals();

    return !intervals.isPresent() || isReady(taskActionClient, intervals.get());
  }

  static boolean isReady(TaskActionClient actionClient, SortedSet<Interval> intervals) throws IOException
  {
    final List<TaskLock> locks = getTaskLocks(actionClient);
    if (locks.size() == 0) {
      try {
        Tasks.tryAcquireExclusiveLocks(actionClient, intervals);
      }
      catch (Exception e) {
        log.error(e, "Failed to acquire locks for intervals[%s]", intervals);
        return false;
      }
    }
    return true;
  }

  @Override
  public io.druid.indexing.common.TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    if (baseFirehoseFactory.isSplittable()) {
      return runParallel(toolbox);
    } else {
      log.warn(
          "firehoseFactory[%s] is not splittable. Running sequentially",
          baseFirehoseFactory.getClass().getSimpleName()
      );
      return runSequential(toolbox);
    }
  }

  private io.druid.indexing.common.TaskStatus runParallel(TaskToolbox toolbox) throws Exception
  {
    final Iterator<ParallelIndexSinglePhaseSubTask> subTaskIterator = subTaskIterator();
    final int numTotalTasks = baseFirehoseFactory.getNumSplits();
    final long taskStatusCheckingPeriod = ingestionSchema.getTuningConfig().getTaskStatusCheckingPeriodMs();
    TaskState state = TaskState.FAILED;

    log.info("Total number of tasks is [%d]", numTotalTasks);

    int numCompleteTasks = 0;
    taskMonitor.start(taskStatusCheckingPeriod);

    try {
      log.info("Submitting initial tasks");
      // Submit initial tasks
      while (subTaskIterator.hasNext() && taskMonitor.getNumRunningTasks() < maxNumTasks) {
        submitNewTask(subTaskIterator.next());
      }

      while (!stopped && !Thread.currentThread().isInterrupted()) {
        final TaskStatus taskCompleteEvent = taskCompleteEvents.poll(taskStatusCheckingPeriod, TimeUnit.MILLISECONDS);

        if (taskCompleteEvent != null) {
          final TaskState completeState = taskCompleteEvent.getStatusCode();
          if (completeState == null) {
            throw new ISE("Complete state of task[%s] is null", taskCompleteEvent.getId());
          }
          switch (completeState) {
            case SUCCESS:
              numCompleteTasks++;
              segments.addAll((Collection<DataSegment>) taskCompleteEvent.getReport().getPayload());
              log.info("[%d/%d] tasks succeeded", numCompleteTasks, numTotalTasks);
              if (!subTaskIterator.hasNext()) {
                if (taskMonitor.getNumRunningTasks() == 0 && taskCompleteEvents.size() == 0) {
                  stopped = true;
                  if (numCompleteTasks == numTotalTasks) {
                    // Publishing all segments reported so far
                    publish(toolbox);

                    // Succeeded
                    state = TaskState.SUCCESS;
                  } else {
                    // Failed
                    throw new ISE("Expected to complete [%d] tasks, but we got [%d]", numTotalTasks, numCompleteTasks);
                  }
                }
              } else if (taskMonitor.getNumRunningTasks() < maxNumTasks) {
                submitNewTask(subTaskIterator.next());
              }
              break;
            case FAILED:
              // TaskMonitor already tried everything it can do for failed tasks. We failed.
              state = TaskState.FAILED;
              stopped = true;
              log.error("Failed because of the failed sub task[%s]", taskCompleteEvent.getId());
              break;
            default:
              throw new ISE("Complete task[%s] is in an invalid state[%s]", taskCompleteEvent.getId(), completeState);
          }
        }
      }
    }
    finally {
      // Cleanup resources
      taskCompleteEvents.clear();
      taskMonitor.stop();

      if (state != TaskState.SUCCESS) {
        // if this fails, kill all sub tasks
        taskMonitor.killAll();
      }
    }

    return io.druid.indexing.common.TaskStatus.fromCode(getId(), state);
  }

  private io.druid.indexing.common.TaskStatus runSequential(TaskToolbox toolbox) throws Exception
  {
    return new IndexTask(
        getId(),
        getGroupId(),
        getTaskResource(),
        getDataSource(),
        new IndexIngestionSpec(
            getIngestionSchema().getDataSchema(),
            getIngestionSchema().getIOConfig(),
            convertToIndexTuningConfig(getIngestionSchema().getTuningConfig())
        ),
        getContext()
    ).run(toolbox);
  }

  private static IndexTuningConfig convertToIndexTuningConfig(ParallelIndexSinglePhaseTuningConfig tuningConfig)
  {
    return new IndexTuningConfig(
        tuningConfig.getTargetPartitionSize(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxTotalRows(),
        null,
        tuningConfig.getNumShards(),
        tuningConfig.getIndexSpec(),
        tuningConfig.getMaxPendingPersists(),
        true,
        tuningConfig.isForceExtendableShardSpecs(),
        tuningConfig.isForceGuaranteedRollup(),
        tuningConfig.isReportParseExceptions(),
        null,
        tuningConfig.getPushTimeout(),
        tuningConfig.getSegmentWriteOutMediumFactory()
    );
  }

  private void publish(TaskToolbox toolbox) throws IOException
  {
    final TransactionalSegmentPublisher publisher = (segments, commitMetadata) -> {
      final SegmentTransactionalInsertAction action = new SegmentTransactionalInsertAction(segments);
      return toolbox.getTaskActionClient().submit(action).isSuccess();
    };
    final UsedSegmentChecker usedSegmentChecker = new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient());
    final Set<DataSegment> segmentsToPublish = ImmutableSet.copyOf(segments);
    final boolean published = publisher.publishSegments(segmentsToPublish, null);

    if (published) {
      log.info("Published segments");
    } else {
      log.info("Transaction failure while publishing segments, checking if someone else beat us to it.");
      final Set<SegmentIdentifier> segmentsIdentifiers = segments
          .stream()
          .map(SegmentIdentifier::fromDataSegment)
          .collect(Collectors.toSet());
      if (usedSegmentChecker.findUsedSegments(segmentsIdentifiers)
                            .equals(segmentsToPublish)) {
        log.info("Our segments really do exist, awaiting handoff.");
      } else {
        throw new ISE("Failed to publish segments[%s]", segmentsToPublish);
      }
    }
  }

  private void submitNewTask(ParallelIndexSinglePhaseSubTask task)
  {
    final ListenableFuture<TaskStatus> future = taskMonitor.submit(task);
    Futures.addCallback(
        future,
        new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(TaskStatus taskStatus)
          {
            taskCompleteEvents.offer(taskStatus);
          }

          @Override
          public void onFailure(Throwable t)
          {
            taskCompleteEvents.offer(new TaskStatus(task.getId(), TaskState.FAILED, null, -1));
          }
        }
    );
  }

  @VisibleForTesting
  Iterator<ParallelIndexSinglePhaseSubTask> subTaskIterator() throws IOException
  {
    return Iterators.transform(baseFirehoseFactory.getSplits(), split -> newTask((InputSplit<?>) split));
  }

  @VisibleForTesting
  ParallelIndexSinglePhaseSubTask newTask(InputSplit<?> split)
  {
    return new ParallelIndexSinglePhaseSubTask(
        null,
        getGroupId(),
        null,
        new IndexIngestionSpec(
            ingestionSchema.getDataSchema(),
            new IndexIOConfig(
                baseFirehoseFactory.withSplit(split),
                ingestionSchema.getIOConfig().isAppendToExisting()
            ),
            convertToIndexTuningConfig(ingestionSchema.getTuningConfig())
        ),
        getContext()
    );
  }
}
