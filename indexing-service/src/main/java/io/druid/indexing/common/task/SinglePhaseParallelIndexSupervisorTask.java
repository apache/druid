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
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.data.input.FiniteFirehoseFactory;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputSplit;
import io.druid.indexer.TaskState;
import io.druid.indexer.TaskStatusPlus;
import io.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import io.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import io.druid.indexing.common.task.TaskMonitor.SubTaskCompleteEvent;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import io.druid.segment.realtime.appenderator.UsedSegmentChecker;
import io.druid.segment.realtime.firehose.ChatHandler;
import io.druid.segment.realtime.firehose.ChatHandlerProvider;
import io.druid.segment.realtime.firehose.ChatHandlers;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizerMapper;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * SinglePhaseParallelIndexSupervisorTask is capable of running multiple subTasks for parallel indexing. This is
 * applicable if the input {@link FiniteFirehoseFactory} is splittable. While this task is running, it can submit
 * multiple child tasks to overlords. This task succeeds only when all its child tasks succeed; otherwise it fails.
 *
 * As its name indicates, distributed indexing is done in a single phase, i.e., without shuffling intermediate data. As
 * a result, this task can't be used for perfect rollup.
 */
public class SinglePhaseParallelIndexSupervisorTask extends AbstractTask implements ChatHandler
{
  private static final Logger log = new Logger(SinglePhaseParallelIndexSupervisorTask.class);
  private static final String TYPE = "index_single_phase_parallel";

  private final SinglePhaseParallelIndexIngestionSpec ingestionSchema;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;
  private final int maxNumTasks;
  private final IndexingServiceClient indexingServiceClient;
  private final ChatHandlerProvider chatHandlerProvider;
  private final AuthorizerMapper authorizerMapper;

  private final BlockingQueue<SubTaskCompleteEvent<SinglePhaseParallelIndexSubTask>> taskCompleteEvents =
      new LinkedBlockingDeque<>();

  // subtaskId -> report
  private final ConcurrentMap<String, PushedSegmentsReport> segmentsMap = new ConcurrentHashMap<>();

  private volatile boolean stopped;
  private int nextSpecId = 0;

  @JsonCreator
  public SinglePhaseParallelIndexSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") SinglePhaseParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject @Nullable IndexingServiceClient indexingServiceClient, // null in overlords
      @JacksonInject @Nullable ChatHandlerProvider chatHandlerProvider,     // null in overlords
      @JacksonInject AuthorizerMapper authorizerMapper
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
    this.indexingServiceClient = indexingServiceClient;
    this.chatHandlerProvider = chatHandlerProvider;
    this.authorizerMapper = authorizerMapper;
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
  public SinglePhaseParallelIndexIngestionSpec getIngestionSchema()
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
  public TaskStatus run(TaskToolbox toolbox) throws Exception
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

  private TaskStatus runParallel(TaskToolbox toolbox) throws Exception
  {
    final TaskMonitor<SinglePhaseParallelIndexSubTask> taskMonitor = new TaskMonitor<>(
        Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient"),
        ingestionSchema.getTuningConfig().getMaxRetry()
    );

    log.info(
        "Found chat handler of class[%s]",
        Preconditions.checkNotNull(chatHandlerProvider, "chatHandlerProvider").getClass().getName()
    );
    chatHandlerProvider.register(getId(), this, false);

    final Iterator<SinglePhaseParallelIndexSubTaskSpec> subTaskSpecIterator = subTaskSpecIterator();
    final int numTotalTasks = baseFirehoseFactory.getNumSplits();
    final long taskStatusCheckingPeriod = ingestionSchema.getTuningConfig().getTaskStatusCheckPeriodMs();
    TaskState state = TaskState.FAILED;

    log.info("Total number of tasks is [%d]", numTotalTasks);

    int numCompleteTasks = 0;
    taskMonitor.start(taskStatusCheckingPeriod);

    try {
      log.info("Submitting initial tasks");
      // Submit initial tasks
      while (subTaskSpecIterator.hasNext() && taskMonitor.getNumRunningTasks() < maxNumTasks) {
        submitNewTask(taskMonitor, subTaskSpecIterator.next());
      }

      log.info("Waiting for subTasks to be completed");
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        final SubTaskCompleteEvent<SinglePhaseParallelIndexSubTask> taskCompleteEvent = taskCompleteEvents.poll(
            taskStatusCheckingPeriod,
            TimeUnit.MILLISECONDS
        );

        if (taskCompleteEvent != null) {
          final TaskState completeState = taskCompleteEvent.getLastState();
          switch (completeState) {
            case SUCCESS:
              numCompleteTasks++;
              final TaskStatusPlus completeStatus = taskCompleteEvent.getLastStatus();
              if (completeStatus == null) {
                throw new ISE("Last status of complete task is missing!");
              }
              // Pushed segments of complete tasks are supposed to be already reported.
              if (!segmentsMap.containsKey(completeStatus.getId())) {
                throw new ISE("Missing reports from task[%s]!", completeStatus.getId());
              }

              log.info("[%d/%d] tasks succeeded", numCompleteTasks, numTotalTasks);
              if (!subTaskSpecIterator.hasNext()) {
                if (taskMonitor.getNumRunningTasks() == 0 && taskCompleteEvents.size() == 0) {
                  stopped = true;
                  if (numCompleteTasks == numTotalTasks) {
                    // Publishing all segments reported so far
                    publish(toolbox);

                    // Succeeded
                    state = TaskState.SUCCESS;
                  } else {
                    // Failed
                    throw new ISE(
                        "Expected to complete [%d] tasks, but we got [%d] tasks",
                        numTotalTasks,
                        numCompleteTasks
                    );
                  }
                }
              } else if (taskMonitor.getNumRunningTasks() < maxNumTasks) {
                submitNewTask(taskMonitor, subTaskSpecIterator.next());
              }
              break;
            case FAILED:
              // TaskMonitor already tried everything it can do for failed tasks. We failed.
              state = TaskState.FAILED;
              stopped = true;
              final TaskStatusPlus lastStatus = taskCompleteEvent.getLastStatus();
              if (lastStatus != null) {
                log.error("Failed because of the failed sub task[%s]", lastStatus.getId());
              } else {
                final SinglePhaseParallelIndexSubTaskSpec spec =
                    (SinglePhaseParallelIndexSubTaskSpec) taskCompleteEvent.getSpec();
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
      log.info("Cleaning up resources");
      // Cleanup resources
      taskCompleteEvents.clear();
      taskMonitor.stop();
      chatHandlerProvider.unregister(getId());

      if (state != TaskState.SUCCESS) {
        log.info(
            "This task is finished with [%s] state. Killing [%d] remaining subtasks.",
            state,
            taskMonitor.getNumRunningTasks()
        );
        // if this fails, kill all sub tasks
        // Note: this doesn't work when this task is killed by users. We need a way for gracefully shutting down tasks
        // for resource cleanup.
        taskMonitor.killAll();
      }
    }

    return TaskStatus.fromCode(getId(), state);
  }

  private TaskStatus runSequential(TaskToolbox toolbox) throws Exception
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

  @POST
  @Path("/report")
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  public Response report(
      PushedSegmentsReport report,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, getDataSource(), authorizerMapper);
    collectReport(report);
    return Response.status(Response.Status.OK).build();
  }

  @VisibleForTesting
  void collectReport(PushedSegmentsReport report)
  {
    if (segmentsMap.put(report.getTaskId(), report) != null) {
      throw new ISE("Dupliate task report from task[%s]", report.getTaskId());
    }
  }

  private static IndexTuningConfig convertToIndexTuningConfig(SinglePhaseParallelIndexTuningConfig tuningConfig)
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
        false,
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
    final Set<DataSegment> segmentsToPublish = segmentsMap
        .values()
        .stream()
        .flatMap(report -> report.getSegments().stream())
        .collect(Collectors.toSet());
    final boolean published = publisher.publishSegments(segmentsToPublish, null);

    if (published) {
      log.info("Published segments");
    } else {
      log.info("Transaction failure while publishing segments, checking if someone else beat us to it.");
      final Set<SegmentIdentifier> segmentsIdentifiers = segmentsMap
          .values()
          .stream()
          .flatMap(report -> report.getSegments().stream())
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

  private void submitNewTask(
      TaskMonitor<SinglePhaseParallelIndexSubTask> taskMonitor,
      SinglePhaseParallelIndexSubTaskSpec spec
  )
  {
    final ListenableFuture<SubTaskCompleteEvent<SinglePhaseParallelIndexSubTask>> future = taskMonitor.submit(spec);
    Futures.addCallback(
        future,
        new FutureCallback<SubTaskCompleteEvent<SinglePhaseParallelIndexSubTask>>()
        {
          @Override
          public void onSuccess(SubTaskCompleteEvent<SinglePhaseParallelIndexSubTask> completeEvent)
          {
            // this callback is called if a task completed wheter it succeeded or not.
            taskCompleteEvents.offer(completeEvent);
          }

          @Override
          public void onFailure(Throwable t)
          {
            // this callback is called only when there were some problems in TaskMonitor.
            try {
              log.error(
                  t,
                  "Error while running a task for inputSplits[%s]",
                  getSplitsIfSplittable(spec.getIngestionSpec().getIOConfig().getFirehoseFactory())
              );
            }
            catch (IOException e) {
              t.addSuppressed(new RE(e, "Error while getting splits for error logging"));
            }
            taskCompleteEvents.offer(new SubTaskCompleteEvent<>(spec, TaskState.FAILED, null));
          }
        }
    );
  }

  @VisibleForTesting
  int getAndIncreaseNextSpecId()
  {
    return nextSpecId++;
  }

  @VisibleForTesting
  Iterator<SinglePhaseParallelIndexSubTaskSpec> subTaskSpecIterator() throws IOException
  {
    return Iterators.transform(baseFirehoseFactory.getSplits(), this::newTaskSpec);
  }

  @VisibleForTesting
  SinglePhaseParallelIndexSubTaskSpec newTaskSpec(InputSplit split)
  {
    return new SinglePhaseParallelIndexSubTaskSpec(
        getId() + "_" + getAndIncreaseNextSpecId(),
        getGroupId(),
        getId(),
        new SinglePhaseParallelIndexIngestionSpec(
            ingestionSchema.getDataSchema(),
            new SinglePhaseParallelIndexIOConfig(
                baseFirehoseFactory.withSplit(split),
                ingestionSchema.getIOConfig().isAppendToExisting()
            ),
            ingestionSchema.getTuningConfig()
        ),
        getContext()
    );
  }

  private static List<InputSplit> getSplitsIfSplittable(FirehoseFactory firehoseFactory) throws IOException
  {
    if (firehoseFactory instanceof FiniteFirehoseFactory) {
      final FiniteFirehoseFactory<?, ?> finiteFirehoseFactory = (FiniteFirehoseFactory) firehoseFactory;
      return Lists.newArrayList(finiteFirehoseFactory.getSplits());
    } else {
      throw new ISE("firehoseFactory[%s] is not splittable", firehoseFactory.getClass().getSimpleName());
    }
  }
}
