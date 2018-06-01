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
import com.google.common.base.Preconditions;
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
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import io.druid.indexing.common.task.ParallelIndexSupervisorTask.Status;
import io.druid.indexing.common.task.TaskMonitor.MonitorEntry;
import io.druid.indexing.common.task.TaskMonitor.SubTaskCompleteEvent;
import io.druid.indexing.common.task.TaskMonitor.TaskHistory;
import io.druid.java.util.common.ISE;
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

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An implementation of {@link ParallelIndexTaskRunner} to support best-effort roll-up. This runner can submit and
 * monitor multiple {@link ParallelIndexSubTask}s.
 *
 * As its name indicates, distributed indexing is done in a single phase, i.e., without shuffling intermediate data. As
 * a result, this task can't be used for perfect rollup.
 */
public class SinglePhaseParallelIndexTaskRunner implements ParallelIndexTaskRunner, ChatHandler
{
  private static final Logger log = new Logger(SinglePhaseParallelIndexTaskRunner.class);

  private final String taskId;
  private final String groupId;
  private final ParallelIndexIngestionSpec ingestionSchema;
  private final Map<String, Object> context;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;
  private final int maxNumTasks;
  private final IndexingServiceClient indexingServiceClient;
  private final ChatHandlerProvider chatHandlerProvider;
  private final AuthorizerMapper authorizerMapper;

  private final BlockingQueue<SubTaskCompleteEvent<ParallelIndexSubTask>> taskCompleteEvents =
      new LinkedBlockingDeque<>();

  // subTaskId -> report
  private final ConcurrentMap<String, PushedSegmentsReport> segmentsMap = new ConcurrentHashMap<>();

  private volatile boolean stopped;
  private volatile TaskMonitor<ParallelIndexSubTask> taskMonitor;

  private int nextSpecId = 0;

  SinglePhaseParallelIndexTaskRunner(
      @JsonProperty("id") String taskId,
      String groupId,
      @JsonProperty("spec") ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject IndexingServiceClient indexingServiceClient,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject AuthorizerMapper authorizerMapper
  )
  {
    this.taskId = taskId;
    this.groupId = groupId;
    this.ingestionSchema = ingestionSchema;
    this.context = context;
    this.baseFirehoseFactory = (FiniteFirehoseFactory) ingestionSchema.getIOConfig().getFirehoseFactory();
    this.maxNumTasks = ingestionSchema.getTuningConfig().getMaxNumSubTasks();
    this.indexingServiceClient = Preconditions.checkNotNull(indexingServiceClient, "indexingServiceClient");
    this.chatHandlerProvider = Preconditions.checkNotNull(chatHandlerProvider, "chatHandlerProvider");
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public TaskState run(TaskToolbox toolbox) throws Exception
  {
    log.info(
        "Found chat handler of class[%s]",
        Preconditions.checkNotNull(chatHandlerProvider, "chatHandlerProvider").getClass().getName()
    );
    chatHandlerProvider.register(taskId, this, false);

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
      while (subTaskSpecIterator.hasNext() && taskMonitor.getNumRunningTasks() < maxNumTasks) {
        submitNewTask(taskMonitor, subTaskSpecIterator.next());
      }

      log.info("Waiting for subTasks to be completed");
      while (!stopped && !Thread.currentThread().isInterrupted()) {
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
                  stopped = true;
                  if (taskMonitor.isSucceeded()) {
                    // Publishing all segments reported so far
                    publish(toolbox);

                    // Succeeded
                    state = TaskState.SUCCESS;
                  } else {
                    // Failed
                    final Status monitorStatus = taskMonitor.getStatus();
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
              stopped = true;
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
      log.info("Cleaning up resources");
      // Cleanup resources
      taskCompleteEvents.clear();
      taskMonitor.stop();
      chatHandlerProvider.unregister(taskId);

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

    return state;
  }

  @VisibleForTesting
  ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  private String getDataSource()
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  // Internal API for collecting reports from subTasks

  @POST
  @Path("/report")
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  public Response report(
      PushedSegmentsReport report,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(
        req,
        Action.WRITE,
        getDataSource(),
        authorizerMapper
    );
    collectReport(report);
    return Response.status(Response.Status.OK).build();
  }

  @VisibleForTesting
  void collectReport(PushedSegmentsReport report)
  {
    // subTasks might send their reports multiple times because of the HTTP retry.
    // Here, we simply make sure the current report is exactly same with the previous one.
    segmentsMap.compute(report.getTaskId(), (taskId, prevReport) -> {
      if (prevReport != null) {
        Preconditions.checkState(
            prevReport.getSegments().equals(report.getSegments()),
            "task[%s] sent two or more reports and previous report[%s] is different from the current one[%s]",
            taskId,
            prevReport,
            report
        );
      }
      return report;
    });
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

  // External APIs to get running status

  @GET
  @Path("/mode")
  @Produces(MediaType.APPLICATION_JSON)
  public Response isRunningInParallel(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    return Response.ok(baseFirehoseFactory.isSplittable() ? "parallel" : "sequential").build();
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getStatus(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    return Response.ok(taskMonitor == null ? Status.empty() : taskMonitor.getStatus()).build();
  }

  @GET
  @Path("/subtasks/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final Set<String> runningTasks = taskMonitor == null ? Collections.emptySet() : taskMonitor.getRunningTaskIds();
    return Response.ok(runningTasks).build();
  }

  @GET
  @Path("/subtaskspecs")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
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
      return Response.ok(new ArrayList<>(subTaskSpecMap.values())).build();
    } else {
      return Response.ok(Collections.emptyList()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final List<SubTaskSpec<ParallelIndexSubTask>> runningSubTaskSpecs = taskMonitor == null ?
                                                                        Collections.emptyList() :
                                                                        taskMonitor.getRunningSubTaskSpecs();
    return Response.ok(runningSubTaskSpecs).build();
  }

  @GET
  @Path("/subtaskspecs/complete")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final List<SubTaskSpec<ParallelIndexSubTask>> completeSubTaskSpecs =
        taskMonitor == null ?
        Collections.emptyList() :
        taskMonitor.getCompleteSubTaskSpecs();
    return Response.ok(completeSubTaskSpecs).build();
  }

  @GET
  @Path("/subtaskspec/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpec(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (taskMonitor != null) {
      // Running tasks should be checked first because, in taskMonitor, subTaskSpecs are removed from runningTasks after
      // adding them to taskHistory.
      final MonitorEntry monitorEntry = taskMonitor.getRunningTaskMonitorEntory(id);
      final TaskHistory<ParallelIndexSubTask> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(id);
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

      if (subTaskSpec == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(subTaskSpec).build();
      }
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }

  @GET
  @Path("/subtaskspec/{id}/state")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskState(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (taskMonitor == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      // Running tasks should be checked first because, in taskMonitor, subTaskSpecs are removed from runningTasks after
      // adding them to taskHistory.
      final MonitorEntry monitorEntry = taskMonitor.getRunningTaskMonitorEntory(id);
      final TaskHistory<ParallelIndexSubTask> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(id);

      final SubTaskStateResponse subTaskStateResponse;

      if (monitorEntry != null) {
        subTaskStateResponse = new SubTaskStateResponse(
            (ParallelIndexSubTaskSpec) monitorEntry.getSpec(),
            monitorEntry.getRunningStatus(),
            monitorEntry.getTaskHistory()
        );
      } else {
        if (taskHistory != null && !taskHistory.isEmpty()) {
          subTaskStateResponse = new SubTaskStateResponse(
              (ParallelIndexSubTaskSpec) taskHistory.getSpec(),
              null,
              taskHistory.getAttemptHistory()
          );
        } else {
          subTaskStateResponse = null;
        }
      }

      if (subTaskStateResponse == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(subTaskStateResponse).build();
      }
    }
  }

  @GET
  @Path("/subtaskspec/{id}/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteSubTaskSpecAttemptHistory(
      @PathParam("id") String id,
      @Context final HttpServletRequest req
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (taskMonitor == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      final TaskHistory<ParallelIndexSubTask> taskHistory = taskMonitor.getCompleteSubTaskSpecHistory(id);

      if (taskHistory == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(taskHistory.getAttemptHistory()).build();
      }
    }
  }

  static class SubTaskStateResponse
  {
    private final ParallelIndexSubTaskSpec spec;
    @Nullable
    private final TaskStatusPlus currentStatus;
    private final List<TaskStatusPlus> taskHistory;

    @JsonCreator
    public SubTaskStateResponse(
        @JsonProperty("spec") ParallelIndexSubTaskSpec spec,
        @JsonProperty("currentStatus") @Nullable TaskStatusPlus currentStatus,
        @JsonProperty("taskHistory") List<TaskStatusPlus> taskHistory
    )
    {
      this.spec = spec;
      this.currentStatus = currentStatus;
      this.taskHistory = taskHistory;
    }

    @JsonProperty
    public ParallelIndexSubTaskSpec getSpec()
    {
      return spec;
    }

    @JsonProperty
    @Nullable
    public TaskStatusPlus getCurrentStatus()
    {
      return currentStatus;
    }

    @JsonProperty
    public List<TaskStatusPlus> getTaskHistory()
    {
      return taskHistory;
    }
  }
}
