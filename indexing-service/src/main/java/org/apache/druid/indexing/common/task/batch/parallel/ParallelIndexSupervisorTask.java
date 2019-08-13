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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskRunner.SubTaskSpecStatus;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

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
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * ParallelIndexSupervisorTask is capable of running multiple subTasks for parallel indexing. This is
 * applicable if the input {@link FiniteFirehoseFactory} is splittable. While this task is running, it can submit
 * multiple child tasks to overlords. This task succeeds only when all its child tasks succeed; otherwise it fails.
 *
 * @see ParallelIndexTaskRunner
 */
public class ParallelIndexSupervisorTask extends AbstractBatchIndexTask implements ChatHandler
{
  public static final String TYPE = "index_parallel";

  private static final Logger LOG = new Logger(ParallelIndexSupervisorTask.class);

  private final ParallelIndexIngestionSpec ingestionSchema;
  private final FiniteFirehoseFactory<?, ?> baseFirehoseFactory;
  private final IndexingServiceClient indexingServiceClient;
  private final ChatHandlerProvider chatHandlerProvider;
  private final AuthorizerMapper authorizerMapper;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final AppenderatorsManager appenderatorsManager;

  /**
   * If intervals are missing in the granularitySpec, parallel index task runs in "dynamic locking mode".
   * In this mode, sub tasks ask new locks whenever they see a new row which is not covered by existing locks.
   * If this task is overwriting existing segments, then we should know this task is changing segment granularity
   * in advance to know what types of lock we should use. However, if intervals are missing, we can't know
   * the segment granularity of existing segments until the task reads all data because we don't know what segments
   * are going to be overwritten. As a result, we assume that segment granularity is going to be changed if intervals
   * are missing and force to use timeChunk lock.
   *
   * This variable is initialized in the constructor and used in {@link #run} to log that timeChunk lock was enforced
   * in the task logs.
   */
  private final boolean missingIntervalsInOverwriteMode;

  private final ConcurrentHashMap<Interval, AtomicInteger> partitionNumCountersPerInterval = new ConcurrentHashMap<>();

  private volatile ParallelIndexTaskRunner runner;
  private volatile IndexTask sequentialIndexTask;

  // toolbox is initlized when run() is called, and can be used for processing HTTP endpoint requests.
  private volatile TaskToolbox toolbox;

  @JsonCreator
  public ParallelIndexSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject @Nullable IndexingServiceClient indexingServiceClient, // null in overlords
      @JacksonInject @Nullable ChatHandlerProvider chatHandlerProvider,     // null in overlords
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject AppenderatorsManager appenderatorsManager
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
    this.indexingServiceClient = indexingServiceClient;
    this.chatHandlerProvider = chatHandlerProvider;
    this.authorizerMapper = authorizerMapper;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.appenderatorsManager = appenderatorsManager;
    this.missingIntervalsInOverwriteMode = !ingestionSchema.getIOConfig().isAppendToExisting()
                                           && !ingestionSchema.getDataSchema()
                                                              .getGranularitySpec()
                                                              .bucketIntervals()
                                                              .isPresent();
    if (missingIntervalsInOverwriteMode) {
      addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    }

    if (ingestionSchema.getTuningConfig().getMaxSavedParseExceptions()
        != TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS) {
      LOG.warn("maxSavedParseExceptions is not supported yet");
    }
    if (ingestionSchema.getTuningConfig().getMaxParseExceptions() != TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS) {
      LOG.warn("maxParseExceptions is not supported yet");
    }
    if (ingestionSchema.getTuningConfig().isLogParseExceptions() != TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS) {
      LOG.warn("logParseExceptions is not supported yet");
    }
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
  public ParallelIndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @VisibleForTesting
  @Nullable
  ParallelIndexTaskRunner getRunner()
  {
    return runner;
  }

  @VisibleForTesting
  AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }

  @VisibleForTesting
  ParallelIndexTaskRunner createRunner(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
    if (ingestionSchema.getTuningConfig().isForceGuaranteedRollup()) {
      throw new UnsupportedOperationException("Perfect roll-up is not supported yet");
    } else {
      runner = new SinglePhaseParallelIndexTaskRunner(
          toolbox,
          getId(),
          getGroupId(),
          ingestionSchema,
          getContext(),
          indexingServiceClient
      );
    }
    return runner;
  }

  @VisibleForTesting
  void setRunner(ParallelIndexTaskRunner runner)
  {
    this.runner = runner;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return determineLockGranularityAndTryLock(taskActionClient, ingestionSchema.getDataSchema().getGranularitySpec());
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return findInputSegments(
        getDataSource(),
        taskActionClient,
        intervals,
        ingestionSchema.getIOConfig().getFirehoseFactory()
    );
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return !ingestionSchema.getIOConfig().isAppendToExisting();
  }

  @Override
  public boolean isPerfectRollup()
  {
    return false;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    if (granularitySpec instanceof ArbitraryGranularitySpec) {
      return null;
    } else {
      return granularitySpec.getSegmentGranularity();
    }
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    if (missingIntervalsInOverwriteMode) {
      LOG.warn(
          "Intervals are missing in granularitySpec while this task is potentially overwriting existing segments. "
          + "Forced to use timeChunk lock."
      );
    }
    LOG.info(
        "Found chat handler of class[%s]",
        Preconditions.checkNotNull(chatHandlerProvider, "chatHandlerProvider").getClass().getName()
    );
    chatHandlerProvider.register(getId(), this, false);

    try {
      if (isParallelMode()) {
        return runParallel(toolbox);
      } else {
        if (!baseFirehoseFactory.isSplittable()) {
          LOG.warn(
              "firehoseFactory[%s] is not splittable. Running sequentially.",
              baseFirehoseFactory.getClass().getSimpleName()
          );
        } else if (ingestionSchema.getTuningConfig().getMaxNumSubTasks() == 1) {
          LOG.warn(
              "maxNumSubTasks is 1. Running sequentially. "
              + "Please set maxNumSubTasks to something higher than 1 if you want to run in parallel ingestion mode."
          );
        } else {
          throw new ISE("Unknown reason for sequentail mode. Failing this task.");
        }

        return runSequential(toolbox);
      }
    }
    finally {
      chatHandlerProvider.unregister(getId());
    }
  }

  private boolean isParallelMode()
  {
    if (baseFirehoseFactory.isSplittable() && ingestionSchema.getTuningConfig().getMaxNumSubTasks() > 1) {
      return true;
    } else {
      return false;
    }
  }

  @VisibleForTesting
  void setToolbox(TaskToolbox toolbox)
  {
    this.toolbox = toolbox;
  }

  private TaskStatus runParallel(TaskToolbox toolbox) throws Exception
  {
    createRunner(toolbox);
    registerResourceCloserOnAbnormalExit(config -> runner.stopGracefully());
    return TaskStatus.fromCode(getId(), Preconditions.checkNotNull(runner, "runner").run());
  }

  private TaskStatus runSequential(TaskToolbox toolbox) throws Exception
  {
    sequentialIndexTask = new IndexTask(
        getId(),
        getGroupId(),
        getTaskResource(),
        getDataSource(),
        new IndexIngestionSpec(
            getIngestionSchema().getDataSchema(),
            getIngestionSchema().getIOConfig(),
            convertToIndexTuningConfig(getIngestionSchema().getTuningConfig())
        ),
        getContext(),
        authorizerMapper,
        chatHandlerProvider,
        rowIngestionMetersFactory,
        appenderatorsManager
    );
    if (sequentialIndexTask.isReady(toolbox.getTaskActionClient())) {
      registerResourceCloserOnAbnormalExit(config -> sequentialIndexTask.stopGracefully(config));
      return sequentialIndexTask.run(toolbox);
    } else {
      return TaskStatus.failure(getId());
    }
  }

  private static IndexTuningConfig convertToIndexTuningConfig(ParallelIndexTuningConfig tuningConfig)
  {
    return new IndexTuningConfig(
        null,
        null,
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemory(),
        null,
        null,
        null,
        null,
        tuningConfig.getPartitionsSpec(),
        tuningConfig.getIndexSpec(),
        tuningConfig.getIndexSpecForIntermediatePersists(),
        tuningConfig.getMaxPendingPersists(),
        false,
        tuningConfig.isReportParseExceptions(),
        null,
        tuningConfig.getPushTimeout(),
        tuningConfig.getSegmentWriteOutMediumFactory(),
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );
  }

  // Internal APIs

  /**
   * Allocate a new {@link SegmentIdWithShardSpec} for a request from {@link ParallelIndexSubTask}.
   * The returned segmentIdentifiers have different {@code partitionNum} (thereby different {@link NumberedShardSpec})
   * per bucket interval.
   */
  @POST
  @Path("/segment/allocate")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  public Response allocateSegment(DateTime timestamp, @Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);

    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    }

    try {
      final SegmentIdWithShardSpec segmentIdentifier = allocateNewSegment(timestamp);
      return Response.ok(toolbox.getObjectMapper().writeValueAsBytes(segmentIdentifier)).build();
    }
    catch (IOException | IllegalStateException e) {
      return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST).entity(Throwables.getStackTraceAsString(e)).build();
    }
  }

  /**
   * Allocate a new segment for the given timestamp locally.
   * Since the segments returned by this method overwrites any existing segments, this method should be called only
   * when the {@link org.apache.druid.indexing.common.LockGranularity} is {@code TIME_CHUNK}.
   */
  @VisibleForTesting
  SegmentIdWithShardSpec allocateNewSegment(DateTime timestamp) throws IOException
  {
    final String dataSource = getDataSource();
    final GranularitySpec granularitySpec = getIngestionSchema().getDataSchema().getGranularitySpec();
    final Optional<SortedSet<Interval>> bucketIntervals = granularitySpec.bucketIntervals();

    // List locks whenever allocating a new segment because locks might be revoked and no longer valid.
    final List<TaskLock> locks = toolbox
        .getTaskActionClient()
        .submit(new LockListAction());
    final TaskLock revokedLock = locks.stream().filter(TaskLock::isRevoked).findAny().orElse(null);
    if (revokedLock != null) {
      throw new ISE("Lock revoked: [%s]", revokedLock);
    }
    final Map<Interval, String> versions = locks
        .stream()
        .collect(Collectors.toMap(TaskLock::getInterval, TaskLock::getVersion));

    Interval interval;
    String version;
    if (bucketIntervals.isPresent()) {
      // If granularity spec has explicit intervals, we just need to find the version associated to the interval.
      // This is because we should have gotten all required locks up front when the task starts up.
      final Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
      if (!maybeInterval.isPresent()) {
        throw new IAE("Could not find interval for timestamp [%s]", timestamp);
      }

      interval = maybeInterval.get();
      if (!bucketIntervals.get().contains(interval)) {
        throw new ISE("Unspecified interval[%s] in granularitySpec[%s]", interval, granularitySpec);
      }

      version = findVersion(versions, interval);
      if (version == null) {
        throw new ISE("Cannot find a version for interval[%s]", interval);
      }
    } else {
      // We don't have explicit intervals. We can use the segment granularity to figure out what
      // interval we need, but we might not have already locked it.
      interval = granularitySpec.getSegmentGranularity().bucket(timestamp);
      version = findVersion(versions, interval);
      if (version == null) {
        // We don't have a lock for this interval, so we should lock it now.
        final TaskLock lock = Preconditions.checkNotNull(
            toolbox.getTaskActionClient().submit(
                new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval)
            ),
            "Cannot acquire a lock for interval[%s]",
            interval
        );
        version = lock.getVersion();
      }
    }

    final int partitionNum = Counters.getAndIncrementInt(partitionNumCountersPerInterval, interval);
    return new SegmentIdWithShardSpec(
        dataSource,
        interval,
        version,
        new NumberedShardSpec(partitionNum, 0)
    );
  }

  @Nullable
  private static String findVersion(Map<Interval, String> versions, Interval interval)
  {
    return versions.entrySet().stream()
                   .filter(entry -> entry.getKey().contains(interval))
                   .map(Entry::getValue)
                   .findFirst()
                   .orElse(null);
  }

  /**
   * {@link ParallelIndexSubTask}s call this API to report the segments they've generated and pushed.
   */
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
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      runner.collectReport(report);
      return Response.ok().build();
    }
  }

  // External APIs to get running status

  @GET
  @Path("/mode")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getMode(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    return Response.ok(isParallelMode() ? "parallel" : "sequential").build();
  }

  @GET
  @Path("/progress")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getProgress()).build();
    }
  }

  @GET
  @Path("/subtasks/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getRunningTaskIds()).build();
    }
  }

  @GET
  @Path("/subtaskspecs")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getRunningSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/complete")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(runner.getCompleteSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspec/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpec(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);

    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final SubTaskSpec subTaskSpec = runner.getSubTaskSpec(id);
      if (subTaskSpec == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(subTaskSpec).build();
      }
    }
  }

  @GET
  @Path("/subtaskspec/{id}/state")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskState(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final SubTaskSpecStatus subTaskSpecStatus = runner.getSubTaskState(id);
      if (subTaskSpecStatus == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      } else {
        return Response.ok(subTaskSpecStatus).build();
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
    if (runner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final TaskHistory taskHistory = runner.getCompleteSubTaskSpecAttemptHistory(id);
      if (taskHistory == null) {
        return Response.status(Status.NOT_FOUND).build();
      } else {
        return Response.ok(taskHistory.getAttemptHistory()).build();
      }
    }
  }
}
