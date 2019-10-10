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
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.appenderator.ActionBasedUsedSegmentChecker;
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskRunner.SubTaskSpecStatus;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.appenderator.UsedSegmentChecker;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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

  /**
   * A holder for the current phase runner (parallel mode) or index task (sequential mode).
   * This variable is lazily initialized in {@link #initializeSubTaskCleaner}.
   * Volatile since HTTP API calls can read this variable at any time while this task is running.
   */
  @MonotonicNonNull
  private volatile CurrentSubTaskHolder currentSubTaskHolder;

  /**
   * A variable to keep the given toolbox. This variable is lazily initialized in {@link #runTask}.
   * Volatile since HTTP API calls can use this variable at any time while this task is running.
   */
  @MonotonicNonNull
  private volatile TaskToolbox toolbox;

  @JsonCreator
  public ParallelIndexSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") @Nullable String groupId,
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
        groupId,
        taskResource,
        ingestionSchema.getDataSchema().getDataSource(),
        context
    );

    this.ingestionSchema = ingestionSchema;

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    if (!(firehoseFactory instanceof FiniteFirehoseFactory)) {
      throw new IAE("[%s] should implement FiniteFirehoseFactory", firehoseFactory.getClass().getSimpleName());
    }

    if (ingestionSchema.getTuningConfig().isForceGuaranteedRollup()) {
      if (ingestionSchema.getTuningConfig().getNumShards() == null) {
        throw new ISE("forceGuaranteedRollup is set but numShards is missing in partitionsSpec");
      }

      if (ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals().isEmpty()) {
        throw new ISE("forceGuaranteedRollup is set but intervals is missing in granularitySpec");
      }
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
  ParallelIndexTaskRunner getCurrentRunner()
  {
    if (isParallelMode()) {
      return currentSubTaskHolder == null ? null : currentSubTaskHolder.getTask();
    } else {
      return null;
    }
  }

  @VisibleForTesting
  IndexingServiceClient getIndexingServiceClient()
  {
    return indexingServiceClient;
  }

  @Nullable
  private <T extends Task, R extends SubTaskReport> ParallelIndexTaskRunner<T, R> createRunner(
      TaskToolbox toolbox,
      Function<TaskToolbox, ParallelIndexTaskRunner<T, R>> runnerCreator
  )
  {
    final ParallelIndexTaskRunner<T, R> newRunner = runnerCreator.apply(toolbox);
    if (currentSubTaskHolder.setTask(newRunner)) {
      return newRunner;
    } else {
      return null;
    }
  }

  private TaskState runNextPhase(@Nullable ParallelIndexTaskRunner nextPhaseRunner) throws Exception
  {
    if (nextPhaseRunner == null) {
      LOG.info("Task is asked to stop. Finish as failed");
      return TaskState.FAILED;
    } else {
      return nextPhaseRunner.run();
    }
  }

  @VisibleForTesting
  SinglePhaseParallelIndexTaskRunner createSinglePhaseTaskRunner(TaskToolbox toolbox)
  {
    return new SinglePhaseParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        ingestionSchema,
        getContext(),
        indexingServiceClient
    );
  }

  @VisibleForTesting
  public PartialSegmentGenerateParallelIndexTaskRunner createPartialSegmentGenerateRunner(TaskToolbox toolbox)
  {
    return new PartialSegmentGenerateParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        ingestionSchema,
        getContext(),
        indexingServiceClient
    );
  }

  @VisibleForTesting
  public PartialSegmentMergeParallelIndexTaskRunner createPartialSegmentMergeRunner(
      TaskToolbox toolbox,
      List<PartialSegmentMergeIOConfig> ioConfigs
  )
  {
    return new PartialSegmentMergeParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        getIngestionSchema().getDataSchema(),
        ioConfigs,
        getIngestionSchema().getTuningConfig(),
        getContext(),
        indexingServiceClient
    );
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
    return isGuaranteedRollup(getIngestionSchema().getIOConfig(), getIngestionSchema().getTuningConfig());
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
      initializeSubTaskCleaner();

      if (isParallelMode()) {
        this.toolbox = toolbox;
        if (getIngestionSchema().getTuningConfig().isForceGuaranteedRollup()) {
          return runMultiPhaseParallel(toolbox);
        } else {
          return runSinglePhaseParallel(toolbox);
        }
      } else {
        if (!baseFirehoseFactory.isSplittable()) {
          LOG.warn(
              "firehoseFactory[%s] is not splittable. Running sequentially.",
              baseFirehoseFactory.getClass().getSimpleName()
          );
        } else if (ingestionSchema.getTuningConfig().getMaxNumConcurrentSubTasks() <= 1) {
          LOG.warn(
              "maxNumConcurrentSubTasks[%s] is less than or equal to 1. Running sequentially. "
              + "Please set maxNumConcurrentSubTasks to something higher than 1 if you want to run in parallel "
              + "ingestion mode.",
              ingestionSchema.getTuningConfig().getMaxNumConcurrentSubTasks()
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

  private void initializeSubTaskCleaner()
  {
    if (isParallelMode()) {
      currentSubTaskHolder = new CurrentSubTaskHolder((currentRunnerObject, taskConfig) -> {
        final ParallelIndexTaskRunner runner = (ParallelIndexTaskRunner) currentRunnerObject;
        runner.stopGracefully();
      });
    } else {
      currentSubTaskHolder = new CurrentSubTaskHolder((taskObject, taskConfig) -> {
        final IndexTask task = (IndexTask) taskObject;
        task.stopGracefully(taskConfig);
      });
    }
    registerResourceCloserOnAbnormalExit(currentSubTaskHolder);
  }

  private boolean isParallelMode()
  {
    return baseFirehoseFactory.isSplittable() && ingestionSchema.getTuningConfig().getMaxNumConcurrentSubTasks() > 1;
  }

  /**
   * Run the single phase parallel indexing for best-effort rollup. In this mode, each sub task created by
   * the supervisor task reads data and generates segments individually.
   */
  private TaskStatus runSinglePhaseParallel(TaskToolbox toolbox) throws Exception
  {
    final ParallelIndexTaskRunner<SinglePhaseSubTask, PushedSegmentsReport> runner = createRunner(
        toolbox,
        this::createSinglePhaseTaskRunner
    );

    final TaskState state = runNextPhase(runner);
    if (state.isSuccess()) {
      //noinspection ConstantConditions
      publishSegments(toolbox, runner.getReports());
    }
    return TaskStatus.fromCode(getId(), state);
  }

  /**
   * Run the multi phase parallel indexing for perfect rollup. In this mode, the parallel indexing is currently
   * executed in two phases.
   *
   * - In the first phase, each task partitions input data and stores those partitions in local storage.
   *   - The partition is created based on the segment granularity (primary partition key) and the partition dimension
   *     values in {@link org.apache.druid.indexer.partitions.PartitionsSpec} (secondary partition key).
   *   - Partitioned data is maintained by {@link org.apache.druid.indexing.worker.IntermediaryDataManager}.
   * - In the second phase, each task reads partitioned data from the intermediary data server (middleManager
   *   or indexer) and merges them to create the final segments.
   */
  private TaskStatus runMultiPhaseParallel(TaskToolbox toolbox) throws Exception
  {
    // 1. Partial segment generation phase
    final ParallelIndexTaskRunner<PartialSegmentGenerateTask, GeneratedPartitionsReport> indexingRunner = createRunner(
        toolbox,
        this::createPartialSegmentGenerateRunner
    );

    TaskState state = runNextPhase(indexingRunner);
    if (state.isFailure()) {
      return TaskStatus.failure(getId());
    }

    // 2. Partial segment merge phase

    // partition (interval, partitionId) -> partition locations
    //noinspection ConstantConditions
    Map<Pair<Interval, Integer>, List<PartitionLocation>> partitionToLocations = groupPartitionLocationsPerPartition(
        indexingRunner.getReports()
    );
    final List<PartialSegmentMergeIOConfig> ioConfigs = createMergeIOConfigs(
        ingestionSchema.getTuningConfig().getTotalNumMergeTasks(),
        partitionToLocations
    );

    final ParallelIndexTaskRunner<PartialSegmentMergeTask, PushedSegmentsReport> mergeRunner = createRunner(
        toolbox,
        tb -> createPartialSegmentMergeRunner(tb, ioConfigs)
    );
    state = runNextPhase(mergeRunner);
    if (state.isSuccess()) {
      //noinspection ConstantConditions
      publishSegments(toolbox, mergeRunner.getReports());
    }

    return TaskStatus.fromCode(getId(), state);
  }

  private static Map<Pair<Interval, Integer>, List<PartitionLocation>> groupPartitionLocationsPerPartition(
      // subTaskId -> report
      Map<String, GeneratedPartitionsReport> reports
  )
  {
    // partition (interval, partitionId) -> partition locations
    final Map<Pair<Interval, Integer>, List<PartitionLocation>> partitionToLocations = new HashMap<>();
    for (Entry<String, GeneratedPartitionsReport> entry : reports.entrySet()) {
      final String subTaskId = entry.getKey();
      final GeneratedPartitionsReport report = entry.getValue();
      for (PartitionStat partitionStat : report.getPartitionStats()) {
        final List<PartitionLocation> locationsOfSamePartition = partitionToLocations.computeIfAbsent(
            Pair.of(partitionStat.getInterval(), partitionStat.getPartitionId()),
            k -> new ArrayList<>()
        );
        locationsOfSamePartition.add(
            new PartitionLocation(
                partitionStat.getTaskExecutorHost(),
                partitionStat.getTaskExecutorPort(),
                partitionStat.isUseHttps(),
                subTaskId,
                partitionStat.getInterval(),
                partitionStat.getPartitionId()
            )
        );
      }
    }

    return partitionToLocations;
  }

  private static List<PartialSegmentMergeIOConfig> createMergeIOConfigs(
      int totalNumMergeTasks,
      Map<Pair<Interval, Integer>, List<PartitionLocation>> partitionToLocations
  )
  {
    final int numMergeTasks = Math.min(totalNumMergeTasks, partitionToLocations.size());
    LOG.info(
        "Number of merge tasks is set to [%d] based on totalNumMergeTasks[%d] and number of partitions[%d]",
        numMergeTasks,
        totalNumMergeTasks,
        partitionToLocations.size()
    );
    // Randomly shuffle partitionIds to evenly distribute partitions of potentially different sizes
    // This will be improved once we collect partition stats properly.
    // See PartitionStat in GeneratedPartitionsReport.
    final List<Pair<Interval, Integer>> partitions = new ArrayList<>(partitionToLocations.keySet());
    Collections.shuffle(partitions, ThreadLocalRandom.current());
    final int numPartitionsPerTask = (int) Math.round(partitions.size() / (double) numMergeTasks);

    final List<PartialSegmentMergeIOConfig> assignedPartitionLocations = new ArrayList<>(numMergeTasks);
    for (int i = 0; i < numMergeTasks - 1; i++) {
      final List<PartitionLocation> assignedToSameTask = partitions
          .subList(i * numPartitionsPerTask, (i + 1) * numPartitionsPerTask)
          .stream()
          .flatMap(intervalAndPartitionId -> partitionToLocations.get(intervalAndPartitionId).stream())
          .collect(Collectors.toList());
      assignedPartitionLocations.add(new PartialSegmentMergeIOConfig(assignedToSameTask));
    }

    // The last task is assigned all remaining partitions.
    final List<PartitionLocation> assignedToSameTask = partitions
        .subList((numMergeTasks - 1) * numPartitionsPerTask, partitions.size())
        .stream()
        .flatMap(intervalAndPartitionId -> partitionToLocations.get(intervalAndPartitionId).stream())
        .collect(Collectors.toList());
    assignedPartitionLocations.add(new PartialSegmentMergeIOConfig(assignedToSameTask));

    return assignedPartitionLocations;
  }

  private static void publishSegments(TaskToolbox toolbox, Map<String, PushedSegmentsReport> reportsMap)
      throws IOException
  {
    final UsedSegmentChecker usedSegmentChecker = new ActionBasedUsedSegmentChecker(toolbox.getTaskActionClient());
    final Set<DataSegment> oldSegments = new HashSet<>();
    final Set<DataSegment> newSegments = new HashSet<>();
    reportsMap
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
      LOG.info("Published [%d] segments", newSegments.size());
    } else {
      LOG.info("Transaction failure while publishing segments, checking if someone else beat us to it.");
      final Set<SegmentIdWithShardSpec> segmentsIdentifiers = reportsMap
          .values()
          .stream()
          .flatMap(report -> report.getNewSegments().stream())
          .map(SegmentIdWithShardSpec::fromDataSegment)
          .collect(Collectors.toSet());
      if (usedSegmentChecker.findUsedSegments(segmentsIdentifiers)
                            .equals(newSegments)) {
        LOG.info("Our segments really do exist, awaiting handoff.");
      } else {
        throw new ISE("Failed to publish segments[%s]", newSegments);
      }
    }
  }

  private TaskStatus runSequential(TaskToolbox toolbox) throws Exception
  {
    final IndexTask indexTask = new IndexTask(
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

    if (currentSubTaskHolder.setTask(indexTask) && indexTask.isReady(toolbox.getTaskActionClient())) {
      return indexTask.run(toolbox);
    } else {
      LOG.info("Task is asked to stop. Finish as failed");
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
        tuningConfig.isForceGuaranteedRollup(),
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
   * Allocate a new {@link SegmentIdWithShardSpec} for a request from {@link SinglePhaseSubTask}.
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
   * {@link SinglePhaseSubTask}s call this API to report the segments they generated and pushed.
   */
  @POST
  @Path("/report")
  @Consumes(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  public Response report(
      SubTaskReport report,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(
        req,
        Action.WRITE,
        getDataSource(),
        authorizerMapper
    );
    if (currentSubTaskHolder == null || currentSubTaskHolder.getTask() == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final ParallelIndexTaskRunner runner = currentSubTaskHolder.getTask();
      //noinspection unchecked
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
  @Path("/phase")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPhaseName(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    if (isParallelMode()) {
      final ParallelIndexTaskRunner runner = getCurrentRunner();
      if (runner == null) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running").build();
      } else {
        return Response.ok(runner.getName()).build();
      }
    } else {
      return Response.status(Status.BAD_REQUEST).entity("task is running in the sequential mode").build();
    }
  }

  @GET
  @Path("/progress")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getProgress(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(currentRunner.getProgress()).build();
    }
  }

  @GET
  @Path("/subtasks/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(currentRunner.getRunningTaskIds()).build();
    }
  }

  @GET
  @Path("/subtaskspecs")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(currentRunner.getSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/running")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(currentRunner.getRunningSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspecs/complete")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteSubTaskSpecs(@Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      return Response.ok(currentRunner.getCompleteSubTaskSpecs()).build();
    }
  }

  @GET
  @Path("/subtaskspec/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubTaskSpec(@PathParam("id") String id, @Context final HttpServletRequest req)
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);

    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final SubTaskSpec subTaskSpec = currentRunner.getSubTaskSpec(id);
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
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final SubTaskSpecStatus subTaskSpecStatus = currentRunner.getSubTaskState(id);
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
    final ParallelIndexTaskRunner currentRunner = getCurrentRunner();
    if (currentRunner == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    } else {
      final TaskHistory taskHistory = currentRunner.getCompleteSubTaskSpecAttemptHistory(id);
      if (taskHistory == null) {
        return Response.status(Status.NOT_FOUND).build();
      } else {
        return Response.ok(taskHistory.getAttemptHistory()).build();
      }
    }
  }
}
