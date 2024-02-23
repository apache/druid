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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.IngestionState;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReport;
import org.apache.druid.indexing.common.IngestionStatsAndErrorsTaskReportData;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.CurrentSubTaskHolder;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.MaxAllowedLocksExceededException;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTaskRunner.SubTaskSpecStatus;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.RowIngestionMetersTotals;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.apache.druid.utils.CollectionUtils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ParallelIndexSupervisorTask is capable of running multiple subTasks for parallel indexing. This is
 * applicable if the input {@link InputSource} is splittable. While this task is running, it can submit
 * multiple child tasks to overlords. This task succeeds only when all its child tasks succeed; otherwise it fails.
 *
 * @see ParallelIndexTaskRunner
 */
public class ParallelIndexSupervisorTask extends AbstractBatchIndexTask implements ChatHandler
{
  public static final String TYPE = "index_parallel";

  private static final Logger LOG = new Logger(ParallelIndexSupervisorTask.class);

  private static final String TASK_PHASE_FAILURE_MSG = "Failed in phase[%s]. See task logs for details.";

  // Sometimes Druid estimates one shard for hash partitioning despite conditions
  // indicating that there ought to be more than one. We have not been able to
  // reproduce but looking at the code around where the following constant is used one
  // possibility is that the sketch's estimate is negative. If that case happens
  // code has been added to log it and to set the estimate to the value of the
  // following constant. It is not necessary to parameterize this value since if this
  // happens it is a bug and the new logging may now provide some evidence to reproduce
  // and fix
  private static final long DEFAULT_NUM_SHARDS_WHEN_ESTIMATE_GOES_NEGATIVE = 7L;

  private final ParallelIndexIngestionSpec ingestionSchema;
  /**
   * Base name for the {@link SubTaskSpec} ID.
   * It is usually null for most task types and {@link #getId()} is used as the base name.
   * Only the compaction task can have a special base name.
   */
  private final String baseSubtaskSpecName;

  private final InputSource baseInputSource;

  /**
   * If intervals are missing in the granularitySpec, parallel index task runs in "dynamic locking mode".
   * In this mode, sub tasks ask new locks whenever they see a new row which is not covered by existing locks.
   * If this task is overwriting existing segments, then we should know this task is changing segment granularity
   * in advance to know what types of lock we should use. However, if intervals are missing, we can't know
   * the segment granularity of existing segments until the task reads all data because we don't know what segments
   * are going to be overwritten. As a result, we assume that segment granularity is going to be changed if intervals
   * are missing and force to use timeChunk lock.
   * <p>
   * This variable is initialized in the constructor and used in {@link #run} to log that timeChunk lock was enforced
   * in the task logs.
   */
  private final boolean missingIntervalsInOverwriteMode;

  private final long awaitSegmentAvailabilityTimeoutMillis;

  @MonotonicNonNull
  private AuthorizerMapper authorizerMapper;

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

  /**
   * Row stats for index generate phase of parallel indexing. This variable is
   * lazily initialized in {@link #runTask}.
   * Volatile since HTTP API calls can use this variable while this task is running.
   */
  @MonotonicNonNull
  private volatile Pair<Map<String, Object>, Map<String, Object>> indexGenerateRowStats;

  private IngestionState ingestionState;


  @JsonCreator
  public ParallelIndexSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") @Nullable String groupId,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this(id, groupId, taskResource, ingestionSchema, null, context);
  }

  public ParallelIndexSupervisorTask(
      String id,
      @Nullable String groupId,
      TaskResource taskResource,
      ParallelIndexIngestionSpec ingestionSchema,
      @Nullable String baseSubtaskSpecName,
      Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, TYPE, ingestionSchema.getDataSchema().getDataSource()),
        groupId,
        taskResource,
        ingestionSchema.getDataSchema().getDataSource(),
        context,
        ingestionSchema.getTuningConfig().getMaxAllowedLockCount(),
        computeBatchIngestionMode(ingestionSchema.getIOConfig())
    );

    this.ingestionSchema = ingestionSchema;
    this.baseSubtaskSpecName = baseSubtaskSpecName == null ? getId() : baseSubtaskSpecName;
    if (getIngestionMode() == IngestionMode.REPLACE &&
        ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals().isEmpty()) {
      throw new ISE("GranularitySpec's intervals cannot be empty when using replace.");
    }

    if (isGuaranteedRollup(getIngestionMode(), ingestionSchema.getTuningConfig())) {
      checkPartitionsSpecForForceGuaranteedRollup(ingestionSchema.getTuningConfig().getGivenOrDefaultPartitionsSpec());
    }

    this.baseInputSource = ingestionSchema.getIOConfig().getNonNullInputSource();
    this.missingIntervalsInOverwriteMode = (getIngestionMode()
                                            != IngestionMode.APPEND)
                                           && ingestionSchema.getDataSchema()
                                                             .getGranularitySpec()
                                                             .inputIntervals()
                                                             .isEmpty();
    if (missingIntervalsInOverwriteMode) {
      addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    }

    awaitSegmentAvailabilityTimeoutMillis = ingestionSchema.getTuningConfig().getAwaitSegmentAvailabilityTimeoutMillis();
    this.ingestionState = IngestionState.NOT_STARTED;
  }

  private static void checkPartitionsSpecForForceGuaranteedRollup(PartitionsSpec partitionsSpec)
  {
    if (!partitionsSpec.isForceGuaranteedRollupCompatible()) {
      String incompatibiltyMsg = partitionsSpec.getForceGuaranteedRollupIncompatiblityReason();
      String msg = "forceGuaranteedRollup is incompatible with partitionsSpec: " + incompatibiltyMsg;
      throw new ISE(msg);
    }
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Nonnull
  @JsonIgnore
  @Override
  public Set<ResourceAction> getInputSourceResources()
  {
    if (getIngestionSchema().getIOConfig().getFirehoseFactory() != null) {
      throw getInputSecurityOnFirehoseUnsupportedError();
    }
    return getIngestionSchema().getIOConfig().getInputSource() != null ?
           getIngestionSchema().getIOConfig().getInputSource().getTypes()
                               .stream()
                               .map(i -> new ResourceAction(new Resource(i, ResourceType.EXTERNAL), Action.READ))
                               .collect(Collectors.toSet()) :
           ImmutableSet.of();
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

  @Nullable
  @VisibleForTesting
  <T extends Task, R extends SubTaskReport> ParallelIndexTaskRunner<T, R> createRunner(
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

  private static TaskState runNextPhase(@Nullable ParallelIndexTaskRunner nextPhaseRunner) throws Exception
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
        baseSubtaskSpecName,
        ingestionSchema,
        getContext()
    );
  }

  @VisibleForTesting
  PartialDimensionCardinalityParallelIndexTaskRunner createPartialDimensionCardinalityRunner(TaskToolbox toolbox)
  {
    return new PartialDimensionCardinalityParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        baseSubtaskSpecName,
        ingestionSchema,
        getContext()
    );
  }

  @VisibleForTesting
  PartialHashSegmentGenerateParallelIndexTaskRunner createPartialHashSegmentGenerateRunner(
      TaskToolbox toolbox,
      ParallelIndexIngestionSpec ingestionSchema,
      @Nullable Map<Interval, Integer> intervalToNumShardsOverride
  )
  {
    return new PartialHashSegmentGenerateParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        baseSubtaskSpecName,
        ingestionSchema,
        getContext(),
        intervalToNumShardsOverride
    );
  }

  @VisibleForTesting
  PartialDimensionDistributionParallelIndexTaskRunner createPartialDimensionDistributionRunner(TaskToolbox toolbox)
  {
    return new PartialDimensionDistributionParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        baseSubtaskSpecName,
        ingestionSchema,
        getContext()
    );
  }

  @VisibleForTesting
  PartialRangeSegmentGenerateParallelIndexTaskRunner createPartialRangeSegmentGenerateRunner(
      TaskToolbox toolbox,
      Map<Interval, PartitionBoundaries> intervalToPartitions,
      ParallelIndexIngestionSpec ingestionSchema
  )
  {
    return new PartialRangeSegmentGenerateParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        baseSubtaskSpecName,
        ingestionSchema,
        getContext(),
        intervalToPartitions
    );
  }

  @VisibleForTesting
  PartialGenericSegmentMergeParallelIndexTaskRunner createPartialGenericSegmentMergeRunner(
      TaskToolbox toolbox,
      List<PartialSegmentMergeIOConfig> ioConfigs,
      ParallelIndexIngestionSpec ingestionSchema
  )
  {
    return new PartialGenericSegmentMergeParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
        baseSubtaskSpecName,
        ingestionSchema.getDataSchema(),
        ioConfigs,
        ingestionSchema.getTuningConfig(),
        getContext()
    );
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return determineLockGranularityAndTryLock(
        taskActionClient,
        ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals()
    );
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return findInputSegments(
        getDataSource(),
        taskActionClient,
        intervals
    );
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return getIngestionMode() != IngestionMode.APPEND;
  }

  @Override
  public boolean isPerfectRollup()
  {
    return isGuaranteedRollup(
        getIngestionMode(),
        getIngestionSchema().getTuningConfig()
    );
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
    LOG.debug(
        "Found chat handler of class[%s]",
        Preconditions.checkNotNull(toolbox.getChatHandlerProvider(), "chatHandlerProvider").getClass().getName()
    );
    authorizerMapper = toolbox.getAuthorizerMapper();
    toolbox.getChatHandlerProvider().register(getId(), this, false);

    // the lineage-based segment allocation protocol must be used as the legacy protocol has a critical bug
    // (see SinglePhaseParallelIndexTaskRunner.allocateNewSegment()). However, we tell subtasks to use
    // the legacy protocol by default if it's not explicitly set in the taskContext here. This is to guarantee that
    // every subtask uses the same protocol so that they can succeed during the replacing rolling upgrade.
    // Once the Overlord is upgraded, it will set this context explicitly and new tasks will use the new protocol.
    // See DefaultTaskConfig and TaskQueue.add().
    addToContextIfAbsent(
        SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
        SinglePhaseParallelIndexTaskRunner.LEGACY_DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION
    );

    try {

      initializeSubTaskCleaner();

      if (isParallelMode()) {
        // emit metric for parallel batch ingestion mode:
        emitMetric(toolbox.getEmitter(), "ingest/count", 1);

        this.toolbox = toolbox;

        if (isGuaranteedRollup(
            getIngestionMode(),
            ingestionSchema.getTuningConfig()
        )) {
          return runMultiPhaseParallel(toolbox);
        } else {
          return runSinglePhaseParallel(toolbox);
        }
      } else {
        if (!baseInputSource.isSplittable()) {
          LOG.warn(
              "firehoseFactory[%s] is not splittable. Running sequentially.",
              baseInputSource.getClass().getSimpleName()
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
      ingestionState = IngestionState.COMPLETED;
      toolbox.getChatHandlerProvider().unregister(getId());
    }
  }

  private void initializeSubTaskCleaner()
  {
    if (isParallelMode()) {
      currentSubTaskHolder = new CurrentSubTaskHolder((currentRunnerObject, taskConfig) -> {
        final ParallelIndexTaskRunner runner = (ParallelIndexTaskRunner) currentRunnerObject;
        runner.stopGracefully(null);
      });
    } else {
      currentSubTaskHolder = new CurrentSubTaskHolder((taskObject, taskConfig) -> {
        final IndexTask task = (IndexTask) taskObject;
        task.stopGracefully(taskConfig);
      });
    }
    registerResourceCloserOnAbnormalExit(currentSubTaskHolder);
  }

  /**
   * Returns true if this task can run in the parallel mode with the given inputSource and tuningConfig.
   * This method should be synchronized with CompactSegments.isParallelMode(ClientCompactionTaskQueryTuningConfig).
   */
  public static boolean isParallelMode(InputSource inputSource, @Nullable ParallelIndexTuningConfig tuningConfig)
  {
    if (null == tuningConfig) {
      return false;
    }
    boolean useRangePartitions = useRangePartitions(tuningConfig);
    // Range partitioning is not implemented for runSequential() (but hash partitioning is)
    int minRequiredNumConcurrentSubTasks = useRangePartitions ? 1 : 2;
    return inputSource.isSplittable() && tuningConfig.getMaxNumConcurrentSubTasks() >= minRequiredNumConcurrentSubTasks;
  }

  private static boolean useRangePartitions(ParallelIndexTuningConfig tuningConfig)
  {
    return tuningConfig.getGivenOrDefaultPartitionsSpec() instanceof DimensionRangePartitionsSpec;
  }

  private boolean isParallelMode()
  {
    return isParallelMode(baseInputSource, ingestionSchema.getTuningConfig());
  }

  /**
   * Attempt to wait for indexed segments to become available on the cluster.
   * @param reportsMap Map containing information with published segments that we are going to wait for.
   */
  private void waitForSegmentAvailability(Map<String, PushedSegmentsReport> reportsMap)
  {
    ArrayList<DataSegment> segmentsToWaitFor = new ArrayList<>();
    reportsMap.values()
              .forEach(report -> {
                segmentsToWaitFor.addAll(report.getNewSegments());
              });
    waitForSegmentAvailability(
        toolbox,
        segmentsToWaitFor,
        awaitSegmentAvailabilityTimeoutMillis
    );
  }

  /**
   * Run the single phase parallel indexing for best-effort rollup. In this mode, each sub task created by
   * the supervisor task reads data and generates segments individually.
   */
  private TaskStatus runSinglePhaseParallel(TaskToolbox toolbox) throws Exception
  {
    ingestionState = IngestionState.BUILD_SEGMENTS;
    ParallelIndexTaskRunner<SinglePhaseSubTask, PushedSegmentsReport> parallelSinglePhaseRunner = createRunner(
        toolbox,
        this::createSinglePhaseTaskRunner
    );

    final TaskState state = runNextPhase(parallelSinglePhaseRunner);
    TaskStatus taskStatus;
    if (state.isSuccess()) {
      //noinspection ConstantConditions
      publishSegments(toolbox, parallelSinglePhaseRunner.getReports());
      if (awaitSegmentAvailabilityTimeoutMillis > 0) {
        waitForSegmentAvailability(parallelSinglePhaseRunner.getReports());
      }
      taskStatus = TaskStatus.success(getId());
    } else {
      // there is only success or failure after running....
      Preconditions.checkState(state.isFailure(), "Unrecognized state after task is complete[%s]", state);
      final String errorMessage;
      if (parallelSinglePhaseRunner.getStopReason() != null) {
        errorMessage = parallelSinglePhaseRunner.getStopReason();
      } else {
        errorMessage = StringUtils.format(
            TASK_PHASE_FAILURE_MSG,
            parallelSinglePhaseRunner.getName()
        );
      }
      taskStatus = TaskStatus.failure(getId(), errorMessage);
    }
    toolbox.getTaskReportFileWriter().write(
        getId(),
        getTaskCompletionReports(taskStatus, segmentAvailabilityConfirmationCompleted)
    );
    return taskStatus;
  }

  /**
   * Run the multi phase parallel indexing for perfect rollup. In this mode, the parallel indexing is currently
   * executed in two phases.
   * <p>
   * - In the first phase, each task partitions input data and stores those partitions in local storage.
   * - The partition is created based on the segment granularity (primary partition key) and the partition dimension
   * values in {@link PartitionsSpec} (secondary partition key).
   * - Partitioned data is maintained by {@link IntermediaryDataManager}.
   * - In the second phase, each task reads partitioned data from the intermediary data server (middleManager
   * or indexer) and merges them to create the final segments.
   */
  private TaskStatus runMultiPhaseParallel(TaskToolbox toolbox) throws Exception
  {
    return useRangePartitions(ingestionSchema.getTuningConfig())
           ? runRangePartitionMultiPhaseParallel(toolbox)
           : runHashPartitionMultiPhaseParallel(toolbox);
  }

  private static ParallelIndexIngestionSpec rewriteIngestionSpecWithIntervalsIfMissing(
      ParallelIndexIngestionSpec ingestionSchema,
      Collection<Interval> intervals
  )
  {
    if (ingestionSchema.getDataSchema().getGranularitySpec().inputIntervals().isEmpty()) {
      return ingestionSchema
          .withDataSchema(
              ingestionSchema.getDataSchema().withGranularitySpec(
                  ingestionSchema
                      .getDataSchema()
                      .getGranularitySpec()
                      .withIntervals(new ArrayList<>(intervals))
              )
          );
    } else {
      return ingestionSchema;
    }
  }

  @VisibleForTesting
  TaskStatus runHashPartitionMultiPhaseParallel(TaskToolbox toolbox) throws Exception
  {
    TaskState state;
    ParallelIndexIngestionSpec ingestionSchemaToUse = ingestionSchema;

    if (!(ingestionSchema.getTuningConfig().getPartitionsSpec() instanceof HashedPartitionsSpec)) {
      // only range and hash partitioning is supported for multiphase parallel ingestion, see runMultiPhaseParallel()
      throw new ISE(
          "forceGuaranteedRollup is set but partitionsSpec [%s] is not a single_dim or hash partition spec.",
          ingestionSchema.getTuningConfig().getPartitionsSpec()
      );
    }

    final Map<Interval, Integer> intervalToNumShards;
    HashedPartitionsSpec partitionsSpec = (HashedPartitionsSpec) ingestionSchema.getTuningConfig().getPartitionsSpec();
    final boolean needsInputSampling =
        partitionsSpec.getNumShards() == null
        || ingestionSchemaToUse.getDataSchema().getGranularitySpec().inputIntervals().isEmpty();
    if (needsInputSampling) {
      // 0. need to determine intervals and numShards by scanning the data
      LOG.info("Needs to determine intervals or numShards, beginning %s phase.", PartialDimensionCardinalityTask.TYPE);
      ParallelIndexTaskRunner<PartialDimensionCardinalityTask, DimensionCardinalityReport> cardinalityRunner =
          createRunner(
              toolbox,
              this::createPartialDimensionCardinalityRunner
          );

      state = runNextPhase(cardinalityRunner);
      if (state.isFailure()) {
        String errMsg = StringUtils.format(
            TASK_PHASE_FAILURE_MSG,
            cardinalityRunner.getName()
        );
        return TaskStatus.failure(getId(), errMsg);
      }

      if (cardinalityRunner.getReports().isEmpty()) {
        String msg = "No valid rows for hash partitioning."
                     + " All rows may have invalid timestamps or have been filtered out.";
        LOG.warn(msg);
        return TaskStatus.success(getId(), msg);
      }

      if (partitionsSpec.getNumShards() == null) {
        int effectiveMaxRowsPerSegment = partitionsSpec.getMaxRowsPerSegment() == null
                                         ? PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT
                                         : partitionsSpec.getMaxRowsPerSegment();
        LOG.info("effective maxRowsPerSegment is: " + effectiveMaxRowsPerSegment);

        intervalToNumShards = determineNumShardsFromCardinalityReport(
            cardinalityRunner.getReports().values(),
            effectiveMaxRowsPerSegment
        );

        // This is for potential debugging in case we suspect bad estimation of cardinalities etc,
        LOG.debug("intervalToNumShards: %s", intervalToNumShards.toString());

      } else {
        intervalToNumShards = CollectionUtils.mapValues(
            mergeCardinalityReports(cardinalityRunner.getReports().values()),
            k -> partitionsSpec.getNumShards()
        );
      }

      ingestionSchemaToUse = rewriteIngestionSpecWithIntervalsIfMissing(
          ingestionSchemaToUse,
          intervalToNumShards.keySet()
      );
    } else {
      // numShards will be determined in PartialHashSegmentGenerateTask
      intervalToNumShards = null;
    }

    // 1. Partial segment generation phase
    final ParallelIndexIngestionSpec segmentCreateIngestionSpec = ingestionSchemaToUse;
    ParallelIndexTaskRunner<PartialHashSegmentGenerateTask, GeneratedPartitionsReport> indexingRunner =
        createRunner(
            toolbox,
            f -> createPartialHashSegmentGenerateRunner(toolbox, segmentCreateIngestionSpec, intervalToNumShards)
        );

    state = runNextPhase(indexingRunner);
    if (state.isFailure()) {
      String errMsg = StringUtils.format(
          TASK_PHASE_FAILURE_MSG,
          indexingRunner.getName()
      );
      return TaskStatus.failure(getId(), errMsg);
    }
    indexGenerateRowStats = doGetRowStatsAndUnparseableEventsParallelMultiPhase(indexingRunner, true);

    // 2. Partial segment merge phase
    // partition (interval, partitionId) -> partition locations
    Map<Partition, List<PartitionLocation>> partitionToLocations =
        getPartitionToLocations(indexingRunner.getReports());
    final List<PartialSegmentMergeIOConfig> ioConfigs = createGenericMergeIOConfigs(
        ingestionSchema.getTuningConfig().getTotalNumMergeTasks(),
        partitionToLocations
    );

    final ParallelIndexIngestionSpec segmentMergeIngestionSpec = ingestionSchemaToUse;
    final ParallelIndexTaskRunner<PartialGenericSegmentMergeTask, PushedSegmentsReport> mergeRunner = createRunner(
        toolbox,
        tb -> createPartialGenericSegmentMergeRunner(tb, ioConfigs, segmentMergeIngestionSpec)
    );
    state = runNextPhase(mergeRunner);
    TaskStatus taskStatus;
    if (state.isSuccess()) {
      //noinspection ConstantConditions
      publishSegments(toolbox, mergeRunner.getReports());
      if (awaitSegmentAvailabilityTimeoutMillis > 0) {
        waitForSegmentAvailability(mergeRunner.getReports());
      }
      taskStatus = TaskStatus.success(getId());
    } else {
      // there is only success or failure after running....
      Preconditions.checkState(state.isFailure(), "Unrecognized state after task is complete[%s]", state);
      String errMsg = StringUtils.format(
          TASK_PHASE_FAILURE_MSG,
          mergeRunner.getName()
      );
      taskStatus = TaskStatus.failure(getId(), errMsg);
    }

    toolbox.getTaskReportFileWriter().write(
        getId(),
        getTaskCompletionReports(taskStatus, segmentAvailabilityConfirmationCompleted)
    );
    return taskStatus;
  }

  @VisibleForTesting
  TaskStatus runRangePartitionMultiPhaseParallel(TaskToolbox toolbox) throws Exception
  {
    ParallelIndexIngestionSpec ingestionSchemaToUse = ingestionSchema;
    PartialDimensionDistributionParallelIndexTaskRunner distributionRunner =
        (PartialDimensionDistributionParallelIndexTaskRunner)
        createRunner(
            toolbox,
            this::createPartialDimensionDistributionRunner
        );

    TaskState distributionState = runNextPhase(distributionRunner);
    if (distributionState.isFailure()) {
      String errMsg = StringUtils.format(TASK_PHASE_FAILURE_MSG, distributionRunner.getName());
      return TaskStatus.failure(getId(), errMsg);
    }

    // Get the partition boundaries for each interval
    final Map<Interval, PartitionBoundaries> intervalToPartitions;
    try {
      intervalToPartitions = distributionRunner.getIntervalToPartitionBoundaries(
          (DimensionRangePartitionsSpec) ingestionSchema.getTuningConfig().getGivenOrDefaultPartitionsSpec()
      );
      if (intervalToPartitions.isEmpty()) {
        String msg = "No valid rows for range partitioning."
                     + " All rows may have invalid timestamps or multiple dimension values.";
        LOG.warn(msg);
        return TaskStatus.success(getId(), msg);
      }
    }
    catch (Exception e) {
      String errorMsg = "Error creating partition boundaries.";
      if (distributionRunner.getStopReason() != null) {
        errorMsg += " " + distributionRunner.getStopReason();
      }
      LOG.error(e, errorMsg);
      return TaskStatus.failure(getId(), errorMsg);
    }

    ingestionSchemaToUse = rewriteIngestionSpecWithIntervalsIfMissing(
        ingestionSchemaToUse,
        intervalToPartitions.keySet()
    );

    final ParallelIndexIngestionSpec segmentCreateIngestionSpec = ingestionSchemaToUse;
    ParallelIndexTaskRunner<PartialRangeSegmentGenerateTask, GeneratedPartitionsReport> indexingRunner =
        createRunner(
            toolbox,
            tb -> createPartialRangeSegmentGenerateRunner(tb, intervalToPartitions, segmentCreateIngestionSpec)
        );

    TaskState indexingState = runNextPhase(indexingRunner);
    if (indexingState.isFailure()) {
      String errMsg = StringUtils.format(
          TASK_PHASE_FAILURE_MSG,
          indexingRunner.getName()
      );
      return TaskStatus.failure(getId(), errMsg);
    }

    indexGenerateRowStats = doGetRowStatsAndUnparseableEventsParallelMultiPhase(indexingRunner, true);

    // partition (interval, partitionId) -> partition locations
    Map<Partition, List<PartitionLocation>> partitionToLocations =
        getPartitionToLocations(indexingRunner.getReports());
    final List<PartialSegmentMergeIOConfig> ioConfigs = createGenericMergeIOConfigs(
        ingestionSchema.getTuningConfig().getTotalNumMergeTasks(),
        partitionToLocations
    );

    final ParallelIndexIngestionSpec segmentMergeIngestionSpec = ingestionSchemaToUse;
    ParallelIndexTaskRunner<PartialGenericSegmentMergeTask, PushedSegmentsReport> mergeRunner = createRunner(
        toolbox,
        tb -> createPartialGenericSegmentMergeRunner(tb, ioConfigs, segmentMergeIngestionSpec)
    );
    TaskState mergeState = runNextPhase(mergeRunner);
    TaskStatus taskStatus;
    if (mergeState.isSuccess()) {
      publishSegments(toolbox, mergeRunner.getReports());
      if (awaitSegmentAvailabilityTimeoutMillis > 0) {
        waitForSegmentAvailability(mergeRunner.getReports());
      }
      taskStatus = TaskStatus.success(getId());
    } else {
      // there is only success or failure after running....
      Preconditions.checkState(mergeState.isFailure(), "Unrecognized state after task is complete[%s]", mergeState);
      String errMsg = StringUtils.format(
          TASK_PHASE_FAILURE_MSG,
          mergeRunner.getName()
      );
      taskStatus = TaskStatus.failure(getId(), errMsg);
    }

    toolbox.getTaskReportFileWriter().write(
        getId(),
        getTaskCompletionReports(taskStatus, segmentAvailabilityConfirmationCompleted)
    );
    return taskStatus;
  }

  private static Map<Interval, Union> mergeCardinalityReports(Collection<DimensionCardinalityReport> reports)
  {
    Map<Interval, Union> finalCollectors = new HashMap<>();
    reports.forEach(report -> {
      Map<Interval, byte[]> intervalToCardinality = report.getIntervalToCardinalities();
      for (Map.Entry<Interval, byte[]> entry : intervalToCardinality.entrySet()) {
        HllSketch entryHll = HllSketch.wrap(Memory.wrap(entry.getValue()));
        finalCollectors.computeIfAbsent(
            entry.getKey(),
            k -> new Union(DimensionCardinalityReport.HLL_SKETCH_LOG_K)
        ).update(entryHll);
      }
    });
    return finalCollectors;
  }

  @VisibleForTesting
  public static Map<Interval, Integer> determineNumShardsFromCardinalityReport(
      Collection<DimensionCardinalityReport> reports,
      int maxRowsPerSegment
  )
  {
    // aggregate all the sub-reports
    Map<Interval, Union> finalCollectors = mergeCardinalityReports(reports);
    return computeIntervalToNumShards(maxRowsPerSegment, finalCollectors);
  }

  @Nonnull
  @VisibleForTesting
  static Map<Interval, Integer> computeIntervalToNumShards(
      int maxRowsPerSegment,
      Map<Interval, Union> finalCollectors
  )
  {
    return CollectionUtils.mapValues(
        finalCollectors,
        union -> {
          final double estimatedCardinality = union.getEstimate();
          final long estimatedNumShards;
          if (estimatedCardinality <= 0) {
            estimatedNumShards = DEFAULT_NUM_SHARDS_WHEN_ESTIMATE_GOES_NEGATIVE;
            LOG.warn("Estimated cardinality for union of estimates is zero or less: %.2f, setting num shards to %d",
                     estimatedCardinality, estimatedNumShards
            );
          } else {
            // determine numShards based on maxRowsPerSegment and the cardinality
            estimatedNumShards = Math.round(estimatedCardinality / maxRowsPerSegment);
          }
          LOG.info("estimatedNumShards %d given estimated cardinality %.2f and maxRowsPerSegment %d",
                    estimatedNumShards, estimatedCardinality, maxRowsPerSegment
          );
          // We have seen this before in the wild in situations where more shards should have been created,
          // log it if it happens with some information & context
          if (estimatedNumShards == 1) {
            LOG.info("estimatedNumShards is ONE (%d) given estimated cardinality %.2f and maxRowsPerSegment %d",
                      estimatedNumShards, estimatedCardinality, maxRowsPerSegment
            );
          }
          try {
            return Math.max(Math.toIntExact(estimatedNumShards), 1);
          }
          catch (ArithmeticException ae) {
            throw new ISE("Estimated numShards [%s] exceeds integer bounds.", estimatedNumShards);
          }
        }
    );
  }

  /**
   * Creates a map from partition (interval + bucketId) to the corresponding
   * PartitionLocations. Note that the bucketId maybe different from the final
   * partitionId (refer to {@link BuildingShardSpec} for more details).
   */
  static Map<Partition, List<PartitionLocation>> getPartitionToLocations(
      Map<String, GeneratedPartitionsReport> subTaskIdToReport
  )
  {
    // Create a map from partition to list of reports (PartitionStat and subTaskId)
    final Map<Partition, List<PartitionReport>> partitionToReports = new TreeMap<>(
        // Sort by (interval, bucketId) to maintain order of partitionIds within interval
        Comparator
            .comparingLong((Partition partition) -> partition.getInterval().getStartMillis())
            .thenComparingLong(partition -> partition.getInterval().getEndMillis())
            .thenComparingInt(Partition::getBucketId)
    );
    subTaskIdToReport.forEach(
        (subTaskId, report) -> report.getPartitionStats().forEach(
            partitionStat -> partitionToReports
                .computeIfAbsent(Partition.fromStat(partitionStat), p -> new ArrayList<>())
                .add(new PartitionReport(subTaskId, partitionStat))
        )
    );

    final Map<Partition, List<PartitionLocation>> partitionToLocations = new HashMap<>();

    Interval prevInterval = null;
    final AtomicInteger partitionId = new AtomicInteger(0);
    for (Entry<Partition, List<PartitionReport>> entry : partitionToReports.entrySet()) {
      final Partition partition = entry.getKey();

      // Reset the partitionId if this is a new interval
      Interval interval = partition.getInterval();
      if (!interval.equals(prevInterval)) {
        partitionId.set(0);
        prevInterval = interval;
      }

      // Use any PartitionStat of this partition to create a shard spec
      final List<PartitionReport> reportsOfPartition = entry.getValue();
      final BuildingShardSpec<?> shardSpec = reportsOfPartition
          .get(0).getPartitionStat().getSecondaryPartition()
          .convert(partitionId.getAndIncrement());

      // Create a PartitionLocation for each PartitionStat
      List<PartitionLocation> locationsOfPartition = reportsOfPartition
          .stream()
          .map(report -> report.getPartitionStat().toPartitionLocation(report.getSubTaskId(), shardSpec))
          .collect(Collectors.toList());

      partitionToLocations.put(partition, locationsOfPartition);
    }

    return partitionToLocations;
  }

  private static List<PartialSegmentMergeIOConfig> createGenericMergeIOConfigs(
      int totalNumMergeTasks,
      Map<Partition, List<PartitionLocation>> partitionToLocations
  )
  {
    return createMergeIOConfigs(
        totalNumMergeTasks,
        partitionToLocations,
        PartialSegmentMergeIOConfig::new
    );
  }

  @VisibleForTesting
  static <M extends PartialSegmentMergeIOConfig, L extends PartitionLocation> List<M> createMergeIOConfigs(
      int totalNumMergeTasks,
      Map<Partition, List<L>> partitionToLocations,
      Function<List<L>, M> createPartialSegmentMergeIOConfig
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
    final List<Partition> partitions = new ArrayList<>(partitionToLocations.keySet());
    Collections.shuffle(partitions, ThreadLocalRandom.current());

    final List<M> assignedPartitionLocations = new ArrayList<>(numMergeTasks);
    for (int i = 0; i < numMergeTasks; i++) {
      Pair<Integer, Integer> partitionBoundaries = getPartitionBoundaries(i, partitions.size(), numMergeTasks);
      final List<L> assignedToSameTask = partitions
          .subList(partitionBoundaries.lhs, partitionBoundaries.rhs)
          .stream()
          .flatMap(partition -> partitionToLocations.get(partition).stream())
          .collect(Collectors.toList());
      assignedPartitionLocations.add(createPartialSegmentMergeIOConfig.apply(assignedToSameTask));
    }

    return assignedPartitionLocations;
  }

  /**
   * Partition items into as evenly-sized splits as possible.
   *
   * @param index  index of partition
   * @param total  number of items to partition
   * @param splits number of desired partitions
   * @return partition range: [lhs, rhs)
   */
  private static Pair<Integer, Integer> getPartitionBoundaries(int index, int total, int splits)
  {
    int chunk = total / splits;
    int remainder = total % splits;

    // Distribute the remainder across the first few partitions. For example total=8 and splits=5, will give partitions
    // of sizes (starting from i=0): 2, 2, 2, 1, 1
    int start = index * chunk + (index < remainder ? index : remainder);
    int stop = start + chunk + (index < remainder ? 1 : 0);

    return Pair.of(start, stop);
  }

  private void publishSegments(
      TaskToolbox toolbox,
      Map<String, PushedSegmentsReport> reportsMap
  )
      throws IOException
  {
    final Set<DataSegment> oldSegments = new HashSet<>();
    final Set<DataSegment> newSegments = new HashSet<>();
    reportsMap
        .values()
        .forEach(report -> {
          oldSegments.addAll(report.getOldSegments());
          newSegments.addAll(report.getNewSegments());
        });
    final boolean storeCompactionState = getContextValue(
        Tasks.STORE_COMPACTION_STATE_KEY,
        Tasks.DEFAULT_STORE_COMPACTION_STATE
    );
    final Function<Set<DataSegment>, Set<DataSegment>> annotateFunction = compactionStateAnnotateFunction(
        storeCompactionState,
        toolbox,
        ingestionSchema
    );


    Set<DataSegment> tombStones = Collections.emptySet();
    if (getIngestionMode() == IngestionMode.REPLACE) {
      TombstoneHelper tombstoneHelper = new TombstoneHelper(toolbox.getTaskActionClient());
      List<Interval> tombstoneIntervals = tombstoneHelper.computeTombstoneIntervals(newSegments, ingestionSchema.getDataSchema());
      if (!tombstoneIntervals.isEmpty()) {

        Map<Interval, SegmentIdWithShardSpec> tombstonesAnShards = new HashMap<>();
        for (Interval interval : tombstoneIntervals) {
          SegmentIdWithShardSpec segmentIdWithShardSpec = allocateNewSegmentForTombstone(
              ingestionSchema,
              interval.getStart()
          );
          tombstonesAnShards.put(interval, segmentIdWithShardSpec);
        }

        tombStones = tombstoneHelper.computeTombstones(ingestionSchema.getDataSchema(), tombstonesAnShards);
        // add tombstones
        newSegments.addAll(tombStones);

        LOG.debugSegments(tombStones, "To publish tombstones");
      }
    }

    final TaskLockType taskLockType = getTaskLockHelper().getLockTypeToUse();
    final TransactionalSegmentPublisher publisher =
        (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) -> toolbox.getTaskActionClient().submit(
            buildPublishAction(segmentsToBeOverwritten, segmentsToPublish, taskLockType)
        );

    final boolean published =
        newSegments.isEmpty()
        || publisher.publishSegments(oldSegments, newSegments, annotateFunction, null).isSuccess();

    if (published) {
      LOG.info("Published [%d] segments", newSegments.size());

      // segment metrics:
      emitMetric(toolbox.getEmitter(), "ingest/tombstones/count", tombStones.size());
      emitMetric(toolbox.getEmitter(), "ingest/segments/count", newSegments.size());

    } else {
      throw new ISE("Failed to publish segments");
    }
  }

  private TaskStatus runSequential(TaskToolbox toolbox) throws Exception
  {
    IndexTask sequentialIndexTask = new IndexTask(
        getId(),
        getGroupId(),
        getTaskResource(),
        getDataSource(),
        baseSubtaskSpecName,
        new IndexIngestionSpec(
            getIngestionSchema().getDataSchema(),
            getIngestionSchema().getIOConfig(),
            convertToIndexTuningConfig(getIngestionSchema().getTuningConfig())
        ),
        getContext(),
        getIngestionSchema().getTuningConfig().getMaxAllowedLockCount(),
        // Don't run cleanup in the IndexTask since we are wrapping it in the ParallelIndexSupervisorTask
        false
    );

    if (currentSubTaskHolder.setTask(sequentialIndexTask)
        && sequentialIndexTask.isReady(toolbox.getTaskActionClient())) {
      return sequentialIndexTask.run(toolbox);
    } else {
      String msg = "Task was asked to stop. Finish as failed";
      LOG.info(msg);
      return TaskStatus.failure(getId(), msg);
    }
  }

  /**
   * Generate an IngestionStatsAndErrorsTaskReport for the task.
   *
   * @param taskStatus {@link TaskStatus}
   * @param segmentAvailabilityConfirmed Whether or not the segments were confirmed to be available for query when
   *                                     when the task completed.
   * @return
   */
  private Map<String, TaskReport> getTaskCompletionReports(TaskStatus taskStatus, boolean segmentAvailabilityConfirmed)
  {
    Pair<Map<String, Object>, Map<String, Object>> rowStatsAndUnparseableEvents =
        doGetRowStatsAndUnparseableEvents("true", true);
    return TaskReport.buildTaskReports(
        new IngestionStatsAndErrorsTaskReport(
            getId(),
            new IngestionStatsAndErrorsTaskReportData(
                IngestionState.COMPLETED,
                rowStatsAndUnparseableEvents.rhs,
                rowStatsAndUnparseableEvents.lhs,
                taskStatus.getErrorMsg(),
                segmentAvailabilityConfirmed,
                segmentAvailabilityWaitTimeMs,
                Collections.emptyMap()
            )
        )
    );
  }

  private static IndexTuningConfig convertToIndexTuningConfig(ParallelIndexTuningConfig tuningConfig)
  {
    return new IndexTuningConfig(
        null,
        null,
        tuningConfig.getAppendableIndexSpec(),
        tuningConfig.getMaxRowsInMemory(),
        tuningConfig.getMaxBytesInMemory(),
        tuningConfig.isSkipBytesInMemoryOverheadCheck(),
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
        tuningConfig.getMaxSavedParseExceptions(),
        tuningConfig.getMaxColumnsToMerge(),
        tuningConfig.getAwaitSegmentAvailabilityTimeoutMillis(),
        tuningConfig.getNumPersistThreads()
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
  public Response allocateSegment(
      Object param,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);

    if (toolbox == null) {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity("task is not running yet").build();
    }

    ParallelIndexTaskRunner runner = Preconditions.checkNotNull(getCurrentRunner(), "runner");
    if (!(runner instanceof SinglePhaseParallelIndexTaskRunner)) {
      throw new ISE(
          "Expected [%s], but [%s] is in use",
          SinglePhaseParallelIndexTaskRunner.class.getName(),
          runner.getClass().getName()
      );
    }

    // This context is set in the constructor of ParallelIndexSupervisorTask if it's not set by others.
    final boolean useLineageBasedSegmentAllocation = Preconditions.checkNotNull(
        getContextValue(SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY),
        "useLineageBasedSegmentAllocation in taskContext"
    );

    try {
      final SegmentIdWithShardSpec segmentIdentifier;
      if (useLineageBasedSegmentAllocation) {
        SegmentAllocationRequest request = toolbox.getJsonMapper().convertValue(param, SegmentAllocationRequest.class);
        segmentIdentifier = ((SinglePhaseParallelIndexTaskRunner) runner)
            .allocateNewSegment(
                getDataSource(),
                request.getTimestamp(),
                request.getSequenceName(),
                request.getPrevSegmentId()
            );
      } else {
        DateTime timestamp = toolbox.getJsonMapper().convertValue(param, DateTime.class);
        segmentIdentifier = ((SinglePhaseParallelIndexTaskRunner) runner)
            .allocateNewSegment(
                getDataSource(),
                timestamp
            );
      }

      return Response.ok(toolbox.getJsonMapper().writeValueAsBytes(segmentIdentifier)).build();
    }
    catch (MaxAllowedLocksExceededException malee) {
      getCurrentRunner().stopGracefully(malee.getMessage());
      return Response.status(Response.Status.BAD_REQUEST).entity(malee.getMessage()).build();
    }
    catch (IOException | IllegalStateException e) {
      return Response.serverError().entity(Throwables.getStackTraceAsString(e)).build();
    }
    catch (IllegalArgumentException e) {
      return Response.status(Response.Status.BAD_REQUEST).entity(Throwables.getStackTraceAsString(e)).build();
    }
  }


  static InputFormat getInputFormat(ParallelIndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getIOConfig().getNonNullInputFormat();
  }

  /**
   * Worker tasks spawned by the supervisor call this API to report the segments they generated and pushed.
   *
   * @see ParallelIndexSupervisorTaskClient#report(SubTaskReport)
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

  private RowIngestionMetersTotals getTotalsFromBuildSegmentsRowStats(Object buildSegmentsRowStats)
  {
    if (buildSegmentsRowStats instanceof RowIngestionMetersTotals) {
      // This case is for unit tests. Normally when deserialized the row stats will apppear as a Map<String, Object>.
      return (RowIngestionMetersTotals) buildSegmentsRowStats;
    } else if (buildSegmentsRowStats instanceof Map) {
      Map<String, Object> buildSegmentsRowStatsMap = (Map<String, Object>) buildSegmentsRowStats;
      return new RowIngestionMetersTotals(
          ((Number) buildSegmentsRowStatsMap.get("processed")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("processedBytes")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("processedWithError")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("thrownAway")).longValue(),
          ((Number) buildSegmentsRowStatsMap.get("unparseable")).longValue()
      );
    } else {
      // should never happen
      throw new RuntimeException("Unrecognized buildSegmentsRowStats type: " + buildSegmentsRowStats.getClass());
    }
  }

  private Pair<Map<String, Object>, Map<String, Object>> doGetRowStatsAndUnparseableEventsParallelSinglePhase(
      SinglePhaseParallelIndexTaskRunner parallelSinglePhaseRunner,
      boolean includeUnparseable
  )
  {
    final SimpleRowIngestionMeters buildSegmentsRowStats = new SimpleRowIngestionMeters();

    List<ParseExceptionReport> unparseableEvents = new ArrayList<>();

    // Get stats from completed tasks
    Map<String, PushedSegmentsReport> completedSubtaskReports = parallelSinglePhaseRunner.getReports();
    for (PushedSegmentsReport pushedSegmentsReport : completedSubtaskReports.values()) {
      Map<String, TaskReport> taskReport = pushedSegmentsReport.getTaskReport();
      if (taskReport == null || taskReport.isEmpty()) {
        LOG.warn("Got an empty task report from subtask: " + pushedSegmentsReport.getTaskId());
        continue;
      }
      RowIngestionMetersTotals rowIngestionMetersTotals =
          getBuildSegmentsStatsFromTaskReport(taskReport, includeUnparseable, unparseableEvents);

      buildSegmentsRowStats.addRowIngestionMetersTotals(rowIngestionMetersTotals);
    }

    RowIngestionMetersTotals rowStatsForRunningTasks = getRowStatsAndUnparseableEventsForRunningTasks(
        parallelSinglePhaseRunner.getRunningTaskIds(),
        unparseableEvents,
        includeUnparseable
    );
    buildSegmentsRowStats.addRowIngestionMetersTotals(rowStatsForRunningTasks);

    return createStatsAndErrorsReport(buildSegmentsRowStats.getTotals(), unparseableEvents);
  }

  private Pair<Map<String, Object>, Map<String, Object>> doGetRowStatsAndUnparseableEventsParallelMultiPhase(
      ParallelIndexTaskRunner<?, ?> currentRunner,
      boolean includeUnparseable
  )
  {
    if (indexGenerateRowStats != null) {
      return Pair.of(indexGenerateRowStats.lhs, includeUnparseable ? indexGenerateRowStats.rhs : ImmutableMap.of());
    } else if (!currentRunner.getName().equals("partial segment generation")) {
      return Pair.of(ImmutableMap.of(), ImmutableMap.of());
    } else {
      Map<String, GeneratedPartitionsReport> completedSubtaskReports =
          (Map<String, GeneratedPartitionsReport>) currentRunner.getReports();

      final SimpleRowIngestionMeters buildSegmentsRowStats = new SimpleRowIngestionMeters();
      final List<ParseExceptionReport> unparseableEvents = new ArrayList<>();
      for (GeneratedPartitionsReport generatedPartitionsReport : completedSubtaskReports.values()) {
        Map<String, TaskReport> taskReport = generatedPartitionsReport.getTaskReport();
        if (taskReport == null || taskReport.isEmpty()) {
          LOG.warn("Got an empty task report from subtask: " + generatedPartitionsReport.getTaskId());
          continue;
        }
        RowIngestionMetersTotals rowStatsForCompletedTask =
            getBuildSegmentsStatsFromTaskReport(taskReport, true, unparseableEvents);

        buildSegmentsRowStats.addRowIngestionMetersTotals(rowStatsForCompletedTask);
      }

      RowIngestionMetersTotals rowStatsForRunningTasks = getRowStatsAndUnparseableEventsForRunningTasks(
          currentRunner.getRunningTaskIds(),
          unparseableEvents,
          includeUnparseable
      );
      buildSegmentsRowStats.addRowIngestionMetersTotals(rowStatsForRunningTasks);

      return createStatsAndErrorsReport(buildSegmentsRowStats.getTotals(), unparseableEvents);
    }
  }

  private RowIngestionMetersTotals getRowStatsAndUnparseableEventsForRunningTasks(
      Set<String> runningTaskIds,
      List<ParseExceptionReport> unparseableEvents,
      boolean includeUnparseable
  )
  {
    final SimpleRowIngestionMeters buildSegmentsRowStats = new SimpleRowIngestionMeters();
    for (String runningTaskId : runningTaskIds) {
      try {
        final Map<String, Object> report = getTaskReport(toolbox.getOverlordClient(), runningTaskId);

        if (report == null || report.isEmpty()) {
          // task does not have a running report yet
          continue;
        }

        Map<String, Object> ingestionStatsAndErrors = (Map<String, Object>) report.get("ingestionStatsAndErrors");
        Map<String, Object> payload = (Map<String, Object>) ingestionStatsAndErrors.get("payload");
        Map<String, Object> rowStats = (Map<String, Object>) payload.get("rowStats");
        Map<String, Object> totals = (Map<String, Object>) rowStats.get("totals");
        Map<String, Object> buildSegments = (Map<String, Object>) totals.get(RowIngestionMeters.BUILD_SEGMENTS);

        if (includeUnparseable) {
          Map<String, Object> taskUnparseableEvents = (Map<String, Object>) payload.get("unparseableEvents");
          List<ParseExceptionReport> buildSegmentsUnparseableEvents = (List<ParseExceptionReport>)
              taskUnparseableEvents.get(RowIngestionMeters.BUILD_SEGMENTS);
          unparseableEvents.addAll(buildSegmentsUnparseableEvents);
        }

        buildSegmentsRowStats.addRowIngestionMetersTotals(getTotalsFromBuildSegmentsRowStats(buildSegments));
      }
      catch (Exception e) {
        LOG.warn(e, "Encountered exception when getting live subtask report for task: " + runningTaskId);
      }
    }
    return buildSegmentsRowStats.getTotals();
  }

  private Pair<Map<String, Object>, Map<String, Object>> createStatsAndErrorsReport(
      RowIngestionMetersTotals rowStats,
      List<ParseExceptionReport> unparseableEvents
  )
  {
    Map<String, Object> rowStatsMap = new HashMap<>();
    Map<String, Object> totalsMap = new HashMap<>();
    totalsMap.put(RowIngestionMeters.BUILD_SEGMENTS, rowStats);
    rowStatsMap.put("totals", totalsMap);

    return Pair.of(rowStatsMap, ImmutableMap.of(RowIngestionMeters.BUILD_SEGMENTS, unparseableEvents));
  }

  private RowIngestionMetersTotals getBuildSegmentsStatsFromTaskReport(
      Map<String, TaskReport> taskReport,
      boolean includeUnparseable,
      List<ParseExceptionReport> unparseableEvents)
  {
    IngestionStatsAndErrorsTaskReport ingestionStatsAndErrorsReport = (IngestionStatsAndErrorsTaskReport) taskReport.get(
        IngestionStatsAndErrorsTaskReport.REPORT_KEY);
    IngestionStatsAndErrorsTaskReportData reportData =
        (IngestionStatsAndErrorsTaskReportData) ingestionStatsAndErrorsReport.getPayload();
    RowIngestionMetersTotals totals = getTotalsFromBuildSegmentsRowStats(
        reportData.getRowStats().get(RowIngestionMeters.BUILD_SEGMENTS)
    );
    if (includeUnparseable) {
      List<ParseExceptionReport> taskUnparsebleEvents = (List<ParseExceptionReport>) reportData.getUnparseableEvents()
                                                                    .get(RowIngestionMeters.BUILD_SEGMENTS);
      unparseableEvents.addAll(taskUnparsebleEvents);
    }
    return totals;
  }

  private Pair<Map<String, Object>, Map<String, Object>> doGetRowStatsAndUnparseableEvents(
      String full,
      boolean includeUnparseable
  )
  {
    if (currentSubTaskHolder == null) {
      return Pair.of(ImmutableMap.of(), ImmutableMap.of());
    }

    Object currentRunner = currentSubTaskHolder.getTask();
    if (currentRunner == null) {
      return Pair.of(ImmutableMap.of(), ImmutableMap.of());
    }

    if (isParallelMode()) {
      if (isGuaranteedRollup(
          getIngestionMode(),
          ingestionSchema.getTuningConfig()
      )) {
        return doGetRowStatsAndUnparseableEventsParallelMultiPhase(
            (ParallelIndexTaskRunner<?, ?>) currentRunner,
            includeUnparseable
        );
      } else {
        return doGetRowStatsAndUnparseableEventsParallelSinglePhase(
            (SinglePhaseParallelIndexTaskRunner) currentRunner,
            includeUnparseable
        );
      }
    } else {
      IndexTask currentSequentialTask = (IndexTask) currentRunner;
      return Pair.of(currentSequentialTask.doGetRowStats(full), currentSequentialTask.doGetUnparseableEvents(full));
    }
  }

  @GET
  @Path("/rowStats")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRowStats(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);
    return Response.ok(doGetRowStatsAndUnparseableEvents(full, false).lhs).build();
  }

  @VisibleForTesting
  public Map<String, Object> doGetLiveReports(String full)
  {
    Map<String, Object> returnMap = new HashMap<>();
    Map<String, Object> ingestionStatsAndErrors = new HashMap<>();
    Map<String, Object> payload = new HashMap<>();

    Pair<Map<String, Object>, Map<String, Object>> rowStatsAndUnparsebleEvents =
        doGetRowStatsAndUnparseableEvents(full, true);

    // use the sequential task's ingestion state if we were running that mode
    IngestionState ingestionStateForReport;
    if (isParallelMode()) {
      ingestionStateForReport = ingestionState;
    } else {
      IndexTask currentSequentialTask = (IndexTask) currentSubTaskHolder.getTask();
      ingestionStateForReport = currentSequentialTask == null
                                ? ingestionState
                                : currentSequentialTask.getIngestionState();
    }

    payload.put("ingestionState", ingestionStateForReport);
    payload.put("unparseableEvents", rowStatsAndUnparsebleEvents.rhs);
    payload.put("rowStats", rowStatsAndUnparsebleEvents.lhs);

    ingestionStatsAndErrors.put("taskId", getId());
    ingestionStatsAndErrors.put("payload", payload);
    ingestionStatsAndErrors.put("type", "ingestionStatsAndErrors");

    returnMap.put("ingestionStatsAndErrors", ingestionStatsAndErrors);
    return returnMap;
  }

  @GET
  @Path("/liveReports")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLiveReports(
      @Context final HttpServletRequest req,
      @QueryParam("full") String full
  )
  {
    IndexTaskUtils.datasourceAuthorizationCheck(req, Action.READ, getDataSource(), authorizerMapper);

    return Response.ok(doGetLiveReports(full)).build();
  }

  /**
   * Like {@link OverlordClient#taskReportAsMap}, but synchronous, and returns null instead of throwing an error if
   * the server returns 404.
   */
  @Nullable
  @VisibleForTesting
  static Map<String, Object> getTaskReport(final OverlordClient overlordClient, final String taskId)
      throws InterruptedException, ExecutionException
  {
    try {
      return FutureUtils.get(overlordClient.taskReportAsMap(taskId), true);
    }
    catch (ExecutionException e) {
      if (e.getCause() instanceof HttpResponseException &&
          ((HttpResponseException) e.getCause()).getResponse().getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
        return null;
      } else {
        throw e;
      }
    }
  }

  /**
   * Represents a partition uniquely identified by an Interval and a bucketId.
   *
   * @see org.apache.druid.timeline.partition.BucketNumberedShardSpec
   */
  static class Partition
  {
    final Interval interval;
    final int bucketId;

    private static Partition fromStat(PartitionStat partitionStat)
    {
      return new Partition(partitionStat.getInterval(), partitionStat.getBucketId());
    }

    Partition(Interval interval, int bucketId)
    {
      this.interval = interval;
      this.bucketId = bucketId;
    }

    public int getBucketId()
    {
      return bucketId;
    }

    public Interval getInterval()
    {
      return interval;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Partition that = (Partition) o;
      return getBucketId() == that.getBucketId()
             && Objects.equals(getInterval(), that.getInterval());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getInterval(), getBucketId());
    }

    @Override
    public String toString()
    {
      return "Partition{" +
             "interval=" + interval +
             ", bucketId=" + bucketId +
             '}';
    }
  }

  /**
   * Encapsulates a {@link PartitionStat} and the subTaskId that generated it.
   */
  private static class PartitionReport
  {
    private final PartitionStat partitionStat;
    private final String subTaskId;

    PartitionReport(String subTaskId, PartitionStat partitionStat)
    {
      this.subTaskId = subTaskId;
      this.partitionStat = partitionStat;
    }

    String getSubTaskId()
    {
      return subTaskId;
    }

    PartitionStat getPartitionStat()
    {
      return partitionStat;
    }
  }

}
