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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.LockListAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
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
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistributionMerger;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketchMerger;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.TransactionalSegmentPublisher;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.BuildingNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.apache.druid.utils.CollectionUtils;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
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

  private final ConcurrentHashMap<Interval, AtomicInteger> partitionNumCountersPerInterval = new ConcurrentHashMap<>();

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

  @JsonCreator
  public ParallelIndexSupervisorTask(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") @Nullable String groupId,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") ParallelIndexIngestionSpec ingestionSchema,
      @JsonProperty("context") Map<String, Object> context
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

    if (isGuaranteedRollup(ingestionSchema.getIOConfig(), ingestionSchema.getTuningConfig())) {
      checkPartitionsSpecForForceGuaranteedRollup(ingestionSchema.getTuningConfig().getGivenOrDefaultPartitionsSpec());
    }

    this.baseInputSource = ingestionSchema.getIOConfig().getNonNullInputSource(
        ingestionSchema.getDataSchema().getParser()
    );
    this.missingIntervalsInOverwriteMode = !ingestionSchema.getIOConfig().isAppendToExisting()
                                           && ingestionSchema.getDataSchema()
                                                             .getGranularitySpec()
                                                             .inputIntervals()
                                                             .isEmpty();
    if (missingIntervalsInOverwriteMode) {
      addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
    }
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
        ingestionSchema,
        getContext(),
        intervalToPartitions
    );
  }

  @VisibleForTesting
  PartialGenericSegmentMergeParallelIndexTaskRunner createPartialGenericSegmentMergeRunner(
      TaskToolbox toolbox,
      List<PartialGenericSegmentMergeIOConfig> ioConfigs,
      ParallelIndexIngestionSpec ingestionSchema
  )
  {
    return new PartialGenericSegmentMergeParallelIndexTaskRunner(
        toolbox,
        getId(),
        getGroupId(),
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
    LOG.debug(
        "Found chat handler of class[%s]",
        Preconditions.checkNotNull(toolbox.getChatHandlerProvider(), "chatHandlerProvider").getClass().getName()
    );
    authorizerMapper = toolbox.getAuthorizerMapper();
    toolbox.getChatHandlerProvider().register(getId(), this, false);

    try {
      initializeSubTaskCleaner();

      if (isParallelMode()) {
        this.toolbox = toolbox;
        if (isGuaranteedRollup(ingestionSchema.getIOConfig(), ingestionSchema.getTuningConfig())) {
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
      toolbox.getChatHandlerProvider().unregister(getId());
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
    return tuningConfig.getGivenOrDefaultPartitionsSpec() instanceof SingleDimensionPartitionsSpec;
  }

  private boolean isParallelMode()
  {
    return isParallelMode(baseInputSource, ingestionSchema.getTuningConfig());
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

  private TaskStatus runHashPartitionMultiPhaseParallel(TaskToolbox toolbox) throws Exception
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
        return TaskStatus.failure(getId());
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
    ParallelIndexTaskRunner<PartialHashSegmentGenerateTask, GeneratedPartitionsReport<GenericPartitionStat>> indexingRunner =
        createRunner(
            toolbox,
            f -> createPartialHashSegmentGenerateRunner(toolbox, segmentCreateIngestionSpec, intervalToNumShards)
        );

    state = runNextPhase(indexingRunner);
    if (state.isFailure()) {
      return TaskStatus.failure(getId());
    }

    // 2. Partial segment merge phase
    // partition (interval, partitionId) -> partition locations
    Map<Pair<Interval, Integer>, List<GenericPartitionLocation>> partitionToLocations =
        groupGenericPartitionLocationsPerPartition(indexingRunner.getReports());
    final List<PartialGenericSegmentMergeIOConfig> ioConfigs = createGenericMergeIOConfigs(
        ingestionSchema.getTuningConfig().getTotalNumMergeTasks(),
        partitionToLocations
    );

    final ParallelIndexIngestionSpec segmentMergeIngestionSpec = ingestionSchemaToUse;
    final ParallelIndexTaskRunner<PartialGenericSegmentMergeTask, PushedSegmentsReport> mergeRunner = createRunner(
        toolbox,
        tb -> createPartialGenericSegmentMergeRunner(tb, ioConfigs, segmentMergeIngestionSpec)
    );
    state = runNextPhase(mergeRunner);
    if (state.isSuccess()) {
      //noinspection ConstantConditions
      publishSegments(toolbox, mergeRunner.getReports());
    }

    return TaskStatus.fromCode(getId(), state);
  }

  private TaskStatus runRangePartitionMultiPhaseParallel(TaskToolbox toolbox) throws Exception
  {
    ParallelIndexIngestionSpec ingestionSchemaToUse = ingestionSchema;
    ParallelIndexTaskRunner<PartialDimensionDistributionTask, DimensionDistributionReport> distributionRunner =
        createRunner(
            toolbox,
            this::createPartialDimensionDistributionRunner
        );

    TaskState distributionState = runNextPhase(distributionRunner);
    if (distributionState.isFailure()) {
      return TaskStatus.failure(getId(), PartialDimensionDistributionTask.TYPE + " failed");
    }

    Map<Interval, PartitionBoundaries> intervalToPartitions =
        determineAllRangePartitions(distributionRunner.getReports().values());

    if (intervalToPartitions.isEmpty()) {
      String msg = "No valid rows for single dimension partitioning."
                   + " All rows may have invalid timestamps or multiple dimension values.";
      LOG.warn(msg);
      return TaskStatus.success(getId(), msg);
    }

    ingestionSchemaToUse = rewriteIngestionSpecWithIntervalsIfMissing(
        ingestionSchemaToUse,
        intervalToPartitions.keySet()
    );

    final ParallelIndexIngestionSpec segmentCreateIngestionSpec = ingestionSchemaToUse;
    ParallelIndexTaskRunner<PartialRangeSegmentGenerateTask, GeneratedPartitionsReport<GenericPartitionStat>> indexingRunner =
        createRunner(
            toolbox,
            tb -> createPartialRangeSegmentGenerateRunner(tb, intervalToPartitions, segmentCreateIngestionSpec)
        );

    TaskState indexingState = runNextPhase(indexingRunner);
    if (indexingState.isFailure()) {
      return TaskStatus.failure(getId(), PartialRangeSegmentGenerateTask.TYPE + " failed");
    }

    // partition (interval, partitionId) -> partition locations
    Map<Pair<Interval, Integer>, List<GenericPartitionLocation>> partitionToLocations =
        groupGenericPartitionLocationsPerPartition(indexingRunner.getReports());
    final List<PartialGenericSegmentMergeIOConfig> ioConfigs = createGenericMergeIOConfigs(
        ingestionSchema.getTuningConfig().getTotalNumMergeTasks(),
        partitionToLocations
    );

    final ParallelIndexIngestionSpec segmentMergeIngestionSpec = ingestionSchemaToUse;
    ParallelIndexTaskRunner<PartialGenericSegmentMergeTask, PushedSegmentsReport> mergeRunner = createRunner(
        toolbox,
        tb -> createPartialGenericSegmentMergeRunner(tb, ioConfigs, segmentMergeIngestionSpec)
    );
    TaskState mergeState = runNextPhase(mergeRunner);
    if (mergeState.isSuccess()) {
      publishSegments(toolbox, mergeRunner.getReports());
    }

    return TaskStatus.fromCode(getId(), mergeState);
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

    return CollectionUtils.mapValues(
        finalCollectors,
        union -> {
          final double estimatedCardinality = union.getEstimate();
          // determine numShards based on maxRowsPerSegment and the cardinality
          final long estimatedNumShards = Math.round(estimatedCardinality / maxRowsPerSegment);
          try {
            return Math.max(Math.toIntExact(estimatedNumShards), 1);
          }
          catch (ArithmeticException ae) {
            throw new ISE("Estimated numShards [%s] exceeds integer bounds.", estimatedNumShards);
          }
        }
    );
  }

  private Map<Interval, PartitionBoundaries> determineAllRangePartitions(Collection<DimensionDistributionReport> reports)
  {
    Multimap<Interval, StringDistribution> intervalToDistributions = ArrayListMultimap.create();
    reports.forEach(report -> {
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      intervalToDistribution.forEach(intervalToDistributions::put);
    });

    return CollectionUtils.mapValues(intervalToDistributions.asMap(), this::determineRangePartition);
  }

  private PartitionBoundaries determineRangePartition(Collection<StringDistribution> distributions)
  {
    StringDistributionMerger distributionMerger = new StringSketchMerger();
    distributions.forEach(distributionMerger::merge);
    StringDistribution mergedDistribution = distributionMerger.getResult();

    SingleDimensionPartitionsSpec partitionsSpec =
        (SingleDimensionPartitionsSpec) ingestionSchema.getTuningConfig().getGivenOrDefaultPartitionsSpec();

    final PartitionBoundaries partitions;
    Integer targetRowsPerSegment = partitionsSpec.getTargetRowsPerSegment();
    if (targetRowsPerSegment == null) {
      partitions = mergedDistribution.getEvenPartitionsByMaxSize(partitionsSpec.getMaxRowsPerSegment());
    } else {
      partitions = mergedDistribution.getEvenPartitionsByTargetSize(targetRowsPerSegment);
    }

    return partitions;
  }

  private static Map<Pair<Interval, Integer>, List<GenericPartitionLocation>> groupGenericPartitionLocationsPerPartition(
      Map<String, GeneratedPartitionsReport<GenericPartitionStat>> subTaskIdToReport
  )
  {
    final Map<Pair<Interval, Integer>, BuildingShardSpec<?>> intervalAndIntegerToShardSpec = new HashMap<>();
    final Object2IntMap<Interval> intervalToNextPartitionId = new Object2IntOpenHashMap<>();
    final BiFunction<String, GenericPartitionStat, GenericPartitionLocation> createPartitionLocationFunction =
        (subtaskId, partitionStat) -> {
          final BuildingShardSpec<?> shardSpec = intervalAndIntegerToShardSpec.computeIfAbsent(
              Pair.of(partitionStat.getInterval(), partitionStat.getBucketId()),
              key -> {
                // Lazily determine the partitionId to create packed partitionIds for the core partitions.
                // See the Javadoc of BucketNumberedShardSpec for details.
                final int partitionId = intervalToNextPartitionId.computeInt(
                    partitionStat.getInterval(),
                    ((interval, nextPartitionId) -> nextPartitionId == null ? 0 : nextPartitionId + 1)
                );
                return partitionStat.getSecondaryPartition().convert(partitionId);
              }
          );
          return new GenericPartitionLocation(
              partitionStat.getTaskExecutorHost(),
              partitionStat.getTaskExecutorPort(),
              partitionStat.isUseHttps(),
              subtaskId,
              partitionStat.getInterval(),
              shardSpec
          );
        };

    return groupPartitionLocationsPerPartition(subTaskIdToReport, createPartitionLocationFunction);
  }

  private static <S extends PartitionStat, L extends PartitionLocation>
      Map<Pair<Interval, Integer>, List<L>> groupPartitionLocationsPerPartition(
      Map<String, ? extends GeneratedPartitionsReport<S>> subTaskIdToReport,
      BiFunction<String, S, L> createPartitionLocationFunction
  )
  {
    // partition (interval, partitionId) -> partition locations
    final Map<Pair<Interval, Integer>, List<L>> partitionToLocations = new HashMap<>();
    for (Entry<String, ? extends GeneratedPartitionsReport<S>> entry : subTaskIdToReport.entrySet()) {
      final String subTaskId = entry.getKey();
      final GeneratedPartitionsReport<S> report = entry.getValue();
      for (S partitionStat : report.getPartitionStats()) {
        final List<L> locationsOfSamePartition = partitionToLocations.computeIfAbsent(
            Pair.of(partitionStat.getInterval(), partitionStat.getBucketId()),
            k -> new ArrayList<>()
        );
        locationsOfSamePartition.add(createPartitionLocationFunction.apply(subTaskId, partitionStat));
      }
    }

    return partitionToLocations;
  }

  private static List<PartialGenericSegmentMergeIOConfig> createGenericMergeIOConfigs(
      int totalNumMergeTasks,
      Map<Pair<Interval, Integer>, List<GenericPartitionLocation>> partitionToLocations
  )
  {
    return createMergeIOConfigs(
        totalNumMergeTasks,
        partitionToLocations,
        PartialGenericSegmentMergeIOConfig::new
    );
  }

  @VisibleForTesting
  static <M extends PartialSegmentMergeIOConfig, L extends PartitionLocation> List<M> createMergeIOConfigs(
      int totalNumMergeTasks,
      Map<Pair<Interval, Integer>, List<L>> partitionToLocations,
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
    final List<Pair<Interval, Integer>> partitions = new ArrayList<>(partitionToLocations.keySet());
    Collections.shuffle(partitions, ThreadLocalRandom.current());

    final List<M> assignedPartitionLocations = new ArrayList<>(numMergeTasks);
    for (int i = 0; i < numMergeTasks; i++) {
      Pair<Integer, Integer> partitionBoundaries = getPartitionBoundaries(i, partitions.size(), numMergeTasks);
      final List<L> assignedToSameTask = partitions
          .subList(partitionBoundaries.lhs, partitionBoundaries.rhs)
          .stream()
          .flatMap(intervalAndPartitionId -> partitionToLocations.get(intervalAndPartitionId).stream())
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
        ingestionSchema.getTuningConfig()
    );
    final TransactionalSegmentPublisher publisher = (segmentsToBeOverwritten, segmentsToPublish, commitMetadata) ->
        toolbox.getTaskActionClient().submit(
            SegmentTransactionalInsertAction.overwriteAction(segmentsToBeOverwritten, segmentsToPublish)
        );
    final boolean published = newSegments.isEmpty()
                              || publisher.publishSegments(oldSegments, newSegments, annotateFunction, null)
                                          .isSuccess();

    if (published) {
      LOG.info("Published [%d] segments", newSegments.size());
    } else {
      throw new ISE("Failed to publish segments");
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
        getContext()
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
        tuningConfig.getMaxColumnsToMerge()
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
      return Response.ok(toolbox.getJsonMapper().writeValueAsBytes(segmentIdentifier)).build();
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
    // This method is called whenever subtasks need to allocate a new segment via the supervisor task.
    // As a result, this code is never called in the Overlord. For now using the materialized intervals
    // here is ok for performance reasons
    final Set<Interval> materializedBucketIntervals = granularitySpec.materializedBucketIntervals();

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
    if (!materializedBucketIntervals.isEmpty()) {
      // If granularity spec has explicit intervals, we just need to find the version associated to the interval.
      // This is because we should have gotten all required locks up front when the task starts up.
      final Optional<Interval> maybeInterval = granularitySpec.bucketInterval(timestamp);
      if (!maybeInterval.isPresent()) {
        throw new IAE("Could not find interval for timestamp [%s]", timestamp);
      }

      interval = maybeInterval.get();
      if (!materializedBucketIntervals.contains(interval)) {
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
        new BuildingNumberedShardSpec(partitionNum)
    );
  }

  @Nullable
  public static String findVersion(Map<Interval, String> versions, Interval interval)
  {
    return versions.entrySet().stream()
                   .filter(entry -> entry.getKey().contains(interval))
                   .map(Entry::getValue)
                   .findFirst()
                   .orElse(null);
  }

  static InputFormat getInputFormat(ParallelIndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getIOConfig().getNonNullInputFormat();
  }

  /**
   * Worker tasks spawned by the supervisor call this API to report the segments they generated and pushed.
   *
   * @see ParallelIndexSupervisorTaskClient#report(String, SubTaskReport)
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
