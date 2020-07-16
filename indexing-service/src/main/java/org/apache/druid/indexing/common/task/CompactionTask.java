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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.curator.shaded.com.google.common.base.Verify;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.Checks;
import org.apache.druid.indexer.Property;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTask.IndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineObjectHolder;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The client representation of this task is {@link ClientCompactionTaskQuery}. JSON
 * serialization fields of this class must correspond to those of {@link
 * ClientCompactionTaskQuery}.
 */
public class CompactionTask extends AbstractBatchIndexTask
{
  private static final Logger log = new Logger(CompactionTask.class);

  /**
   * The CompactionTask creates and runs multiple IndexTask instances. When the {@link AppenderatorsManager}
   * is asked to clean up, it does so on a per-task basis keyed by task ID. However, the subtask IDs of the
   * CompactionTask are not externally visible. This context flag is used to ensure that all the appenderators
   * created for the CompactionTasks's subtasks are tracked under the ID of the parent CompactionTask.
   * The CompactionTask may change in the future and no longer require this behavior (e.g., reusing the same
   * Appenderator across subtasks, or allowing the subtasks to use the same ID). The CompactionTask is also the only
   * task type that currently creates multiple appenderators. Thus, a context flag is used to handle this case
   * instead of a more general approach such as new methods on the Task interface.
   */
  public static final String CTX_KEY_APPENDERATOR_TRACKING_TASK_ID = "appenderatorTrackingTaskId";

  private static final String TYPE = "compact";

  static {
    Verify.verify(TYPE.equals(CompactSegments.COMPACTION_TASK_TYPE));
  }

  private final CompactionIOConfig ioConfig;
  @Nullable
  private final DimensionsSpec dimensionsSpec;
  @Nullable
  private final AggregatorFactory[] metricsSpec;
  @Nullable
  private final Granularity segmentGranularity;
  @Nullable
  private final ParallelIndexTuningConfig tuningConfig;
  private final ObjectMapper jsonMapper;
  @JsonIgnore
  private final SegmentProvider segmentProvider;
  @JsonIgnore
  private final PartitionConfigurationManager partitionConfigurationManager;

  @JsonIgnore
  private final AuthorizerMapper authorizerMapper;

  @JsonIgnore
  private final ChatHandlerProvider chatHandlerProvider;

  @JsonIgnore
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  @JsonIgnore
  private final CoordinatorClient coordinatorClient;

  private final IndexingServiceClient indexingServiceClient;

  @JsonIgnore
  private final SegmentLoaderFactory segmentLoaderFactory;

  @JsonIgnore
  private final RetryPolicyFactory retryPolicyFactory;

  @JsonIgnore
  private final AppenderatorsManager appenderatorsManager;

  @JsonIgnore
  private final CurrentSubTaskHolder currentSubTaskHolder = new CurrentSubTaskHolder(
      (taskObject, config) -> {
        final ParallelIndexSupervisorTask indexTask = (ParallelIndexSupervisorTask) taskObject;
        indexTask.stopGracefully(config);
      }
  );

  @JsonCreator
  public CompactionTask(
      @JsonProperty("id") final String id,
      @JsonProperty("resource") final TaskResource taskResource,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("interval") @Deprecated @Nullable final Interval interval,
      @JsonProperty("segments") @Deprecated @Nullable final List<DataSegment> segments,
      @JsonProperty("ioConfig") @Nullable CompactionIOConfig ioConfig,
      @JsonProperty("dimensions") @Nullable final DimensionsSpec dimensions,
      @JsonProperty("dimensionsSpec") @Nullable final DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable final AggregatorFactory[] metricsSpec,
      @JsonProperty("segmentGranularity") @Nullable final Granularity segmentGranularity,
      @JsonProperty("tuningConfig") @Nullable final TuningConfig tuningConfig,
      @JsonProperty("context") @Nullable final Map<String, Object> context,
      @JacksonInject ObjectMapper jsonMapper,
      @JacksonInject AuthorizerMapper authorizerMapper,
      @JacksonInject ChatHandlerProvider chatHandlerProvider,
      @JacksonInject RowIngestionMetersFactory rowIngestionMetersFactory,
      @JacksonInject CoordinatorClient coordinatorClient,
      @JacksonInject @Nullable IndexingServiceClient indexingServiceClient,
      @JacksonInject SegmentLoaderFactory segmentLoaderFactory,
      @JacksonInject RetryPolicyFactory retryPolicyFactory,
      @JacksonInject AppenderatorsManager appenderatorsManager
  )
  {
    super(getOrMakeId(id, TYPE, dataSource), null, taskResource, dataSource, context);

    Checks.checkOneNotNullOrEmpty(
        ImmutableList.of(
            new Property<>("ioConfig", ioConfig),
            new Property<>("interval", interval),
            new Property<>("segments", segments)
        )
    );

    if (ioConfig != null) {
      this.ioConfig = ioConfig;
    } else if (interval != null) {
      this.ioConfig = new CompactionIOConfig(new CompactionIntervalSpec(interval, null));
    } else {
      // We already checked segments is not null or empty above.
      //noinspection ConstantConditions
      this.ioConfig = new CompactionIOConfig(SpecificSegmentsSpec.fromSegments(segments));
    }

    this.dimensionsSpec = dimensionsSpec == null ? dimensions : dimensionsSpec;
    this.metricsSpec = metricsSpec;
    this.segmentGranularity = segmentGranularity;
    this.tuningConfig = tuningConfig != null ? getTuningConfig(tuningConfig) : null;
    this.jsonMapper = jsonMapper;
    this.segmentProvider = new SegmentProvider(dataSource, this.ioConfig.getInputSpec());
    this.partitionConfigurationManager = new PartitionConfigurationManager(this.tuningConfig);
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.indexingServiceClient = indexingServiceClient;
    this.coordinatorClient = coordinatorClient;
    this.segmentLoaderFactory = segmentLoaderFactory;
    this.retryPolicyFactory = retryPolicyFactory;
    this.appenderatorsManager = appenderatorsManager;
  }

  @VisibleForTesting
  static ParallelIndexTuningConfig getTuningConfig(TuningConfig tuningConfig)
  {
    if (tuningConfig instanceof ParallelIndexTuningConfig) {
      return (ParallelIndexTuningConfig) tuningConfig;
    } else if (tuningConfig instanceof IndexTuningConfig) {
      final IndexTuningConfig indexTuningConfig = (IndexTuningConfig) tuningConfig;
      return new ParallelIndexTuningConfig(
          null,
          indexTuningConfig.getMaxRowsPerSegment(),
          indexTuningConfig.getMaxRowsPerSegment(),
          indexTuningConfig.getMaxBytesInMemory(),
          indexTuningConfig.getMaxTotalRows(),
          indexTuningConfig.getNumShards(),
          null,
          indexTuningConfig.getPartitionsSpec(),
          indexTuningConfig.getIndexSpec(),
          indexTuningConfig.getIndexSpecForIntermediatePersists(),
          indexTuningConfig.getMaxPendingPersists(),
          indexTuningConfig.isForceGuaranteedRollup(),
          indexTuningConfig.isReportParseExceptions(),
          indexTuningConfig.getPushTimeout(),
          indexTuningConfig.getSegmentWriteOutMediumFactory(),
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          indexTuningConfig.isLogParseExceptions(),
          indexTuningConfig.getMaxParseExceptions(),
          indexTuningConfig.getMaxSavedParseExceptions()
      );
    } else {
      throw new ISE(
          "Unknown tuningConfig type: [%s], Must be either [%s] or [%s]",
          tuningConfig.getClass().getName(),
          ParallelIndexTuningConfig.class.getName(),
          IndexTuningConfig.class.getName()
      );
    }
  }

  @VisibleForTesting
  public CurrentSubTaskHolder getCurrentSubTaskHolder()
  {
    return currentSubTaskHolder;
  }

  @JsonProperty
  public CompactionIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  @Nullable
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  @Nullable
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  @Nullable
  @JsonProperty
  public ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @Override
  public int getPriority()
  {
    return getContextValue(Tasks.PRIORITY_KEY, Tasks.DEFAULT_MERGE_TASK_PRIORITY);
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    final List<DataSegment> segments = segmentProvider.findSegments(taskActionClient);
    return determineLockGranularityandTryLockWithSegments(taskActionClient, segments, segmentProvider::checkSegments);
  }

  @Override
  public boolean requireLockExistingSegments()
  {
    return true;
  }

  @Override
  public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
      throws IOException
  {
    return ImmutableList.copyOf(
        taskActionClient.submit(new RetrieveUsedSegmentsAction(getDataSource(), null, intervals, Segments.ONLY_VISIBLE))
    );
  }

  @Override
  public boolean isPerfectRollup()
  {
    return tuningConfig != null && tuningConfig.isForceGuaranteedRollup();
  }

  @Override
  public TaskStatus runTask(TaskToolbox toolbox) throws Exception
  {
    final List<ParallelIndexIngestionSpec> ingestionSpecs = createIngestionSchema(
        toolbox,
        getTaskLockHelper().getLockGranularityToUse(),
        segmentProvider,
        partitionConfigurationManager,
        dimensionsSpec,
        metricsSpec,
        segmentGranularity,
        jsonMapper,
        coordinatorClient,
        segmentLoaderFactory,
        retryPolicyFactory
    );
    final List<ParallelIndexSupervisorTask> indexTaskSpecs = IntStream
        .range(0, ingestionSpecs.size())
        .mapToObj(i -> {
          // taskId is used for different purposes in parallel indexing and local indexing.
          // In parallel indexing, it's the taskId of the supervisor task. This supervisor taskId must be
          // a valid taskId to communicate with sub tasks properly. We use the ID of the compaction task in this case.
          //
          // In local indexing, it's used as the sequence name for Appenderator. Even though a compaction task can run
          // multiple index tasks (one per interval), the appenderator is not shared by those tasks. Each task creates
          // a new Appenderator on its own instead. As a result, they should use different sequence names to allocate
          // new segmentIds properly. See IndexerSQLMetadataStorageCoordinator.allocatePendingSegments() for details.
          // In this case, we use different fake IDs for each created index task.
          final String subtaskId = tuningConfig == null || tuningConfig.getMaxNumConcurrentSubTasks() == 1
                                   ? createIndexTaskSpecId(i)
                                   : getId();
          return newTask(subtaskId, ingestionSpecs.get(i));
        })
        .collect(Collectors.toList());

    if (indexTaskSpecs.isEmpty()) {
      log.warn("Can't find segments from inputSpec[%s], nothing to do.", ioConfig.getInputSpec());
      return TaskStatus.failure(getId());
    } else {
      registerResourceCloserOnAbnormalExit(currentSubTaskHolder);
      final int totalNumSpecs = indexTaskSpecs.size();
      log.info("Generated [%d] compaction task specs", totalNumSpecs);

      int failCnt = 0;
      for (ParallelIndexSupervisorTask eachSpec : indexTaskSpecs) {
        final String json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(eachSpec);
        if (!currentSubTaskHolder.setTask(eachSpec)) {
          log.info("Task is asked to stop. Finish as failed.");
          return TaskStatus.failure(getId());
        }
        try {
          if (eachSpec.isReady(toolbox.getTaskActionClient())) {
            log.info("Running indexSpec: " + json);
            final TaskStatus eachResult = eachSpec.run(toolbox);
            if (!eachResult.isSuccess()) {
              failCnt++;
              log.warn("Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
            }
          } else {
            failCnt++;
            log.warn("indexSpec is not ready: [%s].\nTrying the next indexSpec.", json);
          }
        }
        catch (Exception e) {
          failCnt++;
          log.warn(e, "Failed to run indexSpec: [%s].\nTrying the next indexSpec.", json);
        }
      }

      log.info("Run [%d] specs, [%d] succeeded, [%d] failed", totalNumSpecs, totalNumSpecs - failCnt, failCnt);
      return failCnt == 0 ? TaskStatus.success(getId()) : TaskStatus.failure(getId());
    }
  }

  @VisibleForTesting
  ParallelIndexSupervisorTask newTask(String taskId, ParallelIndexIngestionSpec ingestionSpec)
  {
    return new ParallelIndexSupervisorTask(
        taskId,
        getGroupId(),
        getTaskResource(),
        ingestionSpec,
        createContextForSubtask(),
        indexingServiceClient,
        chatHandlerProvider,
        authorizerMapper,
        rowIngestionMetersFactory,
        appenderatorsManager
    );
  }

  @VisibleForTesting
  Map<String, Object> createContextForSubtask()
  {
    final Map<String, Object> newContext = new HashMap<>(getContext());
    newContext.put(CTX_KEY_APPENDERATOR_TRACKING_TASK_ID, getId());
    // Set the priority of the compaction task.
    newContext.put(Tasks.PRIORITY_KEY, getPriority());
    return newContext;
  }

  private String createIndexTaskSpecId(int i)
  {
    return StringUtils.format("%s_%d", getId(), i);
  }

  /**
   * Generate {@link ParallelIndexIngestionSpec} from input segments.
   *
   * @return an empty list if input segments don't exist. Otherwise, a generated ingestionSpec.
   */
  @VisibleForTesting
  static List<ParallelIndexIngestionSpec> createIngestionSchema(
      final TaskToolbox toolbox,
      final LockGranularity lockGranularityInUse,
      final SegmentProvider segmentProvider,
      final PartitionConfigurationManager partitionConfigurationManager,
      @Nullable final DimensionsSpec dimensionsSpec,
      @Nullable final AggregatorFactory[] metricsSpec,
      @Nullable final Granularity segmentGranularity,
      final ObjectMapper jsonMapper,
      final CoordinatorClient coordinatorClient,
      final SegmentLoaderFactory segmentLoaderFactory,
      final RetryPolicyFactory retryPolicyFactory
  ) throws IOException, SegmentLoadingException
  {
    NonnullPair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> pair = prepareSegments(
        toolbox,
        segmentProvider,
        lockGranularityInUse
    );
    final Map<DataSegment, File> segmentFileMap = pair.lhs;
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = pair.rhs;

    if (timelineSegments.size() == 0) {
      return Collections.emptyList();
    }

    // find metadata for interval
    // queryableIndexAndSegments is sorted by the interval of the dataSegment
    final List<NonnullPair<QueryableIndex, DataSegment>> queryableIndexAndSegments = loadSegments(
        timelineSegments,
        segmentFileMap,
        toolbox.getIndexIO()
    );

    final ParallelIndexTuningConfig compactionTuningConfig = partitionConfigurationManager.computeTuningConfig();

    if (segmentGranularity == null) {
      // original granularity
      final Map<Interval, List<NonnullPair<QueryableIndex, DataSegment>>> intervalToSegments = new TreeMap<>(
          Comparators.intervalsByStartThenEnd()
      );
      queryableIndexAndSegments.forEach(
          p -> intervalToSegments.computeIfAbsent(p.rhs.getInterval(), k -> new ArrayList<>())
                                 .add(p)
      );

      // unify overlapping intervals to ensure overlapping segments compacting in the same indexSpec
      List<NonnullPair<Interval, List<NonnullPair<QueryableIndex, DataSegment>>>> intervalToSegmentsUnified =
          new ArrayList<>();
      Interval union = null;
      List<NonnullPair<QueryableIndex, DataSegment>> segments = new ArrayList<>();
      for (Entry<Interval, List<NonnullPair<QueryableIndex, DataSegment>>> entry : intervalToSegments.entrySet()) {
        Interval cur = entry.getKey();
        if (union == null) {
          union = cur;
          segments.addAll(entry.getValue());
        } else if (union.overlaps(cur)) {
          union = Intervals.utc(union.getStartMillis(), Math.max(union.getEndMillis(), cur.getEndMillis()));
          segments.addAll(entry.getValue());
        } else {
          intervalToSegmentsUnified.add(new NonnullPair<>(union, segments));
          union = cur;
          segments = new ArrayList<>(entry.getValue());
        }
      }
      intervalToSegmentsUnified.add(new NonnullPair<>(union, segments));

      final List<ParallelIndexIngestionSpec> specs = new ArrayList<>(intervalToSegmentsUnified.size());
      for (NonnullPair<Interval, List<NonnullPair<QueryableIndex, DataSegment>>> entry : intervalToSegmentsUnified) {
        final Interval interval = entry.lhs;
        final List<NonnullPair<QueryableIndex, DataSegment>> segmentsToCompact = entry.rhs;
        final DataSchema dataSchema = createDataSchema(
            segmentProvider.dataSource,
            segmentsToCompact,
            dimensionsSpec,
            metricsSpec,
            GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity()
        );

        specs.add(
            new ParallelIndexIngestionSpec(
                dataSchema,
                createIoConfig(
                    toolbox,
                    dataSchema,
                    interval,
                    coordinatorClient,
                    segmentLoaderFactory,
                    retryPolicyFactory
                ),
                compactionTuningConfig
            )
        );
      }

      return specs;
    } else {
      // given segment granularity
      final DataSchema dataSchema = createDataSchema(
          segmentProvider.dataSource,
          queryableIndexAndSegments,
          dimensionsSpec,
          metricsSpec,
          segmentGranularity
      );

      return Collections.singletonList(
          new ParallelIndexIngestionSpec(
              dataSchema,
              createIoConfig(
                  toolbox,
                  dataSchema,
                  segmentProvider.interval,
                  coordinatorClient,
                  segmentLoaderFactory,
                  retryPolicyFactory
              ),
              compactionTuningConfig
          )
      );
    }
  }

  private static ParallelIndexIOConfig createIoConfig(
      TaskToolbox toolbox,
      DataSchema dataSchema,
      Interval interval,
      CoordinatorClient coordinatorClient,
      SegmentLoaderFactory segmentLoaderFactory,
      RetryPolicyFactory retryPolicyFactory
  )
  {
    return new ParallelIndexIOConfig(
        null,
        new DruidInputSource(
            dataSchema.getDataSource(),
            interval,
            null,
            null,
            dataSchema.getDimensionsSpec().getDimensionNames(),
            Arrays.stream(dataSchema.getAggregators()).map(AggregatorFactory::getName).collect(Collectors.toList()),
            toolbox.getIndexIO(),
            coordinatorClient,
            segmentLoaderFactory,
            retryPolicyFactory
        ),
        null,
        false
    );
  }

  private static NonnullPair<Map<DataSegment, File>, List<TimelineObjectHolder<String, DataSegment>>> prepareSegments(
      TaskToolbox toolbox,
      SegmentProvider segmentProvider,
      LockGranularity lockGranularityInUse
  ) throws IOException, SegmentLoadingException
  {
    final List<DataSegment> usedSegments = segmentProvider.findSegments(toolbox.getTaskActionClient());
    segmentProvider.checkSegments(lockGranularityInUse, usedSegments);
    final Map<DataSegment, File> segmentFileMap = toolbox.fetchSegments(usedSegments);
    final List<TimelineObjectHolder<String, DataSegment>> timelineSegments = VersionedIntervalTimeline
        .forSegments(usedSegments)
        .lookup(segmentProvider.interval);
    return new NonnullPair<>(segmentFileMap, timelineSegments);
  }

  private static DataSchema createDataSchema(
      String dataSource,
      List<NonnullPair<QueryableIndex, DataSegment>> queryableIndexAndSegments,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      Granularity segmentGranularity
  )
  {
    // check index metadata
    for (NonnullPair<QueryableIndex, DataSegment> pair : queryableIndexAndSegments) {
      final QueryableIndex index = pair.lhs;
      if (index.getMetadata() == null) {
        throw new RE("Index metadata doesn't exist for segment[%s]", pair.rhs.getId());
      }
    }

    // find granularity spec
    // set rollup only if rollup is set for all segments
    final boolean rollup = queryableIndexAndSegments.stream().allMatch(pair -> {
      // We have already checked getMetadata() doesn't return null
      final Boolean isRollup = pair.lhs.getMetadata().isRollup();
      return isRollup != null && isRollup;
    });

    final Interval totalInterval = JodaUtils.umbrellaInterval(
        queryableIndexAndSegments.stream().map(p -> p.rhs.getInterval()).collect(Collectors.toList())
    );

    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Preconditions.checkNotNull(segmentGranularity),
        Granularities.NONE,
        rollup,
        Collections.singletonList(totalInterval)
    );

    // find unique dimensions
    final DimensionsSpec finalDimensionsSpec = dimensionsSpec == null
                                               ? createDimensionsSpec(queryableIndexAndSegments)
                                               : dimensionsSpec;
    final AggregatorFactory[] finalMetricsSpec = metricsSpec == null
                                                 ? createMetricsSpec(queryableIndexAndSegments)
                                                 : convertToCombiningFactories(metricsSpec);

    return new DataSchema(
        dataSource,
        new TimestampSpec(null, null, null),
        finalDimensionsSpec,
        finalMetricsSpec,
        granularitySpec,
        null
    );
  }

  private static AggregatorFactory[] createMetricsSpec(
      List<NonnullPair<QueryableIndex, DataSegment>> queryableIndexAndSegments
  )
  {
    final List<AggregatorFactory[]> aggregatorFactories = queryableIndexAndSegments
        .stream()
        .map(pair -> pair.lhs.getMetadata().getAggregators()) // We have already done null check on index.getMetadata()
        .collect(Collectors.toList());
    final AggregatorFactory[] mergedAggregators = AggregatorFactory.mergeAggregators(aggregatorFactories);

    if (mergedAggregators == null) {
      throw new ISE("Failed to merge aggregators[%s]", aggregatorFactories);
    }
    return mergedAggregators;
  }

  private static AggregatorFactory[] convertToCombiningFactories(AggregatorFactory[] metricsSpec)
  {
    return Arrays.stream(metricsSpec)
                 .map(AggregatorFactory::getCombiningFactory)
                 .toArray(AggregatorFactory[]::new);
  }

  private static DimensionsSpec createDimensionsSpec(List<NonnullPair<QueryableIndex, DataSegment>> queryableIndices)
  {
    final BiMap<String, Integer> uniqueDims = HashBiMap.create();
    final Map<String, DimensionSchema> dimensionSchemaMap = new HashMap<>();

    // Here, we try to retain the order of dimensions as they were specified since the order of dimensions may be
    // optimized for performance.
    // Dimensions are extracted from the recent segments to olders because recent segments are likely to be queried more
    // frequently, and thus the performance should be optimized for recent ones rather than old ones.

    // sort timelineSegments in order of interval, see https://github.com/apache/druid/pull/9905
    queryableIndices.sort(
        (o1, o2) -> Comparators.intervalsByStartThenEnd().compare(o1.rhs.getInterval(), o2.rhs.getInterval())
    );

    int index = 0;
    for (NonnullPair<QueryableIndex, DataSegment> pair : Lists.reverse(queryableIndices)) {
      final QueryableIndex queryableIndex = pair.lhs;
      final Map<String, DimensionHandler> dimensionHandlerMap = queryableIndex.getDimensionHandlers();

      for (String dimension : queryableIndex.getAvailableDimensions()) {
        final ColumnHolder columnHolder = Preconditions.checkNotNull(
            queryableIndex.getColumnHolder(dimension),
            "Cannot find column for dimension[%s]",
            dimension
        );

        if (!uniqueDims.containsKey(dimension)) {
          final DimensionHandler dimensionHandler = Preconditions.checkNotNull(
              dimensionHandlerMap.get(dimension),
              "Cannot find dimensionHandler for dimension[%s]",
              dimension
          );

          uniqueDims.put(dimension, index++);
          dimensionSchemaMap.put(
              dimension,
              createDimensionSchema(
                  columnHolder.getCapabilities().getType(),
                  dimension,
                  dimensionHandler.getMultivalueHandling(),
                  columnHolder.getCapabilities().hasBitmapIndexes()
              )
          );
        }
      }
    }

    final BiMap<Integer, String> orderedDims = uniqueDims.inverse();
    final List<DimensionSchema> dimensionSchemas = IntStream.range(0, orderedDims.size())
                                                            .mapToObj(i -> {
                                                              final String dimName = orderedDims.get(i);
                                                              return Preconditions.checkNotNull(
                                                                  dimensionSchemaMap.get(dimName),
                                                                  "Cannot find dimension[%s] from dimensionSchemaMap",
                                                                  dimName
                                                              );
                                                            })
                                                            .collect(Collectors.toList());

    return new DimensionsSpec(dimensionSchemas, null, null);
  }

  private static List<NonnullPair<QueryableIndex, DataSegment>> loadSegments(
      List<TimelineObjectHolder<String, DataSegment>> timelineObjectHolders,
      Map<DataSegment, File> segmentFileMap,
      IndexIO indexIO
  ) throws IOException
  {
    final List<NonnullPair<QueryableIndex, DataSegment>> segments = new ArrayList<>();

    for (TimelineObjectHolder<String, DataSegment> timelineObjectHolder : timelineObjectHolders) {
      final PartitionHolder<DataSegment> partitionHolder = timelineObjectHolder.getObject();
      for (PartitionChunk<DataSegment> chunk : partitionHolder) {
        final DataSegment segment = chunk.getObject();
        final QueryableIndex queryableIndex = indexIO.loadIndex(
            Preconditions.checkNotNull(segmentFileMap.get(segment), "File for segment %s", segment.getId())
        );
        segments.add(new NonnullPair<>(queryableIndex, segment));
      }
    }

    return segments;
  }

  private static DimensionSchema createDimensionSchema(
      ValueType type,
      String name,
      MultiValueHandling multiValueHandling,
      boolean hasBitmapIndexes
  )
  {
    switch (type) {
      case FLOAT:
        Preconditions.checkArgument(
            multiValueHandling == null,
            "multi-value dimension [%s] is not supported for float type yet",
            name
        );
        return new FloatDimensionSchema(name);
      case LONG:
        Preconditions.checkArgument(
            multiValueHandling == null,
            "multi-value dimension [%s] is not supported for long type yet",
            name
        );
        return new LongDimensionSchema(name);
      case DOUBLE:
        Preconditions.checkArgument(
            multiValueHandling == null,
            "multi-value dimension [%s] is not supported for double type yet",
            name
        );
        return new DoubleDimensionSchema(name);
      case STRING:
        return new StringDimensionSchema(name, multiValueHandling, hasBitmapIndexes);
      default:
        throw new ISE("Unsupported value type[%s] for dimension[%s]", type, name);
    }
  }

  @VisibleForTesting
  static class SegmentProvider
  {
    private final String dataSource;
    private final CompactionInputSpec inputSpec;
    private final Interval interval;

    SegmentProvider(String dataSource, CompactionInputSpec inputSpec)
    {
      this.dataSource = Preconditions.checkNotNull(dataSource);
      this.inputSpec = inputSpec;
      this.interval = inputSpec.findInterval(dataSource);
    }

    List<DataSegment> findSegments(TaskActionClient actionClient) throws IOException
    {
      return new ArrayList<>(
          actionClient.submit(new RetrieveUsedSegmentsAction(dataSource, interval, null, Segments.ONLY_VISIBLE))
      );
    }

    void checkSegments(LockGranularity lockGranularityInUse, List<DataSegment> latestSegments)
    {
      if (!inputSpec.validateSegments(lockGranularityInUse, latestSegments)) {
        throw new ISE(
            "Specified segments in the spec are different from the current used segments. "
            + "Possibly new segments would have been added or some segments have been unpublished."
        );
      }
    }
  }

  @VisibleForTesting
  static class PartitionConfigurationManager
  {
    @Nullable
    private final ParallelIndexTuningConfig tuningConfig;

    PartitionConfigurationManager(@Nullable ParallelIndexTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
    }

    @Nullable
    ParallelIndexTuningConfig computeTuningConfig()
    {
      ParallelIndexTuningConfig newTuningConfig = tuningConfig == null
                                          ? ParallelIndexTuningConfig.defaultConfig()
                                          : tuningConfig;
      PartitionsSpec partitionsSpec = newTuningConfig.getGivenOrDefaultPartitionsSpec();
      if (partitionsSpec instanceof DynamicPartitionsSpec) {
        final DynamicPartitionsSpec dynamicPartitionsSpec = (DynamicPartitionsSpec) partitionsSpec;
        partitionsSpec = new DynamicPartitionsSpec(
            dynamicPartitionsSpec.getMaxRowsPerSegment(),
            // Setting maxTotalRows to Long.MAX_VALUE to respect the computed maxRowsPerSegment.
            // If this is set to something too small, compactionTask can generate small segments
            // which need to be compacted again, which in turn making auto compaction stuck in the same interval.
            dynamicPartitionsSpec.getMaxTotalRowsOr(Long.MAX_VALUE)
        );
      }
      return newTuningConfig.withPartitionsSpec(partitionsSpec);
    }
  }

  public static class Builder
  {
    private final String dataSource;
    private final ObjectMapper jsonMapper;
    private final AuthorizerMapper authorizerMapper;
    private final ChatHandlerProvider chatHandlerProvider;
    private final RowIngestionMetersFactory rowIngestionMetersFactory;
    private final IndexingServiceClient indexingServiceClient;
    private final CoordinatorClient coordinatorClient;
    private final SegmentLoaderFactory segmentLoaderFactory;
    private final RetryPolicyFactory retryPolicyFactory;
    private final AppenderatorsManager appenderatorsManager;

    private CompactionIOConfig ioConfig;
    @Nullable
    private DimensionsSpec dimensionsSpec;
    @Nullable
    private AggregatorFactory[] metricsSpec;
    @Nullable
    private Granularity segmentGranularity;
    @Nullable
    private TuningConfig tuningConfig;
    @Nullable
    private Map<String, Object> context;

    public Builder(
        String dataSource,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper,
        ChatHandlerProvider chatHandlerProvider,
        RowIngestionMetersFactory rowIngestionMetersFactory,
        IndexingServiceClient indexingServiceClient,
        CoordinatorClient coordinatorClient,
        SegmentLoaderFactory segmentLoaderFactory,
        RetryPolicyFactory retryPolicyFactory,
        AppenderatorsManager appenderatorsManager
    )
    {
      this.dataSource = dataSource;
      this.jsonMapper = jsonMapper;
      this.authorizerMapper = authorizerMapper;
      this.chatHandlerProvider = chatHandlerProvider;
      this.rowIngestionMetersFactory = rowIngestionMetersFactory;
      this.indexingServiceClient = indexingServiceClient;
      this.coordinatorClient = coordinatorClient;
      this.segmentLoaderFactory = segmentLoaderFactory;
      this.retryPolicyFactory = retryPolicyFactory;
      this.appenderatorsManager = appenderatorsManager;
    }

    public Builder interval(Interval interval)
    {
      return inputSpec(new CompactionIntervalSpec(interval, null));
    }

    public Builder segments(List<DataSegment> segments)
    {
      return inputSpec(SpecificSegmentsSpec.fromSegments(segments));
    }

    public Builder inputSpec(CompactionInputSpec inputSpec)
    {
      this.ioConfig = new CompactionIOConfig(inputSpec);
      return this;
    }

    public Builder dimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder metricsSpec(AggregatorFactory[] metricsSpec)
    {
      this.metricsSpec = metricsSpec;
      return this;
    }

    public Builder segmentGranularity(Granularity segmentGranularity)
    {
      this.segmentGranularity = segmentGranularity;
      return this;
    }

    public Builder tuningConfig(TuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder context(Map<String, Object> context)
    {
      this.context = context;
      return this;
    }

    public CompactionTask build()
    {
      return new CompactionTask(
          null,
          null,
          dataSource,
          null,
          null,
          ioConfig,
          null,
          dimensionsSpec,
          metricsSpec,
          segmentGranularity,
          tuningConfig,
          context,
          jsonMapper,
          authorizerMapper,
          chatHandlerProvider,
          rowIngestionMetersFactory,
          coordinatorClient,
          indexingServiceClient,
          segmentLoaderFactory,
          retryPolicyFactory,
          appenderatorsManager
      );
    }
  }
}
