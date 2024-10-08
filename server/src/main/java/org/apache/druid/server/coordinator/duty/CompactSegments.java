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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionIOConfig;
import org.apache.druid.client.indexing.ClientCompactionIntervalSpec;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.client.indexing.ClientCompactionTaskDimensionsSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.CompactionSegmentIterator;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.compaction.PriorityBasedCompactionSegmentIterator;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompactSegments implements CoordinatorCustomDuty
{
  /**
   * Must be the same as org.apache.druid.indexing.common.task.CompactionTask.TYPE.
   */
  public static final String COMPACTION_TASK_TYPE = "compact";
  /**
   * Must be the same as org.apache.druid.indexing.common.task.Tasks.STORE_COMPACTION_STATE_KEY
   */
  public static final String STORE_COMPACTION_STATE_KEY = "storeCompactionState";
  private static final String COMPACTION_REASON_KEY = "compactionReason";

  private static final Logger LOG = new Logger(CompactSegments.class);

  private static final String TASK_ID_PREFIX = "coordinator-issued";
  private static final Predicate<TaskStatusPlus> IS_COMPACTION_TASK =
      status -> null != status && COMPACTION_TASK_TYPE.equals(status.getType());

  private final CompactionStatusTracker statusTracker;
  private final OverlordClient overlordClient;

  // This variable is updated by the Coordinator thread executing duties and
  // read by HTTP threads processing Coordinator API calls.
  private final AtomicReference<Map<String, AutoCompactionSnapshot>> autoCompactionSnapshotPerDataSource = new AtomicReference<>();

  @JsonCreator
  public CompactSegments(
      @JacksonInject CompactionStatusTracker statusTracker,
      @JacksonInject OverlordClient overlordClient
  )
  {
    this.overlordClient = overlordClient;
    this.statusTracker = statusTracker;
    resetCompactionSnapshot();
  }

  @VisibleForTesting
  public OverlordClient getOverlordClient()
  {
    return overlordClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (isCompactionSupervisorEnabled()) {
      LOG.warn(
          "Skipping CompactSegments duty since compaction supervisors"
          + " are already running on Overlord."
      );
    } else {
      run(
          params.getCompactionConfig(),
          params.getDataSourcesSnapshot(),
          CompactionEngine.NATIVE,
          params.getCoordinatorStats()
      );
    }
    return params;
  }

  public void run(
      DruidCompactionConfig dynamicConfig,
      DataSourcesSnapshot dataSources,
      CompactionEngine defaultEngine,
      CoordinatorRunStats stats
  )
  {
    final int maxCompactionTaskSlots = dynamicConfig.getMaxCompactionTaskSlots();
    if (maxCompactionTaskSlots <= 0) {
      resetCompactionSnapshot();
      return;
    }

    statusTracker.onSegmentTimelineUpdated(dataSources.getSnapshotTime());
    statusTracker.onCompactionConfigUpdated(dynamicConfig);
    List<DataSourceCompactionConfig> compactionConfigList = dynamicConfig.getCompactionConfigs();
    if (compactionConfigList == null || compactionConfigList.isEmpty()) {
      resetCompactionSnapshot();
      return;
    }

    Map<String, DataSourceCompactionConfig> compactionConfigs = compactionConfigList
        .stream()
        .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));

    // Map from dataSource to list of intervals for which compaction will be skipped in this run
    final Map<String, List<Interval>> intervalsToSkipCompaction = new HashMap<>();

    // Fetch currently running compaction tasks
    int busyCompactionTaskSlots = 0;
    final List<TaskStatusPlus> compactionTasks = CoordinatorDutyUtils.getStatusOfActiveTasks(
        overlordClient,
        IS_COMPACTION_TASK
    );

    final Set<String> activeTaskIds
        = compactionTasks.stream().map(TaskStatusPlus::getId).collect(Collectors.toSet());
    trackStatusOfCompletedTasks(activeTaskIds);

    for (TaskStatusPlus status : compactionTasks) {
      final TaskPayloadResponse response =
          FutureUtils.getUnchecked(overlordClient.taskPayload(status.getId()), true);
      if (response == null) {
        throw new ISE("Could not find payload for active compaction task[%s]", status.getId());
      } else if (!COMPACTION_TASK_TYPE.equals(response.getPayload().getType())) {
        throw new ISE(
            "Payload of active compaction task[%s] is of invalid type[%s]",
            status.getId(), response.getPayload().getType()
        );
      }

      final ClientCompactionTaskQuery compactionTaskQuery = (ClientCompactionTaskQuery) response.getPayload();
      DataSourceCompactionConfig dataSourceCompactionConfig = compactionConfigs.get(status.getDataSource());
      if (cancelTaskIfGranularityChanged(compactionTaskQuery, dataSourceCompactionConfig)) {
        continue;
      }

      // Skip this interval as the current active compaction task is good
      final Interval interval = compactionTaskQuery.getIoConfig().getInputSpec().getInterval();
      intervalsToSkipCompaction.computeIfAbsent(status.getDataSource(), k -> new ArrayList<>())
                               .add(interval);
      // Note: The default compactionRunnerType used here should match the default runner used in CompactionTask when
      // no runner is provided there.
      CompactionEngine compactionRunnerType = compactionTaskQuery.getCompactionRunner() == null
                                              ? CompactionEngine.NATIVE
                                              : compactionTaskQuery.getCompactionRunner().getType();
      if (compactionRunnerType == CompactionEngine.NATIVE) {
        busyCompactionTaskSlots +=
            findMaxNumTaskSlotsUsedByOneNativeCompactionTask(compactionTaskQuery.getTuningConfig());
      } else {
        busyCompactionTaskSlots += findMaxNumTaskSlotsUsedByOneMsqCompactionTask(compactionTaskQuery.getContext());
      }
    }

    // Skip all the intervals locked by higher priority tasks for each datasource
    // This must be done after the invalid compaction tasks are cancelled
    // in the loop above so that their intervals are not considered locked
    getLockedIntervals(compactionConfigList).forEach(
        (dataSource, intervals) ->
            intervalsToSkipCompaction
                .computeIfAbsent(dataSource, ds -> new ArrayList<>())
                .addAll(intervals)
    );

    // Get iterator over segments to compact and submit compaction tasks
    final CompactionCandidateSearchPolicy policy = dynamicConfig.getCompactionPolicy();
    final CompactionSegmentIterator iterator = new PriorityBasedCompactionSegmentIterator(
        policy,
        compactionConfigs,
        dataSources.getUsedSegmentsTimelinesPerDataSource(),
        intervalsToSkipCompaction,
        statusTracker
    );

    final int compactionTaskCapacity = getCompactionTaskCapacity(dynamicConfig);
    final int availableCompactionTaskSlots
        = getAvailableCompactionTaskSlots(compactionTaskCapacity, busyCompactionTaskSlots);

    final Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders = new HashMap<>();
    final int numSubmittedCompactionTasks = submitCompactionTasks(
        compactionConfigs,
        currentRunAutoCompactionSnapshotBuilders,
        availableCompactionTaskSlots,
        iterator,
        defaultEngine
    );

    stats.add(Stats.Compaction.MAX_SLOTS, compactionTaskCapacity);
    stats.add(Stats.Compaction.AVAILABLE_SLOTS, availableCompactionTaskSlots);
    stats.add(Stats.Compaction.SUBMITTED_TASKS, numSubmittedCompactionTasks);
    updateCompactionSnapshotStats(currentRunAutoCompactionSnapshotBuilders, iterator, stats);
  }

  private void resetCompactionSnapshot()
  {
    autoCompactionSnapshotPerDataSource.set(Collections.emptyMap());
  }

  /**
   * Check if compaction supervisors are enabled on the Overlord. In this case,
   * CompactSegments duty should not be run.
   */
  private boolean isCompactionSupervisorEnabled()
  {
    try {
      return FutureUtils.getUnchecked(overlordClient.isCompactionSupervisorEnabled(), true);
    }
    catch (Exception e) {
      // The Overlord is probably on an older version, assume that compaction supervisor is NOT enabled
      return false;
    }
  }

  /**
   * Queries the Overlord for the status of all tasks that were submitted
   * recently but are not active anymore. The statuses are then updated in the
   * {@link #statusTracker}.
   */
  private void trackStatusOfCompletedTasks(Set<String> activeTaskIds)
  {
    final Set<String> finishedTaskIds = new HashSet<>(statusTracker.getSubmittedTaskIds());
    finishedTaskIds.removeAll(activeTaskIds);

    if (finishedTaskIds.isEmpty()) {
      return;
    }

    final Map<String, TaskStatus> taskStatusMap
        = FutureUtils.getUnchecked(overlordClient.taskStatuses(finishedTaskIds), true);
    for (String taskId : finishedTaskIds) {
      // Assume unknown task to have finished successfully
      final TaskStatus taskStatus = taskStatusMap.getOrDefault(taskId, TaskStatus.success(taskId));
      if (taskStatus.isComplete()) {
        statusTracker.onTaskFinished(taskId, taskStatus);
      }
    }
  }

  /**
   * Cancels a currently running compaction task if the segment granularity
   * for this datasource has changed in the compaction config.
   *
   * @return true if the task was canceled, false otherwise.
   */
  private boolean cancelTaskIfGranularityChanged(
      ClientCompactionTaskQuery compactionTaskQuery,
      DataSourceCompactionConfig dataSourceCompactionConfig
  )
  {
    if (dataSourceCompactionConfig == null
        || dataSourceCompactionConfig.getGranularitySpec() == null
        || compactionTaskQuery.getGranularitySpec() == null) {
      return false;
    }

    Granularity configuredSegmentGranularity = dataSourceCompactionConfig.getGranularitySpec()
                                                                         .getSegmentGranularity();
    Granularity taskSegmentGranularity = compactionTaskQuery.getGranularitySpec().getSegmentGranularity();
    if (configuredSegmentGranularity == null || configuredSegmentGranularity.equals(taskSegmentGranularity)) {
      return false;
    }

    LOG.info(
        "Cancelling task[%s] as task segmentGranularity[%s] differs from compaction config segmentGranularity[%s].",
        compactionTaskQuery.getId(), taskSegmentGranularity, configuredSegmentGranularity
    );
    overlordClient.cancelTask(compactionTaskQuery.getId());
    return true;
  }

  /**
   * Gets a List of Intervals locked by higher priority tasks for each datasource.
   * However, when using a REPLACE lock for compaction, intervals locked with any APPEND lock will not be returned
   * Since compaction tasks submitted for these Intervals would have to wait anyway,
   * we skip these Intervals until the next compaction run.
   * <p>
   * For now, Segment Locks are being treated the same as Time Chunk Locks even
   * though they lock only a Segment and not the entire Interval. Thus,
   * a compaction task will not be submitted for an Interval if
   * <ul>
   *   <li>either the whole Interval is locked by a higher priority Task with an incompatible lock type</li>
   *   <li>or there is atleast one Segment in the Interval that is locked by a
   *   higher priority Task</li>
   * </ul>
   */
  private Map<String, List<Interval>> getLockedIntervals(
      List<DataSourceCompactionConfig> compactionConfigs
  )
  {
    final List<LockFilterPolicy> lockFilterPolicies = compactionConfigs
        .stream()
        .map(config ->
                 new LockFilterPolicy(config.getDataSource(), config.getTaskPriority(), null, config.getTaskContext()))
        .collect(Collectors.toList());
    final Map<String, List<Interval>> datasourceToLockedIntervals =
        new HashMap<>(FutureUtils.getUnchecked(overlordClient.findLockedIntervals(lockFilterPolicies), true));
    LOG.debug(
        "Skipping the following intervals for Compaction as they are currently locked: %s",
        datasourceToLockedIntervals
    );

    return datasourceToLockedIntervals;
  }

  /**
   * Returns the maximum number of task slots used by one native compaction task at any time when the task is
   * issued with the given tuningConfig.
   */
  public static int findMaxNumTaskSlotsUsedByOneNativeCompactionTask(
      @Nullable ClientCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    if (isParallelMode(tuningConfig)) {
      @Nullable
      Integer maxNumConcurrentSubTasks = tuningConfig.getMaxNumConcurrentSubTasks();
      // Max number of task slots used in parallel mode = maxNumConcurrentSubTasks + 1 (supervisor task)
      return (maxNumConcurrentSubTasks == null ? 1 : maxNumConcurrentSubTasks) + 1;
    } else {
      return 1;
    }
  }

  /**
   * Returns the maximum number of task slots used by one MSQ compaction task at any time when the task is
   * issued with the given context.
   */
  static int findMaxNumTaskSlotsUsedByOneMsqCompactionTask(@Nullable Map<String, Object> context)
  {
    return context == null
           ? ClientMSQContext.DEFAULT_MAX_NUM_TASKS
           : (int) context.getOrDefault(ClientMSQContext.CTX_MAX_NUM_TASKS, ClientMSQContext.DEFAULT_MAX_NUM_TASKS);
  }


  /**
   * Returns true if the compaction task can run in the parallel mode with the given tuningConfig.
   * This method should be synchronized with ParallelIndexSupervisorTask.isParallelMode(InputSource, ParallelIndexTuningConfig).
   */
  @VisibleForTesting
  static boolean isParallelMode(@Nullable ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    if (null == tuningConfig) {
      return false;
    }
    boolean useRangePartitions = useRangePartitions(tuningConfig);
    int minRequiredNumConcurrentSubTasks = useRangePartitions ? 1 : 2;
    return tuningConfig.getMaxNumConcurrentSubTasks() != null
           && tuningConfig.getMaxNumConcurrentSubTasks() >= minRequiredNumConcurrentSubTasks;
  }

  private static boolean useRangePartitions(ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    // dynamic partitionsSpec will be used if getPartitionsSpec() returns null
    return tuningConfig.getPartitionsSpec() instanceof DimensionRangePartitionsSpec;
  }

  private int getCompactionTaskCapacity(DruidCompactionConfig dynamicConfig)
  {
    int totalWorkerCapacity = CoordinatorDutyUtils.getTotalWorkerCapacity(overlordClient);

    return Math.min(
        (int) (totalWorkerCapacity * dynamicConfig.getCompactionTaskSlotRatio()),
        dynamicConfig.getMaxCompactionTaskSlots()
    );
  }

  private int getAvailableCompactionTaskSlots(int compactionTaskCapacity, int busyCompactionTaskSlots)
  {
    final int availableCompactionTaskSlots;
    if (busyCompactionTaskSlots > 0) {
      availableCompactionTaskSlots = Math.max(0, compactionTaskCapacity - busyCompactionTaskSlots);
    } else {
      // compactionTaskCapacity might be 0 if totalWorkerCapacity is low.
      // This guarantees that at least one slot is available if
      // compaction is enabled and estimatedIncompleteCompactionTasks is 0.
      availableCompactionTaskSlots = Math.max(1, compactionTaskCapacity);
    }
    LOG.debug(
        "Found [%d] available task slots for compaction out of max compaction task capacity [%d]",
        availableCompactionTaskSlots, compactionTaskCapacity
    );

    return availableCompactionTaskSlots;
  }

  /**
   * Submits compaction tasks to the Overlord. Returns total number of tasks submitted.
   */
  private int submitCompactionTasks(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders,
      int numAvailableCompactionTaskSlots,
      CompactionSegmentIterator iterator,
      CompactionEngine defaultEngine
  )
  {
    if (numAvailableCompactionTaskSlots <= 0) {
      return 0;
    }

    int numSubmittedTasks = 0;
    int totalTaskSlotsAssigned = 0;

    while (iterator.hasNext() && totalTaskSlotsAssigned < numAvailableCompactionTaskSlots) {
      final CompactionCandidate entry = iterator.next();
      final String dataSourceName = entry.getDataSource();

      // As these segments will be compacted, we will aggregate the statistic to the Compacted statistics
      currentRunAutoCompactionSnapshotBuilders
          .computeIfAbsent(dataSourceName, AutoCompactionSnapshot::builder)
          .incrementCompactedStats(entry.getStats());

      final DataSourceCompactionConfig config = compactionConfigs.get(dataSourceName);
      final List<DataSegment> segmentsToCompact = entry.getSegments();

      // Create granularitySpec to send to compaction task
      Granularity segmentGranularityToUse = null;
      if (config.getGranularitySpec() == null || config.getGranularitySpec().getSegmentGranularity() == null) {
        // Determines segmentGranularity from the segmentsToCompact
        // Each batch of segmentToCompact from CompactionSegmentIterator will contain the same interval as
        // segmentGranularity is not set in the compaction config
        Interval interval = segmentsToCompact.get(0).getInterval();
        if (segmentsToCompact.stream().allMatch(segment -> interval.overlaps(segment.getInterval()))) {
          try {
            segmentGranularityToUse = GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity();
          }
          catch (IllegalArgumentException iae) {
            // This case can happen if the existing segment interval result in complicated periods.
            // Fall back to setting segmentGranularity as null
            LOG.warn("Cannot determine segmentGranularity from interval[%s].", interval);
          }
        } else {
          LOG.warn(
              "Not setting 'segmentGranularity' for auto-compaction task as"
              + " the segments to compact do not have the same interval."
          );
        }
      } else {
        segmentGranularityToUse = config.getGranularitySpec().getSegmentGranularity();
      }
      final ClientCompactionTaskGranularitySpec granularitySpec = new ClientCompactionTaskGranularitySpec(
          segmentGranularityToUse,
          config.getGranularitySpec() != null ? config.getGranularitySpec().getQueryGranularity() : null,
          config.getGranularitySpec() != null ? config.getGranularitySpec().isRollup() : null
      );

      // Create dimensionsSpec to send to compaction task
      final ClientCompactionTaskDimensionsSpec dimensionsSpec;
      if (config.getDimensionsSpec() != null) {
        dimensionsSpec = new ClientCompactionTaskDimensionsSpec(
            config.getDimensionsSpec().getDimensions()
        );
      } else {
        dimensionsSpec = null;
      }

      // Create transformSpec to send to compaction task
      ClientCompactionTaskTransformSpec transformSpec = null;
      if (config.getTransformSpec() != null) {
        transformSpec = new ClientCompactionTaskTransformSpec(
            config.getTransformSpec().getFilter()
        );
      }

      Boolean dropExisting = null;
      if (config.getIoConfig() != null) {
        dropExisting = config.getIoConfig().isDropExisting();
      }

      // If all the segments found to be compacted are tombstones then dropExisting
      // needs to be forced to true. This forcing needs to  happen in the case that
      // the flag is null, or it is false. It is needed when it is null to avoid the
      // possibility of the code deciding to default it to false later.
      // Forcing the flag to true will enable the task ingestion code to generate new, compacted, tombstones to
      // cover the tombstones found to be compacted as well as to mark them
      // as compacted (update their lastCompactionState). If we don't force the
      // flag then every time this compact duty runs it will find the same tombstones
      // in the interval since their lastCompactionState
      // was not set repeating this over and over and the duty will not make progress; it
      // will become stuck on this set of tombstones.
      // This forcing code should be revised
      // when/if the autocompaction code policy to decide which segments to compact changes
      if (dropExisting == null || !dropExisting) {
        if (segmentsToCompact.stream().allMatch(DataSegment::isTombstone)) {
          dropExisting = true;
          LOG.info("Forcing dropExisting to true since all segments to compact are tombstones.");
        }
      }

      final CompactionEngine compactionEngine = config.getEngine() == null ? defaultEngine : config.getEngine();
      final Map<String, Object> autoCompactionContext = newAutoCompactionContext(config.getTaskContext());
      int slotsRequiredForCurrentTask;

      if (compactionEngine == CompactionEngine.MSQ) {
        if (autoCompactionContext.containsKey(ClientMSQContext.CTX_MAX_NUM_TASKS)) {
          slotsRequiredForCurrentTask = (int) autoCompactionContext.get(ClientMSQContext.CTX_MAX_NUM_TASKS);
        } else {
          // Since MSQ needs all task slots for the calculated #tasks to be available upfront, allot all available
          // compaction slots (upto a max of MAX_TASK_SLOTS_FOR_MSQ_COMPACTION) to current compaction task to avoid
          // stalling. Setting "taskAssignment" to "auto" has the problem of not being able to determine the actual
          // count, which is required for subsequent tasks.
          slotsRequiredForCurrentTask = Math.min(
              // Update the slots to 2 (min required for MSQ) if only 1 slot is available.
              numAvailableCompactionTaskSlots == 1 ? 2 : numAvailableCompactionTaskSlots,
              ClientMSQContext.MAX_TASK_SLOTS_FOR_MSQ_COMPACTION_TASK
          );
          autoCompactionContext.put(ClientMSQContext.CTX_MAX_NUM_TASKS, slotsRequiredForCurrentTask);
        }
      } else {
        slotsRequiredForCurrentTask = findMaxNumTaskSlotsUsedByOneNativeCompactionTask(config.getTuningConfig());
      }

      if (entry.getCurrentStatus() != null) {
        autoCompactionContext.put(COMPACTION_REASON_KEY, entry.getCurrentStatus().getReason());
      }

      final String taskId = compactSegments(
          entry,
          config.getTaskPriority(),
          ClientCompactionTaskQueryTuningConfig.from(
              config.getTuningConfig(),
              config.getMaxRowsPerSegment(),
              config.getMetricsSpec() != null
          ),
          granularitySpec,
          dimensionsSpec,
          config.getMetricsSpec(),
          transformSpec,
          dropExisting,
          autoCompactionContext,
          new ClientCompactionRunnerInfo(compactionEngine)
      );

      LOG.debug(
          "Submitted a compaction task[%s] for [%d] segments in datasource[%s], umbrella interval[%s].",
          taskId, segmentsToCompact.size(), dataSourceName, entry.getUmbrellaInterval()
      );
      LOG.debugSegments(segmentsToCompact, "Compacting segments");
      numSubmittedTasks++;
      totalTaskSlotsAssigned += slotsRequiredForCurrentTask;
    }

    LOG.info("Submitted a total of [%d] compaction tasks.", numSubmittedTasks);
    return numSubmittedTasks;
  }

  private Map<String, Object> newAutoCompactionContext(@Nullable Map<String, Object> configuredContext)
  {
    final Map<String, Object> newContext = configuredContext == null
                                           ? new HashMap<>()
                                           : new HashMap<>(configuredContext);
    newContext.put(STORE_COMPACTION_STATE_KEY, true);
    return newContext;
  }

  private void updateCompactionSnapshotStats(
      Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders,
      CompactionSegmentIterator iterator,
      CoordinatorRunStats stats
  )
  {
    // Mark all the segments remaining in the iterator as "awaiting compaction"
    while (iterator.hasNext()) {
      final CompactionCandidate entry = iterator.next();
      currentRunAutoCompactionSnapshotBuilders
          .computeIfAbsent(entry.getDataSource(), AutoCompactionSnapshot::builder)
          .incrementWaitingStats(entry.getStats());
    }

    // Statistics of all segments considered compacted after this run
    iterator.getCompactedSegments().forEach(
        candidateSegments -> currentRunAutoCompactionSnapshotBuilders
            .computeIfAbsent(candidateSegments.getDataSource(), AutoCompactionSnapshot::builder)
            .incrementCompactedStats(candidateSegments.getStats())
    );

    // Statistics of all segments considered skipped after this run
    iterator.getSkippedSegments().forEach(
        candidateSegments -> currentRunAutoCompactionSnapshotBuilders
            .computeIfAbsent(candidateSegments.getDataSource(), AutoCompactionSnapshot::builder)
            .incrementSkippedStats(candidateSegments.getStats())
    );

    final Map<String, AutoCompactionSnapshot> currentAutoCompactionSnapshotPerDataSource = new HashMap<>();
    currentRunAutoCompactionSnapshotBuilders.forEach((dataSource, builder) -> {
      final AutoCompactionSnapshot autoCompactionSnapshot = builder.build();
      currentAutoCompactionSnapshotPerDataSource.put(dataSource, autoCompactionSnapshot);
      collectSnapshotStats(autoCompactionSnapshot, stats);
    });

    // Atomic update of autoCompactionSnapshotPerDataSource with the latest from this coordinator run
    autoCompactionSnapshotPerDataSource.set(currentAutoCompactionSnapshotPerDataSource);
  }

  private void collectSnapshotStats(
      AutoCompactionSnapshot autoCompactionSnapshot,
      CoordinatorRunStats stats
  )
  {
    final RowKey rowKey = RowKey.of(Dimension.DATASOURCE, autoCompactionSnapshot.getDataSource());

    stats.add(Stats.Compaction.PENDING_BYTES, rowKey, autoCompactionSnapshot.getBytesAwaitingCompaction());
    stats.add(Stats.Compaction.PENDING_SEGMENTS, rowKey, autoCompactionSnapshot.getSegmentCountAwaitingCompaction());
    stats.add(Stats.Compaction.PENDING_INTERVALS, rowKey, autoCompactionSnapshot.getIntervalCountAwaitingCompaction());
    stats.add(Stats.Compaction.COMPACTED_BYTES, rowKey, autoCompactionSnapshot.getBytesCompacted());
    stats.add(Stats.Compaction.COMPACTED_SEGMENTS, rowKey, autoCompactionSnapshot.getSegmentCountCompacted());
    stats.add(Stats.Compaction.COMPACTED_INTERVALS, rowKey, autoCompactionSnapshot.getIntervalCountCompacted());
    stats.add(Stats.Compaction.SKIPPED_BYTES, rowKey, autoCompactionSnapshot.getBytesSkipped());
    stats.add(Stats.Compaction.SKIPPED_SEGMENTS, rowKey, autoCompactionSnapshot.getSegmentCountSkipped());
    stats.add(Stats.Compaction.SKIPPED_INTERVALS, rowKey, autoCompactionSnapshot.getIntervalCountSkipped());
  }

  @Nullable
  public AutoCompactionSnapshot getAutoCompactionSnapshot(String dataSource)
  {
    return autoCompactionSnapshotPerDataSource.get().get(dataSource);
  }

  public Map<String, AutoCompactionSnapshot> getAutoCompactionSnapshot()
  {
    return autoCompactionSnapshotPerDataSource.get();
  }

  private String compactSegments(
      CompactionCandidate entry,
      int compactionTaskPriority,
      ClientCompactionTaskQueryTuningConfig tuningConfig,
      ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable ClientCompactionTaskDimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      @Nullable ClientCompactionTaskTransformSpec transformSpec,
      @Nullable Boolean dropExisting,
      @Nullable Map<String, Object> context,
      ClientCompactionRunnerInfo compactionRunner
  )
  {
    final List<DataSegment> segments = entry.getSegments();
    Preconditions.checkArgument(!segments.isEmpty(), "Expect non-empty segments to compact");

    final String dataSource = segments.get(0).getDataSource();
    Preconditions.checkArgument(
        segments.stream().allMatch(segment -> segment.getDataSource().equals(dataSource)),
        "Segments must have the same dataSource"
    );

    context = context == null ? new HashMap<>() : context;
    context.put("priority", compactionTaskPriority);

    final String taskId = IdUtils.newTaskId(TASK_ID_PREFIX, ClientCompactionTaskQuery.TYPE, dataSource, null);
    final Granularity segmentGranularity = granularitySpec == null ? null : granularitySpec.getSegmentGranularity();
    final ClientCompactionTaskQuery taskPayload = new ClientCompactionTaskQuery(
        taskId,
        dataSource,
        new ClientCompactionIOConfig(
            ClientCompactionIntervalSpec.fromSegments(segments, segmentGranularity),
            dropExisting
        ),
        tuningConfig,
        granularitySpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        context,
        compactionRunner
    );
    FutureUtils.getUnchecked(overlordClient.runTask(taskId, taskPayload), true);
    statusTracker.onTaskSubmitted(taskPayload, entry);

    return taskId;
  }
}
