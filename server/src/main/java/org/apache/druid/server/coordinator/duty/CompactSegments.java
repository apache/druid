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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionIOConfig;
import org.apache.druid.client.indexing.ClientCompactionIntervalSpec;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.client.indexing.ClientCompactionTaskDimensionsSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.CompactionSegmentIterator;
import org.apache.druid.server.compaction.CompactionSlotManager;
import org.apache.druid.server.compaction.CompactionSnapshotBuilder;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompactSegments implements CoordinatorCustomDuty
{
  /**
   * Must be the same as org.apache.druid.indexing.common.task.Tasks.STORE_COMPACTION_STATE_KEY
   */
  public static final String STORE_COMPACTION_STATE_KEY = "storeCompactionState";

  /**
   * Must be the same as org.apache.druid.indexing.common.task.Tasks.INDEXING_STATE_FINGERPRINT_KEY
   */
  public static final String INDEXING_STATE_FINGERPRINT_KEY = "indexingStateFingerprint";

  private static final String COMPACTION_REASON_KEY = "compactionReason";

  private static final Logger LOG = new Logger(CompactSegments.class);

  private static final String TASK_ID_PREFIX = "coordinator-issued";

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
    List<DataSourceCompactionConfig> compactionConfigList = dynamicConfig.getCompactionConfigs();
    if (compactionConfigList == null || compactionConfigList.isEmpty()) {
      resetCompactionSnapshot();
      statusTracker.resetActiveDatasources(Set.of());
      return;
    }

    Map<String, DataSourceCompactionConfig> compactionConfigs = compactionConfigList
        .stream()
        .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));
    statusTracker.resetActiveDatasources(compactionConfigs.keySet());

    final CompactionSlotManager slotManager = new CompactionSlotManager(
        overlordClient,
        statusTracker,
        dynamicConfig.clusterConfig()
    );
    stats.add(Stats.Compaction.MAX_SLOTS, slotManager.getNumAvailableTaskSlots());

    // Fetch currently running compaction tasks
    for (ClientCompactionTaskQuery compactionTaskQuery : slotManager.fetchRunningCompactionTasks()) {
      final String dataSource = compactionTaskQuery.getDataSource();
      DataSourceCompactionConfig dataSourceCompactionConfig = compactionConfigs.get(dataSource);
      if (slotManager.cancelTaskOnlyIfGranularityChanged(compactionTaskQuery, dataSourceCompactionConfig)) {
        stats.add(Stats.Compaction.CANCELLED_TASKS, RowKey.of(Dimension.DATASOURCE, dataSource), 1L);
      }
    }
    stats.add(Stats.Compaction.AVAILABLE_SLOTS, slotManager.getNumAvailableTaskSlots());

    slotManager.skipLockedIntervals(compactionConfigList);

    // Get iterator over segments to compact and submit compaction tasks
    final CompactionCandidateSearchPolicy policy = dynamicConfig.getCompactionPolicy();
    final CompactionSegmentIterator iterator = new PriorityBasedCompactionSegmentIterator(
        policy,
        compactionConfigs,
        dataSources.getUsedSegmentsTimelinesPerDataSource(),
        slotManager.getDatasourceIntervalsToSkipCompaction(),
        null
    );

    final CompactionSnapshotBuilder compactionSnapshotBuilder = new CompactionSnapshotBuilder(stats);
    final int numSubmittedCompactionTasks = submitCompactionTasks(
        compactionConfigs,
        compactionSnapshotBuilder,
        slotManager,
        iterator,
        defaultEngine
    );

    stats.add(Stats.Compaction.SUBMITTED_TASKS, numSubmittedCompactionTasks);
    updateCompactionSnapshotStats(compactionSnapshotBuilder, iterator, compactionConfigs);
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
   * Submits compaction tasks to the Overlord. Returns total number of tasks submitted.
   */
  private int submitCompactionTasks(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      CompactionSnapshotBuilder snapshotBuilder,
      CompactionSlotManager slotManager,
      CompactionSegmentIterator iterator,
      CompactionEngine defaultEngine
  )
  {
    if (slotManager.getNumAvailableTaskSlots() <= 0) {
      return 0;
    }

    int numSubmittedTasks = 0;
    int totalTaskSlotsAssigned = 0;

    while (iterator.hasNext() && totalTaskSlotsAssigned < slotManager.getNumAvailableTaskSlots()) {
      final CompactionCandidate entry = iterator.next();
      final String dataSourceName = entry.getDataSource();
      final DataSourceCompactionConfig config = compactionConfigs.get(dataSourceName);

      final CompactionCandidate.TaskState compactionTaskState = statusTracker.computeCompactionTaskState(entry);
      statusTracker.onCompactionTaskStateComputed(entry, compactionTaskState, config);

      switch (compactionTaskState) {
        case READY:
        case TASK_IN_PROGRESS:
          // As these segments will be compacted, we will aggregate the statistic to the Compacted statistics
          snapshotBuilder.addToComplete(entry);
          break;
        case RECENTLY_COMPLETED:
          snapshotBuilder.addToComplete(entry);
          break;
        default:
          throw DruidException.defensive("unexpected task state[%s]", compactionTaskState);
      }

      final ClientCompactionTaskQuery taskPayload = createCompactionTask(
          entry,
          config,
          defaultEngine,
          null,
          true
      );

      final String taskId = taskPayload.getId();
      FutureUtils.getUnchecked(overlordClient.runTask(taskId, taskPayload), true);
      statusTracker.onTaskSubmitted(taskId, entry);

      LOG.debug(
          "Submitted a compaction task[%s] for [%d] segments in datasource[%s], umbrella interval[%s].",
          taskId, entry.numSegments(), dataSourceName, entry.getUmbrellaInterval()
      );
      LOG.debugSegments(entry.getSegments(), "Compacting segments");
      numSubmittedTasks++;
      totalTaskSlotsAssigned += slotManager.computeSlotsRequiredForTask(taskPayload, config);
    }

    LOG.info("Submitted a total of [%d] compaction tasks.", numSubmittedTasks);
    return numSubmittedTasks;
  }

  /**
   * Creates a {@link ClientCompactionTaskQuery} which can be submitted to an
   * {@link OverlordClient} to start a compaction task.
   */
  public static ClientCompactionTaskQuery createCompactionTask(
      CompactionCandidate candidate,
      DataSourceCompactionConfig config,
      CompactionEngine defaultEngine,
      String indexingStateFingerprint,
      boolean storeCompactionStatePerSegment
  )
  {
    final List<DataSegment> segmentsToCompact = candidate.getSegments();

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
    if (CompactionEngine.MSQ.equals(compactionEngine) && !Boolean.TRUE.equals(dropExisting)) {
      dropExisting = true;
    }
    final Map<String, Object> autoCompactionContext = newAutoCompactionContext(config.getTaskContext());

    if (candidate.getEligibility().getReason() != null) {
      autoCompactionContext.put(COMPACTION_REASON_KEY, candidate.getEligibility().getReason());
    }

    autoCompactionContext.put(STORE_COMPACTION_STATE_KEY, storeCompactionStatePerSegment);
    autoCompactionContext.put(INDEXING_STATE_FINGERPRINT_KEY, indexingStateFingerprint);

    return compactSegments(
        candidate,
        config.getTaskPriority(),
        ClientCompactionTaskQueryTuningConfig.from(
            config.getTuningConfig(),
            config.getMaxRowsPerSegment(),
            config.getMetricsSpec() != null
        ),
        granularitySpec,
        dimensionsSpec,
        config.getMetricsSpec(),
        config.getTransformSpec(),
        config.getProjections(),
        dropExisting,
        autoCompactionContext,
        new ClientCompactionRunnerInfo(compactionEngine)
    );
  }

  private static Map<String, Object> newAutoCompactionContext(@Nullable Map<String, Object> configuredContext)
  {
    final Map<String, Object> newContext = configuredContext == null
                                           ? new HashMap<>()
                                           : new HashMap<>(configuredContext);
    newContext.put(STORE_COMPACTION_STATE_KEY, true);
    return newContext;
  }

  private void updateCompactionSnapshotStats(
      CompactionSnapshotBuilder snapshotBuilder,
      CompactionSegmentIterator iterator,
      Map<String, DataSourceCompactionConfig> datasourceToConfig
  )
  {
    // Mark all the segments remaining in the iterator as "awaiting compaction"
    while (iterator.hasNext()) {
      snapshotBuilder.addToPending(iterator.next());
    }
    iterator.getCompactedSegments().forEach(snapshotBuilder::addToComplete);
    iterator.getSkippedSegments().forEach(entry -> {
      statusTracker.onSkippedCandidate(entry, datasourceToConfig.get(entry.getDataSource()));
      snapshotBuilder.addToSkipped(entry);
    });

    // Atomic update of autoCompactionSnapshotPerDataSource with the latest from this coordinator run
    autoCompactionSnapshotPerDataSource.set(snapshotBuilder.build());
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

  private static ClientCompactionTaskQuery compactSegments(
      CompactionCandidate entry,
      int compactionTaskPriority,
      ClientCompactionTaskQueryTuningConfig tuningConfig,
      ClientCompactionTaskGranularitySpec granularitySpec,
      @Nullable ClientCompactionTaskDimensionsSpec dimensionsSpec,
      @Nullable AggregatorFactory[] metricsSpec,
      @Nullable CompactionTransformSpec transformSpec,
      @Nullable List<AggregateProjectionSpec> projectionSpecs,
      @Nullable Boolean dropExisting,
      Map<String, Object> context,
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

    context.put("priority", compactionTaskPriority);

    final String taskId = IdUtils.newTaskId(TASK_ID_PREFIX, ClientCompactionTaskQuery.TYPE, dataSource, null);
    final ClientCompactionIntervalSpec clientCompactionIntervalSpec;
    switch (entry.getMode()) {
      case FULL_COMPACTION:
        clientCompactionIntervalSpec = new ClientCompactionIntervalSpec(entry.getCompactionInterval(), null);
        break;
      default:
        throw DruidException.defensive("Unexpected compaction mode[%s]", entry.getMode());
    }

    return new ClientCompactionTaskQuery(
        taskId,
        dataSource,
        new ClientCompactionIOConfig(clientCompactionIntervalSpec, dropExisting),
        tuningConfig,
        granularitySpec,
        dimensionsSpec,
        metricsSpec,
        transformSpec,
        projectionSpecs,
        context,
        compactionRunner
    );
  }
}
