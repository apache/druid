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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.ClientCompactionTaskDimensionsSpec;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientCompactionTaskTransformSpec;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.GranularityType;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CompactionStatistics;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompactSegments implements CoordinatorCustomDuty
{
  static final String COMPACTION_TASK_COUNT = "compactTaskCount";
  static final String AVAILABLE_COMPACTION_TASK_SLOT = "availableCompactionTaskSlot";
  static final String MAX_COMPACTION_TASK_SLOT = "maxCompactionTaskSlot";

  static final String TOTAL_SIZE_OF_SEGMENTS_SKIPPED = "segmentSizeSkippedCompact";
  static final String TOTAL_COUNT_OF_SEGMENTS_SKIPPED = "segmentCountSkippedCompact";
  static final String TOTAL_INTERVAL_OF_SEGMENTS_SKIPPED = "segmentIntervalSkippedCompact";

  static final String TOTAL_SIZE_OF_SEGMENTS_AWAITING = "segmentSizeWaitCompact";
  static final String TOTAL_COUNT_OF_SEGMENTS_AWAITING = "segmentCountWaitCompact";
  static final String TOTAL_INTERVAL_OF_SEGMENTS_AWAITING = "segmentIntervalWaitCompact";

  static final String TOTAL_SIZE_OF_SEGMENTS_COMPACTED = "segmentSizeCompacted";
  static final String TOTAL_COUNT_OF_SEGMENTS_COMPACTED = "segmentCountCompacted";
  static final String TOTAL_INTERVAL_OF_SEGMENTS_COMPACTED = "segmentIntervalCompacted";

  /** Must be synced with org.apache.druid.indexing.common.task.CompactionTask.TYPE. */
  public static final String COMPACTION_TASK_TYPE = "compact";
  /** Must be synced with org.apache.druid.indexing.common.task.Tasks.STORE_COMPACTION_STATE_KEY */
  public static final String STORE_COMPACTION_STATE_KEY = "storeCompactionState";

  private static final Logger LOG = new Logger(CompactSegments.class);

  private final CompactionSegmentSearchPolicy policy;
  private final boolean skipLockedIntervals;
  private final IndexingServiceClient indexingServiceClient;

  // This variable is updated by the Coordinator thread executing duties and
  // read by HTTP threads processing Coordinator API calls.
  private final AtomicReference<Map<String, AutoCompactionSnapshot>> autoCompactionSnapshotPerDataSource = new AtomicReference<>();

  @Inject
  @JsonCreator
  public CompactSegments(
      @JacksonInject DruidCoordinatorConfig config,
      @JacksonInject ObjectMapper objectMapper,
      @JacksonInject IndexingServiceClient indexingServiceClient
  )
  {
    this.policy = new NewestSegmentFirstPolicy(objectMapper);
    this.indexingServiceClient = indexingServiceClient;
    this.skipLockedIntervals = config.getCompactionSkipLockedIntervals();
    autoCompactionSnapshotPerDataSource.set(new HashMap<>());

    LOG.info("Scheduling compaction with skipLockedIntervals [%s]", skipLockedIntervals);
  }

  @VisibleForTesting
  public boolean isSkipLockedIntervals()
  {
    return skipLockedIntervals;
  }

  @VisibleForTesting
  IndexingServiceClient getIndexingServiceClient()
  {
    return indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    LOG.info("Compact segments");

    final CoordinatorCompactionConfig dynamicConfig = params.getCoordinatorCompactionConfig();
    final CoordinatorStats stats = new CoordinatorStats();
    List<DataSourceCompactionConfig> compactionConfigList = dynamicConfig.getCompactionConfigs();
    if (dynamicConfig.getMaxCompactionTaskSlots() > 0) {
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources =
          params.getUsedSegmentsTimelinesPerDataSource();
      if (compactionConfigList != null && !compactionConfigList.isEmpty()) {
        Map<String, DataSourceCompactionConfig> compactionConfigs = compactionConfigList
            .stream()
            .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));
        final List<TaskStatusPlus> compactionTasks = filterNonCompactionTasks(indexingServiceClient.getActiveTasks());

        // dataSource -> list of intervals for which compaction will be skipped in this run
        final Map<String, List<Interval>> intervalsToSkipCompaction = new HashMap<>();

        int numEstimatedNonCompleteCompactionTasks = 0;
        for (TaskStatusPlus status : compactionTasks) {
          final TaskPayloadResponse response = indexingServiceClient.getTaskPayload(status.getId());
          if (response == null) {
            throw new ISE("Got a null paylord from overlord for task[%s]", status.getId());
          }
          if (COMPACTION_TASK_TYPE.equals(response.getPayload().getType())) {
            final ClientCompactionTaskQuery compactionTaskQuery = (ClientCompactionTaskQuery) response.getPayload();
            DataSourceCompactionConfig dataSourceCompactionConfig = compactionConfigs.get(status.getDataSource());
            if (dataSourceCompactionConfig != null && dataSourceCompactionConfig.getGranularitySpec() != null) {
              Granularity configuredSegmentGranularity = dataSourceCompactionConfig.getGranularitySpec().getSegmentGranularity();
              if (configuredSegmentGranularity != null
                  && compactionTaskQuery.getGranularitySpec() != null
                  && !configuredSegmentGranularity.equals(compactionTaskQuery.getGranularitySpec().getSegmentGranularity())) {
                // We will cancel active compaction task if segmentGranularity changes and we will need to
                // re-compact the interval
                LOG.info("Canceled task[%s] as task segmentGranularity is [%s] but compaction config "
                         + "segmentGranularity is [%s]",
                         status.getId(),
                         compactionTaskQuery.getGranularitySpec().getSegmentGranularity(),
                         configuredSegmentGranularity);
                indexingServiceClient.cancelTask(status.getId());
                continue;
              }
            }
            // Skip interval as the current active compaction task is good
            final Interval interval = compactionTaskQuery.getIoConfig().getInputSpec().getInterval();
            intervalsToSkipCompaction.computeIfAbsent(status.getDataSource(), k -> new ArrayList<>()).add(interval);
            // Since we keep the current active compaction task running, we count the active task slots
            numEstimatedNonCompleteCompactionTasks += findMaxNumTaskSlotsUsedByOneCompactionTask(
                compactionTaskQuery.getTuningConfig()
            );
          } else {
            throw new ISE("task[%s] is not a compactionTask", status.getId());
          }
        }

        // Skip all the intervals locked by higher priority tasks for each datasource
        // This must be done after the invalid compaction tasks are cancelled
        // in the loop above so that their intervals are not considered locked
        getLockedIntervalsToSkip(compactionConfigList).forEach(
            (dataSource, intervals) ->
                intervalsToSkipCompaction
                    .computeIfAbsent(dataSource, ds -> new ArrayList<>())
                    .addAll(intervals)
        );

        final CompactionSegmentIterator iterator =
            policy.reset(compactionConfigs, dataSources, intervalsToSkipCompaction);

        int totalCapacity;
        if (dynamicConfig.isUseAutoScaleSlots()) {
          try {
            totalCapacity = indexingServiceClient.getTotalWorkerCapacityWithAutoScale();
          }
          catch (Exception e) {
            LOG.warn("Failed to get total worker capacity with auto scale slots. Falling back to current capacity count");
            totalCapacity = indexingServiceClient.getTotalWorkerCapacity();
          }
        } else {
          totalCapacity = indexingServiceClient.getTotalWorkerCapacity();
        }

        final int compactionTaskCapacity = (int) Math.min(
            totalCapacity * dynamicConfig.getCompactionTaskSlotRatio(),
            dynamicConfig.getMaxCompactionTaskSlots()
        );
        final int numAvailableCompactionTaskSlots;
        if (numEstimatedNonCompleteCompactionTasks > 0) {
          numAvailableCompactionTaskSlots = Math.max(
              0,
              compactionTaskCapacity - numEstimatedNonCompleteCompactionTasks
          );
        } else {
          // compactionTaskCapacity might be 0 if totalWorkerCapacity is low.
          // This guarantees that at least one slot is available if
          // compaction is enabled and numEstimatedNonCompleteCompactionTasks is 0.
          numAvailableCompactionTaskSlots = Math.max(1, compactionTaskCapacity);
        }

        LOG.info(
            "Found [%d] available task slots for compaction out of [%d] max compaction task capacity",
            numAvailableCompactionTaskSlots,
            compactionTaskCapacity
        );
        stats.addToGlobalStat(AVAILABLE_COMPACTION_TASK_SLOT, numAvailableCompactionTaskSlots);
        stats.addToGlobalStat(MAX_COMPACTION_TASK_SLOT, compactionTaskCapacity);
        final Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders = new HashMap<>();
        if (numAvailableCompactionTaskSlots > 0) {
          stats.accumulate(
              doRun(
                  compactionConfigs,
                  currentRunAutoCompactionSnapshotBuilders,
                  numAvailableCompactionTaskSlots,
                  iterator
              )
          );
        } else {
          stats.accumulate(makeStats(currentRunAutoCompactionSnapshotBuilders, 0, iterator));
        }
      } else {
        LOG.info("compactionConfig is empty. Skip.");
        autoCompactionSnapshotPerDataSource.set(new HashMap<>());
      }
    } else {
      LOG.info("maxCompactionTaskSlots was set to 0. Skip compaction");
      autoCompactionSnapshotPerDataSource.set(new HashMap<>());
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  /**
   * Gets a List of Intervals locked by higher priority tasks for each datasource.
   * Since compaction tasks submitted for these Intervals would have to wait anyway,
   * we skip these Intervals until the next compaction run.
   * <p>
   * For now, Segment Locks are being treated the same as Time Chunk Locks even
   * though they lock only a Segment and not the entire Interval. Thus,
   * a compaction task will not be submitted for an Interval if
   * <ul>
   *   <li>either the whole Interval is locked by a higher priority Task</li>
   *   <li>or there is atleast one Segment in the Interval that is locked by a
   *   higher priority Task</li>
   * </ul>
   */
  private Map<String, List<Interval>> getLockedIntervalsToSkip(
      List<DataSourceCompactionConfig> compactionConfigs
  )
  {
    if (!skipLockedIntervals) {
      LOG.info("Not skipping any locked interval for Compaction");
      return new HashMap<>();
    }

    final Map<String, Integer> minTaskPriority = compactionConfigs
        .stream()
        .collect(
            Collectors.toMap(
                DataSourceCompactionConfig::getDataSource,
                DataSourceCompactionConfig::getTaskPriority
            )
        );
    final Map<String, List<Interval>> datasourceToLockedIntervals =
        new HashMap<>(indexingServiceClient.getLockedIntervals(minTaskPriority));
    LOG.debug(
        "Skipping the following intervals for Compaction as they are currently locked: %s",
        datasourceToLockedIntervals
    );

    return datasourceToLockedIntervals;
  }

  /**
   * Returns the maximum number of task slots used by one compaction task at any time when the task is issued with
   * the given tuningConfig.
   */
  @VisibleForTesting
  static int findMaxNumTaskSlotsUsedByOneCompactionTask(@Nullable ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    if (isParallelMode(tuningConfig)) {
      @Nullable Integer maxNumConcurrentSubTasks = tuningConfig.getMaxNumConcurrentSubTasks();
      // Max number of task slots used in parallel mode = maxNumConcurrentSubTasks + 1 (supervisor task)
      return (maxNumConcurrentSubTasks == null ? 1 : maxNumConcurrentSubTasks) + 1;
    } else {
      return 1;
    }
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

  private static List<TaskStatusPlus> filterNonCompactionTasks(List<TaskStatusPlus> taskStatuses)
  {
    return taskStatuses
        .stream()
        .filter(status -> {
          final String taskType = status.getType();
          // taskType can be null if middleManagers are running with an older version. Here, we consevatively regard
          // the tasks of the unknown taskType as the compactionTask. This is because it's important to not run
          // compactionTasks more than the configured limit at any time which might impact to the ingestion
          // performance.
          return taskType == null || COMPACTION_TASK_TYPE.equals(taskType);
        })
        .collect(Collectors.toList());
  }

  private CoordinatorStats doRun(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders,
      int numAvailableCompactionTaskSlots,
      CompactionSegmentIterator iterator
  )
  {
    int numSubmittedTasks = 0;
    int numCompactionTasksAndSubtasks = 0;

    while (iterator.hasNext() && numCompactionTasksAndSubtasks < numAvailableCompactionTaskSlots) {
      final List<DataSegment> segmentsToCompact = iterator.next();

      if (!segmentsToCompact.isEmpty()) {
        final String dataSourceName = segmentsToCompact.get(0).getDataSource();
        // As these segments will be compacted, we will aggregates the statistic to the Compacted statistics

        AutoCompactionSnapshot.Builder snapshotBuilder = currentRunAutoCompactionSnapshotBuilders.computeIfAbsent(
            dataSourceName,
            k -> new AutoCompactionSnapshot.Builder(k, AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING)
        );
        snapshotBuilder.incrementBytesCompacted(segmentsToCompact.stream().mapToLong(DataSegment::getSize).sum());
        snapshotBuilder.incrementIntervalCountCompacted(segmentsToCompact.stream().map(DataSegment::getInterval).distinct().count());
        snapshotBuilder.incrementSegmentCountCompacted(segmentsToCompact.size());

        final DataSourceCompactionConfig config = compactionConfigs.get(dataSourceName);

        // Create granularitySpec to send to compaction task
        ClientCompactionTaskGranularitySpec granularitySpec;
        Granularity segmentGranularityToUse = null;
        if (config.getGranularitySpec() == null || config.getGranularitySpec().getSegmentGranularity() == null) {
          // Determines segmentGranularity from the segmentsToCompact
          // Each batch of segmentToCompact from CompactionSegmentIterator will contains the same interval as
          // segmentGranularity is not set in the compaction config
          Interval interval = segmentsToCompact.get(0).getInterval();
          if (segmentsToCompact.stream().allMatch(segment -> interval.overlaps(segment.getInterval()))) {
            try {
              segmentGranularityToUse = GranularityType.fromPeriod(interval.toPeriod()).getDefaultGranularity();
            }
            catch (IllegalArgumentException iae) {
              // This case can happen if the existing segment interval result in complicated periods.
              // Fall back to setting segmentGranularity as null
              LOG.warn("Cannot determine segmentGranularity from interval [%s]", interval);
            }
          } else {
            LOG.warn("segmentsToCompact does not have the same interval. Fallback to not setting segmentGranularity for auto compaction task");
          }
        } else {
          segmentGranularityToUse = config.getGranularitySpec().getSegmentGranularity();
        }
        granularitySpec = new ClientCompactionTaskGranularitySpec(
            segmentGranularityToUse,
            config.getGranularitySpec() != null ? config.getGranularitySpec().getQueryGranularity() : null,
            config.getGranularitySpec() != null ? config.getGranularitySpec().isRollup() : null

        );

        // Create dimensionsSpec to send to compaction task
        ClientCompactionTaskDimensionsSpec dimensionsSpec;
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
          if (segmentsToCompact.stream().allMatch(dataSegment -> dataSegment.isTombstone())) {
            dropExisting = true;
            LOG.info("Forcing dropExisting to %s since all segments to compact are tombstones", dropExisting);
          }
        }

        // make tuningConfig
        final String taskId = indexingServiceClient.compactSegments(
            "coordinator-issued",
            segmentsToCompact,
            config.getTaskPriority(),
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment(), config.getMetricsSpec() != null),
            granularitySpec,
            dimensionsSpec,
            config.getMetricsSpec(),
            transformSpec,
            dropExisting,
            newAutoCompactionContext(config.getTaskContext())
        );

        LOG.info(
            "Submitted a compactionTask[%s] for %s segments",
            taskId,
            segmentsToCompact.size()
        );
        LOG.infoSegments(segmentsToCompact, "Compacting segments");
        // Count the compaction task itself + its sub tasks
        numSubmittedTasks++;
        numCompactionTasksAndSubtasks += findMaxNumTaskSlotsUsedByOneCompactionTask(config.getTuningConfig());
      } else {
        throw new ISE("segmentsToCompact is empty?");
      }
    }

    return makeStats(currentRunAutoCompactionSnapshotBuilders, numSubmittedTasks, iterator);
  }

  private Map<String, Object> newAutoCompactionContext(@Nullable Map<String, Object> configuredContext)
  {
    final Map<String, Object> newContext = configuredContext == null
                                           ? new HashMap<>()
                                           : new HashMap<>(configuredContext);
    newContext.put(STORE_COMPACTION_STATE_KEY, true);
    return newContext;
  }

  private CoordinatorStats makeStats(
      Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders,
      int numCompactionTasks,
      CompactionSegmentIterator iterator
  )
  {
    final Map<String, AutoCompactionSnapshot> currentAutoCompactionSnapshotPerDataSource = new HashMap<>();
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(COMPACTION_TASK_COUNT, numCompactionTasks);

    // Iterate through all the remaining segments in the iterator.
    // As these segments could be compacted but were not compacted due to lack of task slot, we will aggregates
    // the statistic to the AwaitingCompaction statistics
    while (iterator.hasNext()) {
      final List<DataSegment> segmentsToCompact = iterator.next();
      if (!segmentsToCompact.isEmpty()) {
        final String dataSourceName = segmentsToCompact.get(0).getDataSource();
        AutoCompactionSnapshot.Builder snapshotBuilder = currentRunAutoCompactionSnapshotBuilders.computeIfAbsent(
            dataSourceName,
            k -> new AutoCompactionSnapshot.Builder(k, AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING)
        );
        snapshotBuilder.incrementBytesAwaitingCompaction(
            segmentsToCompact.stream()
                             .mapToLong(DataSegment::getSize)
                             .sum()
        );
        snapshotBuilder.incrementIntervalCountAwaitingCompaction(
            segmentsToCompact.stream()
                             .map(DataSegment::getInterval)
                             .distinct()
                             .count()
        );
        snapshotBuilder.incrementSegmentCountAwaitingCompaction(segmentsToCompact.size());
      }
    }

    // Statistics of all segments considered compacted after this run
    Map<String, CompactionStatistics> allCompactedStatistics = iterator.totalCompactedStatistics();
    for (Map.Entry<String, CompactionStatistics> compactionStatisticsEntry : allCompactedStatistics.entrySet()) {
      final String dataSource = compactionStatisticsEntry.getKey();
      final CompactionStatistics dataSourceCompactedStatistics = compactionStatisticsEntry.getValue();
      AutoCompactionSnapshot.Builder builder = currentRunAutoCompactionSnapshotBuilders.computeIfAbsent(
          dataSource,
          k -> new AutoCompactionSnapshot.Builder(k, AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING)
      );
      builder.incrementBytesCompacted(dataSourceCompactedStatistics.getByteSum());
      builder.incrementSegmentCountCompacted(dataSourceCompactedStatistics.getSegmentNumberCountSum());
      builder.incrementIntervalCountCompacted(dataSourceCompactedStatistics.getSegmentIntervalCountSum());
    }

    // Statistics of all segments considered skipped after this run
    Map<String, CompactionStatistics> allSkippedStatistics = iterator.totalSkippedStatistics();
    for (Map.Entry<String, CompactionStatistics> compactionStatisticsEntry : allSkippedStatistics.entrySet()) {
      final String dataSource = compactionStatisticsEntry.getKey();
      final CompactionStatistics dataSourceSkippedStatistics = compactionStatisticsEntry.getValue();
      AutoCompactionSnapshot.Builder builder = currentRunAutoCompactionSnapshotBuilders.computeIfAbsent(
          dataSource,
          k -> new AutoCompactionSnapshot.Builder(k, AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING)
      );
      builder.incrementBytesSkipped(dataSourceSkippedStatistics.getByteSum());
      builder.incrementSegmentCountSkipped(dataSourceSkippedStatistics.getSegmentNumberCountSum());
      builder.incrementIntervalCountSkipped(dataSourceSkippedStatistics.getSegmentIntervalCountSum());
    }

    for (Map.Entry<String, AutoCompactionSnapshot.Builder> autoCompactionSnapshotBuilderEntry : currentRunAutoCompactionSnapshotBuilders.entrySet()) {
      final String dataSource = autoCompactionSnapshotBuilderEntry.getKey();
      final AutoCompactionSnapshot.Builder builder = autoCompactionSnapshotBuilderEntry.getValue();

      // Build the complete snapshot for the datasource
      AutoCompactionSnapshot autoCompactionSnapshot = builder.build();
      currentAutoCompactionSnapshotPerDataSource.put(dataSource, autoCompactionSnapshot);

      // Use the complete snapshot to emits metrics
      stats.addToDataSourceStat(
          TOTAL_SIZE_OF_SEGMENTS_AWAITING,
          dataSource,
          autoCompactionSnapshot.getBytesAwaitingCompaction()
      );
      stats.addToDataSourceStat(
          TOTAL_COUNT_OF_SEGMENTS_AWAITING,
          dataSource,
          autoCompactionSnapshot.getSegmentCountAwaitingCompaction()
      );
      stats.addToDataSourceStat(
          TOTAL_INTERVAL_OF_SEGMENTS_AWAITING,
          dataSource,
          autoCompactionSnapshot.getIntervalCountAwaitingCompaction()
      );
      stats.addToDataSourceStat(
          TOTAL_SIZE_OF_SEGMENTS_COMPACTED,
          dataSource,
          autoCompactionSnapshot.getBytesCompacted()
      );
      stats.addToDataSourceStat(
          TOTAL_COUNT_OF_SEGMENTS_COMPACTED,
          dataSource,
          autoCompactionSnapshot.getSegmentCountCompacted()
      );
      stats.addToDataSourceStat(
          TOTAL_INTERVAL_OF_SEGMENTS_COMPACTED,
          dataSource,
          autoCompactionSnapshot.getIntervalCountCompacted()
      );
      stats.addToDataSourceStat(
          TOTAL_SIZE_OF_SEGMENTS_SKIPPED,
          dataSource,
          autoCompactionSnapshot.getBytesSkipped()
      );
      stats.addToDataSourceStat(
          TOTAL_COUNT_OF_SEGMENTS_SKIPPED,
          dataSource,
          autoCompactionSnapshot.getSegmentCountSkipped()
      );
      stats.addToDataSourceStat(
          TOTAL_INTERVAL_OF_SEGMENTS_SKIPPED,
          dataSource,
          autoCompactionSnapshot.getIntervalCountSkipped()
      );
    }

    // Atomic update of autoCompactionSnapshotPerDataSource with the latest from this coordinator run
    autoCompactionSnapshotPerDataSource.set(currentAutoCompactionSnapshotPerDataSource);

    return stats;
  }

  @Nullable
  public Long getTotalSizeOfSegmentsAwaitingCompaction(String dataSource)
  {
    AutoCompactionSnapshot autoCompactionSnapshot = autoCompactionSnapshotPerDataSource.get().get(dataSource);
    if (autoCompactionSnapshot == null) {
      return null;
    }
    return autoCompactionSnapshot.getBytesAwaitingCompaction();
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
}
