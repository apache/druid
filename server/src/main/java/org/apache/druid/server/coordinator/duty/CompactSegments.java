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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.ClientCompactionTaskGranularitySpec;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CompactionStatistics;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompactSegments implements CoordinatorDuty
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
  private final IndexingServiceClient indexingServiceClient;

  // This variable is updated by the Coordinator thread executing duties and
  // read by HTTP threads processing Coordinator API calls.
  private final AtomicReference<Map<String, AutoCompactionSnapshot>> autoCompactionSnapshotPerDataSource = new AtomicReference<>();

  @Inject
  public CompactSegments(
      ObjectMapper objectMapper,
      IndexingServiceClient indexingServiceClient
  )
  {
    this.policy = new NewestSegmentFirstPolicy(objectMapper);
    this.indexingServiceClient = indexingServiceClient;
    autoCompactionSnapshotPerDataSource.set(new HashMap<>());
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    LOG.info("Compact segments");

    final CoordinatorCompactionConfig dynamicConfig = params.getCoordinatorCompactionConfig();
    final CoordinatorStats stats = new CoordinatorStats();
    final Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders = new HashMap<>();
    List<DataSourceCompactionConfig> compactionConfigList = dynamicConfig.getCompactionConfigs();
    updateAutoCompactionSnapshot(compactionConfigList, currentRunAutoCompactionSnapshotBuilders);
    if (dynamicConfig.getMaxCompactionTaskSlots() > 0) {
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources =
          params.getUsedSegmentsTimelinesPerDataSource();
      if (compactionConfigList != null && !compactionConfigList.isEmpty()) {
        Map<String, DataSourceCompactionConfig> compactionConfigs = compactionConfigList
            .stream()
            .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));
        final List<TaskStatusPlus> compactionTasks = filterNonCompactionTasks(indexingServiceClient.getActiveTasks());
        // dataSource -> list of intervals of compaction tasks
        final Map<String, List<Interval>> compactionTaskIntervals = Maps.newHashMapWithExpectedSize(
            compactionConfigList.size());
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
            compactionTaskIntervals.computeIfAbsent(status.getDataSource(), k -> new ArrayList<>()).add(interval);
            // Since we keep the current active compaction task running, we count the active task slots
            numEstimatedNonCompleteCompactionTasks += findMaxNumTaskSlotsUsedByOneCompactionTask(
                compactionTaskQuery.getTuningConfig()
            );
          } else {
            throw new ISE("task[%s] is not a compactionTask", status.getId());
          }
        }

        final CompactionSegmentIterator iterator =
            policy.reset(compactionConfigs, dataSources, compactionTaskIntervals);

        final int compactionTaskCapacity = (int) Math.min(
            indexingServiceClient.getTotalWorkerCapacity() * dynamicConfig.getCompactionTaskSlotRatio(),
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
        updateAutoCompactionSnapshotWhenNoCompactTaskScheduled(currentRunAutoCompactionSnapshotBuilders);
      }
    } else {
      LOG.info("maxCompactionTaskSlots was set to 0. Skip compaction");
      updateAutoCompactionSnapshotWhenNoCompactTaskScheduled(currentRunAutoCompactionSnapshotBuilders);
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
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
    return tuningConfig.getPartitionsSpec() instanceof SingleDimensionPartitionsSpec;
  }

  private void updateAutoCompactionSnapshot(
      List<DataSourceCompactionConfig> compactionConfigList,
      Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders)
  {

    Set<String> enabledDatasources = compactionConfigList.stream()
                                                         .map(dataSourceCompactionConfig -> dataSourceCompactionConfig.getDataSource())
                                                         .collect(Collectors.toSet());
    // Update AutoCompactionScheduleStatus for dataSource that now has auto compaction disabled
    for (Map.Entry<String, AutoCompactionSnapshot> snapshot : autoCompactionSnapshotPerDataSource.get().entrySet()) {
      if (!enabledDatasources.contains(snapshot.getKey())) {
        currentRunAutoCompactionSnapshotBuilders.computeIfAbsent(
            snapshot.getKey(),
            k -> new AutoCompactionSnapshot.Builder(k, AutoCompactionSnapshot.AutoCompactionScheduleStatus.NOT_ENABLED)
        );
      }
    }

    // Create and Update snapshot for dataSource that has auto compaction enabled
    for (String compactionConfigDataSource : enabledDatasources) {
      currentRunAutoCompactionSnapshotBuilders.computeIfAbsent(
          compactionConfigDataSource,
          k -> new AutoCompactionSnapshot.Builder(k, AutoCompactionSnapshot.AutoCompactionScheduleStatus.RUNNING)
      );
    }
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
        AutoCompactionSnapshot.Builder snapshotBuilder = currentRunAutoCompactionSnapshotBuilders.get(dataSourceName);
        snapshotBuilder.incrementBytesCompacted(segmentsToCompact.stream().mapToLong(DataSegment::getSize).sum());
        snapshotBuilder.incrementIntervalCountCompacted(segmentsToCompact.stream().map(DataSegment::getInterval).distinct().count());
        snapshotBuilder.incrementSegmentCountCompacted(segmentsToCompact.size());

        final DataSourceCompactionConfig config = compactionConfigs.get(dataSourceName);
        ClientCompactionTaskGranularitySpec queryGranularitySpec;
        if (config.getGranularitySpec() != null) {
          queryGranularitySpec = new ClientCompactionTaskGranularitySpec(
              config.getGranularitySpec().getSegmentGranularity(),
              config.getGranularitySpec().getQueryGranularity()
          );
        } else {
          queryGranularitySpec = null;
        }

        Boolean dropExisting = null;
        if (config.getIoConfig() != null) {
          dropExisting = config.getIoConfig().isDropExisting();
        }

        // make tuningConfig
        final String taskId = indexingServiceClient.compactSegments(
            "coordinator-issued",
            segmentsToCompact,
            config.getTaskPriority(),
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment()),
            queryGranularitySpec,
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

  /**
   * This method can be use to atomically update the snapshots in {@code autoCompactionSnapshotPerDataSource} when
   * no compaction task is schedule in this run. Currently, this method does not update compaction statistics
   * (bytes, interval count, segment count, etc) since we skip iterating through the segments and cannot get an update
   * on those statistics. Thus, this method only updates the schedule status and task list (compaction statistics
   * remains the same as the previous snapshot).
   */
  private void updateAutoCompactionSnapshotWhenNoCompactTaskScheduled(
      Map<String, AutoCompactionSnapshot.Builder> currentRunAutoCompactionSnapshotBuilders
  )
  {
    Map<String, AutoCompactionSnapshot> previousSnapshots = autoCompactionSnapshotPerDataSource.get();
    for (Map.Entry<String, AutoCompactionSnapshot.Builder> autoCompactionSnapshotBuilderEntry : currentRunAutoCompactionSnapshotBuilders.entrySet()) {
      final String dataSource = autoCompactionSnapshotBuilderEntry.getKey();
      AutoCompactionSnapshot previousSnapshot = previousSnapshots.get(dataSource);
      if (previousSnapshot != null) {
        autoCompactionSnapshotBuilderEntry.getValue().incrementBytesAwaitingCompaction(previousSnapshot.getBytesAwaitingCompaction());
        autoCompactionSnapshotBuilderEntry.getValue().incrementBytesCompacted(previousSnapshot.getBytesCompacted());
        autoCompactionSnapshotBuilderEntry.getValue().incrementBytesSkipped(previousSnapshot.getBytesSkipped());
        autoCompactionSnapshotBuilderEntry.getValue().incrementSegmentCountAwaitingCompaction(previousSnapshot.getSegmentCountAwaitingCompaction());
        autoCompactionSnapshotBuilderEntry.getValue().incrementSegmentCountCompacted(previousSnapshot.getSegmentCountCompacted());
        autoCompactionSnapshotBuilderEntry.getValue().incrementSegmentCountSkipped(previousSnapshot.getSegmentCountSkipped());
        autoCompactionSnapshotBuilderEntry.getValue().incrementIntervalCountAwaitingCompaction(previousSnapshot.getIntervalCountAwaitingCompaction());
        autoCompactionSnapshotBuilderEntry.getValue().incrementIntervalCountCompacted(previousSnapshot.getIntervalCountCompacted());
        autoCompactionSnapshotBuilderEntry.getValue().incrementIntervalCountSkipped(previousSnapshot.getIntervalCountSkipped());
      }
    }

    Map<String, AutoCompactionSnapshot> currentAutoCompactionSnapshotPerDataSource = Maps.transformValues(
        currentRunAutoCompactionSnapshotBuilders,
        AutoCompactionSnapshot.Builder::build
    );
    // Atomic update of autoCompactionSnapshotPerDataSource with the latest from this coordinator run
    autoCompactionSnapshotPerDataSource.set(currentAutoCompactionSnapshotPerDataSource);
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
        AutoCompactionSnapshot.Builder snapshotBuilder = currentRunAutoCompactionSnapshotBuilders.get(dataSourceName);
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
    // Statistics of all segments considered skipped after this run
    Map<String, CompactionStatistics> allSkippedStatistics = iterator.totalSkippedStatistics();

    for (Map.Entry<String, AutoCompactionSnapshot.Builder> autoCompactionSnapshotBuilderEntry : currentRunAutoCompactionSnapshotBuilders.entrySet()) {
      final String dataSource = autoCompactionSnapshotBuilderEntry.getKey();
      final AutoCompactionSnapshot.Builder builder = autoCompactionSnapshotBuilderEntry.getValue();
      CompactionStatistics dataSourceCompactedStatistics = allCompactedStatistics.get(dataSource);
      CompactionStatistics dataSourceSkippedStatistics = allSkippedStatistics.get(dataSource);

      if (dataSourceCompactedStatistics != null) {
        builder.incrementBytesCompacted(dataSourceCompactedStatistics.getByteSum());
        builder.incrementSegmentCountCompacted(dataSourceCompactedStatistics.getSegmentNumberCountSum());
        builder.incrementIntervalCountCompacted(dataSourceCompactedStatistics.getSegmentIntervalCountSum());
      }

      if (dataSourceSkippedStatistics != null) {
        builder.incrementBytesSkipped(dataSourceSkippedStatistics.getByteSum());
        builder.incrementSegmentCountSkipped(dataSourceSkippedStatistics.getSegmentNumberCountSum());
        builder.incrementIntervalCountSkipped(dataSourceSkippedStatistics.getSegmentIntervalCountSum());
      }

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
