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
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
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

  private AtomicReference<Map<String, AutoCompactionSnapshot>> autoCompactionSnapshotPerDataSource = new AtomicReference<>();

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
            final Interval interval = compactionTaskQuery.getIoConfig().getInputSpec().getInterval();
            compactionTaskIntervals.computeIfAbsent(status.getDataSource(), k -> new ArrayList<>()).add(interval);
            final int numSubTasks = findNumMaxConcurrentSubTasks(compactionTaskQuery.getTuningConfig());
            numEstimatedNonCompleteCompactionTasks += numSubTasks + 1; // count the compaction task itself
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
              doRun(compactionConfigs, currentRunAutoCompactionSnapshotBuilders, numAvailableCompactionTaskSlots, iterator)
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
   * Each compaction task can run a parallel indexing task. When we count the number of current running
   * compaction tasks, we should count the sub tasks of the parallel indexing task as well. However, we currently
   * don't have a good way to get the number of current running sub tasks except poking each supervisor task,
   * which is complex to handle all kinds of failures. Here, we simply return {@code maxNumConcurrentSubTasks} instead
   * to estimate the number of sub tasks conservatively. This should be ok since it won't affect to the performance of
   * other ingestion types.
   */
  private int findNumMaxConcurrentSubTasks(@Nullable ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    if (tuningConfig != null && tuningConfig.getMaxNumConcurrentSubTasks() != null) {
      // The actual number of subtasks might be smaller than the configured max.
      // However, we use the max to simplify the estimation here.
      return tuningConfig.getMaxNumConcurrentSubTasks();
    } else {
      return 0;
    }
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

    for (; iterator.hasNext() && numSubmittedTasks < numAvailableCompactionTaskSlots;) {
      final List<DataSegment> segmentsToCompact = iterator.next();

      if (!segmentsToCompact.isEmpty()) {
        final String dataSourceName = segmentsToCompact.get(0).getDataSource();
        final DataSourceCompactionConfig config = compactionConfigs.get(dataSourceName);
        // make tuningConfig
        final String taskId = indexingServiceClient.compactSegments(
            "coordinator-issued",
            segmentsToCompact,
            config.getTaskPriority(),
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment()),
            newAutoCompactionContext(config.getTaskContext())
        );
        currentRunAutoCompactionSnapshotBuilders.get(dataSourceName).addScheduledTaskId(taskId);

        LOG.info(
            "Submitted a compactionTask[%s] for %s segments",
            taskId,
            segmentsToCompact.size()
        );
        LOG.infoSegments(segmentsToCompact, "Compacting segments");
        // Count the compaction task itself + its sub tasks
        numSubmittedTasks += findNumMaxConcurrentSubTasks(config.getTuningConfig()) + 1;
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
        autoCompactionSnapshotBuilderEntry.getValue().setBytesAwaitingCompaction(previousSnapshot.getBytesAwaitingCompaction());
        autoCompactionSnapshotBuilderEntry.getValue().setBytesCompacted(previousSnapshot.getBytesCompacted());
        autoCompactionSnapshotBuilderEntry.getValue().setBytesSkipped(previousSnapshot.getBytesSkipped());
        autoCompactionSnapshotBuilderEntry.getValue().setSegmentCountAwaitingCompaction(previousSnapshot.getSegmentCountAwaitingCompaction());
        autoCompactionSnapshotBuilderEntry.getValue().setSegmentCountCompacted(previousSnapshot.getSegmentCountCompacted());
        autoCompactionSnapshotBuilderEntry.getValue().setSegmentCountSkipped(previousSnapshot.getSegmentCountSkipped());
        autoCompactionSnapshotBuilderEntry.getValue().setIntervalCountAwaitingCompaction(previousSnapshot.getIntervalCountAwaitingCompaction());
        autoCompactionSnapshotBuilderEntry.getValue().setIntervalCountCompacted(previousSnapshot.getIntervalCountCompacted());
        autoCompactionSnapshotBuilderEntry.getValue().setIntervalCountSkipped(previousSnapshot.getIntervalCountSkipped());
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
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(COMPACTION_TASK_COUNT, numCompactionTasks);

    // Make sure that the iterator iterate through all the remaining segments so that we can get accurate and correct
    // statistics (remaining, skipped, compacted). The reason we have to do this explicitly here is because
    // earlier (when we are iterating to submit compaction tasks) we may have ran out of task slot and were not able
    // to iterate to the first segment that needs compaction for some datasource.
    iterator.flushAllSegments();
    // Statistics of all segments that still need compaction after this run
    Map<String, CompactionStatistics> allRemainingStatistics = iterator.totalRemainingStatistics();
    // Statistics of all segments considered compacted after this run
    Map<String, CompactionStatistics> allCompactedStatistics = iterator.totalCompactedStatistics();
    // Statistics of all segments considered skipped after this run
    Map<String, CompactionStatistics> allSkippedStatistics = iterator.totalSkippedStatistics();

    for (Map.Entry<String, AutoCompactionSnapshot.Builder> autoCompactionSnapshotBuilderEntry : currentRunAutoCompactionSnapshotBuilders.entrySet()) {
      final String dataSource = autoCompactionSnapshotBuilderEntry.getKey();
      CompactionStatistics remainingStatistics = allRemainingStatistics.get(dataSource);
      CompactionStatistics compactedStatistics = allCompactedStatistics.get(dataSource);
      CompactionStatistics skippedStatistics = allSkippedStatistics.get(dataSource);

      long byteAwaitingCompaction = 0;
      long segmentCountAwaitingCompaction = 0;
      long intervalCountAwaitingCompaction = 0;
      if (remainingStatistics != null) {
        // If null means that all segments are either compacted or skipped.
        // Hence, we can leave these set to default value of 0. If not null, we set it to the collected statistic.
        byteAwaitingCompaction = remainingStatistics.getByteSum();
        segmentCountAwaitingCompaction = remainingStatistics.getSegmentNumberCountSum();
        intervalCountAwaitingCompaction = remainingStatistics.getSegmentIntervalCountSum();
      }

      long byteCompacted = 0;
      long segmentCountCompacted = 0;
      long intervalCountCompacted = 0;
      if (compactedStatistics != null) {
        byteCompacted = compactedStatistics.getByteSum();
        segmentCountCompacted = compactedStatistics.getSegmentNumberCountSum();
        intervalCountCompacted = compactedStatistics.getSegmentIntervalCountSum();
      }

      long byteSkipped = 0;
      long segmentCountSkipped = 0;
      long intervalCountSkipped = 0;
      if (skippedStatistics != null) {
        byteSkipped = skippedStatistics.getByteSum();
        segmentCountSkipped = skippedStatistics.getSegmentNumberCountSum();
        intervalCountSkipped = skippedStatistics.getSegmentIntervalCountSum();
      }

      autoCompactionSnapshotBuilderEntry.getValue().setBytesAwaitingCompaction(byteAwaitingCompaction);
      autoCompactionSnapshotBuilderEntry.getValue().setBytesCompacted(byteCompacted);
      autoCompactionSnapshotBuilderEntry.getValue().setBytesSkipped(byteSkipped);
      autoCompactionSnapshotBuilderEntry.getValue().setSegmentCountAwaitingCompaction(segmentCountAwaitingCompaction);
      autoCompactionSnapshotBuilderEntry.getValue().setSegmentCountCompacted(segmentCountCompacted);
      autoCompactionSnapshotBuilderEntry.getValue().setSegmentCountSkipped(segmentCountSkipped);
      autoCompactionSnapshotBuilderEntry.getValue().setIntervalCountAwaitingCompaction(intervalCountAwaitingCompaction);
      autoCompactionSnapshotBuilderEntry.getValue().setIntervalCountCompacted(intervalCountCompacted);
      autoCompactionSnapshotBuilderEntry.getValue().setIntervalCountSkipped(intervalCountSkipped);


      stats.addToDataSourceStat(
          TOTAL_SIZE_OF_SEGMENTS_AWAITING,
          dataSource,
          byteAwaitingCompaction
      );
      stats.addToDataSourceStat(
          TOTAL_COUNT_OF_SEGMENTS_AWAITING,
          dataSource,
          segmentCountAwaitingCompaction
      );
      stats.addToDataSourceStat(
          TOTAL_INTERVAL_OF_SEGMENTS_AWAITING,
          dataSource,
          intervalCountAwaitingCompaction
      );
      stats.addToDataSourceStat(
          TOTAL_SIZE_OF_SEGMENTS_COMPACTED,
          dataSource,
          byteCompacted
      );
      stats.addToDataSourceStat(
          TOTAL_COUNT_OF_SEGMENTS_COMPACTED,
          dataSource,
          segmentCountCompacted
      );
      stats.addToDataSourceStat(
          TOTAL_INTERVAL_OF_SEGMENTS_COMPACTED,
          dataSource,
          intervalCountCompacted
      );
      stats.addToDataSourceStat(
          TOTAL_SIZE_OF_SEGMENTS_SKIPPED,
          dataSource,
          byteSkipped
      );
      stats.addToDataSourceStat(
          TOTAL_COUNT_OF_SEGMENTS_SKIPPED,
          dataSource,
          segmentCountSkipped
      );
      stats.addToDataSourceStat(
          TOTAL_INTERVAL_OF_SEGMENTS_SKIPPED,
          dataSource,
          intervalCountSkipped
      );
    }

    Map<String, AutoCompactionSnapshot> currentAutoCompactionSnapshotPerDataSource = Maps.transformValues(
        currentRunAutoCompactionSnapshotBuilders,
        AutoCompactionSnapshot.Builder::build
    );
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
