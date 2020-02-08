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
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
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
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompactSegments implements CoordinatorDuty
{
  static final String COMPACTION_TASK_COUNT = "compactTaskCount";
  static final String TOTAL_SIZE_OF_SEGMENTS_AWAITING_COMPACTION = "segmentSizeWaitCompact";

  /** Must be synced with org.apache.druid.indexing.common.task.CompactionTask.TYPE. */
  public static final String COMPACTION_TASK_TYPE = "compact";
  /** Must be synced with org.apache.druid.indexing.common.task.Tasks.STORE_COMPACTION_STATE_KEY */
  public static final String STORE_COMPACTION_STATE_KEY = "storeCompactionState";

  private static final Logger LOG = new Logger(CompactSegments.class);

  private final CompactionSegmentSearchPolicy policy;
  private final IndexingServiceClient indexingServiceClient;

  private Object2LongOpenHashMap<String> totalSizesOfSegmentsAwaitingCompactionPerDataSource;

  @Inject
  public CompactSegments(
      ObjectMapper objectMapper,
      IndexingServiceClient indexingServiceClient
  )
  {
    this.policy = new NewestSegmentFirstPolicy(objectMapper);
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    LOG.info("Compact segments");

    final CoordinatorCompactionConfig dynamicConfig = params.getCoordinatorCompactionConfig();
    final CoordinatorStats stats = new CoordinatorStats();

    if (dynamicConfig.getMaxCompactionTaskSlots() > 0) {
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources =
          params.getUsedSegmentsTimelinesPerDataSource();
      List<DataSourceCompactionConfig> compactionConfigList = dynamicConfig.getCompactionConfigs();

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
            throw new ISE("WTH? got a null paylord from overlord for task[%s]", status.getId());
          }
          if (COMPACTION_TASK_TYPE.equals(response.getPayload().getType())) {
            final ClientCompactionTaskQuery compactionTaskQuery = (ClientCompactionTaskQuery) response.getPayload();
            final Interval interval = compactionTaskQuery.getIoConfig().getInputSpec().getInterval();
            compactionTaskIntervals.computeIfAbsent(status.getDataSource(), k -> new ArrayList<>()).add(interval);
            final int numSubTasks = findNumMaxConcurrentSubTasks(compactionTaskQuery.getTuningConfig());
            numEstimatedNonCompleteCompactionTasks += numSubTasks + 1; // count the compaction task itself
          } else {
            throw new ISE("WTH? task[%s] is not a compactionTask?", status.getId());
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
        if (numAvailableCompactionTaskSlots > 0) {
          stats.accumulate(doRun(compactionConfigs, numAvailableCompactionTaskSlots, iterator));
        } else {
          stats.accumulate(makeStats(0, iterator));
        }
      } else {
        LOG.info("compactionConfig is empty. Skip.");
      }
    } else {
      LOG.info("maxCompactionTaskSlots was set to 0. Skip compaction");
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
            segmentsToCompact,
            config.getTaskPriority(),
            ClientCompactionTaskQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment()),
            newAutoCompactionContext(config.getTaskContext())
        );
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

    return makeStats(numSubmittedTasks, iterator);
  }

  private Map<String, Object> newAutoCompactionContext(@Nullable Map<String, Object> configuredContext)
  {
    final Map<String, Object> newContext = configuredContext == null
                                           ? new HashMap<>()
                                           : new HashMap<>(configuredContext);
    newContext.put(STORE_COMPACTION_STATE_KEY, true);
    return newContext;
  }

  private CoordinatorStats makeStats(int numCompactionTasks, CompactionSegmentIterator iterator)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(COMPACTION_TASK_COUNT, numCompactionTasks);
    totalSizesOfSegmentsAwaitingCompactionPerDataSource = iterator.totalRemainingSegmentsSizeBytes();
    totalSizesOfSegmentsAwaitingCompactionPerDataSource.object2LongEntrySet().fastForEach(
        entry -> {
          final String dataSource = entry.getKey();
          final long totalSizeOfSegmentsAwaitingCompaction = entry.getLongValue();
          stats.addToDataSourceStat(
              TOTAL_SIZE_OF_SEGMENTS_AWAITING_COMPACTION,
              dataSource,
              totalSizeOfSegmentsAwaitingCompaction
          );
        }
    );
    return stats;
  }

  @SuppressWarnings("deprecation") // Intentionally using boxing get() to return null if dataSource is unknown
  @Nullable
  public Long getTotalSizeOfSegmentsAwaitingCompaction(String dataSource)
  {
    return totalSizesOfSegmentsAwaitingCompactionPerDataSource.get(dataSource);
  }
}
