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

package org.apache.druid.server.coordinator.helper;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.client.indexing.ClientCompactQuery;
import org.apache.druid.client.indexing.ClientCompactQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.guava.Comparators;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DruidCoordinatorSegmentCompactor implements DruidCoordinatorHelper
{
  static final String COMPACT_TASK_COUNT = "compactTaskCount";
  static final String SEGMENT_SIZE_WAIT_COMPACT = "segmentSizeWaitCompact";

  // Should be synced with CompactionTask.TYPE
  private static final String COMPACT_TASK_TYPE = "compact";
  private static final Logger LOG = new Logger(DruidCoordinatorSegmentCompactor.class);

  private final CompactionSegmentSearchPolicy policy = new NewestSegmentFirstPolicy();
  private final IndexingServiceClient indexingServiceClient;

  private Object2LongMap<String> remainingSegmentSizeBytes;

  @Inject
  public DruidCoordinatorSegmentCompactor(IndexingServiceClient indexingServiceClient)
  {
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    LOG.info("Run coordinator segment compactor");

    final CoordinatorCompactionConfig dynamicConfig = params.getCoordinatorCompactionConfig();
    final CoordinatorStats stats = new CoordinatorStats();

    if (dynamicConfig.getMaxCompactionTaskSlots() > 0) {
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources = params.getDataSources();
      List<DataSourceCompactionConfig> compactionConfigList = dynamicConfig.getCompactionConfigs();

      if (compactionConfigList != null && !compactionConfigList.isEmpty()) {
        Map<String, DataSourceCompactionConfig> compactionConfigs = compactionConfigList
            .stream()
            .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));
        final List<TaskStatusPlus> compactTasks = filterNonCompactTasks(
            indexingServiceClient.getRunningTasks(),
            indexingServiceClient.getPendingTasks(),
            indexingServiceClient.getWaitingTasks()
        );
        // dataSource -> list of intervals of compact tasks
        final Map<String, List<Interval>> compactTaskIntervals = new HashMap<>(compactionConfigList.size());
        for (TaskStatusPlus status : compactTasks) {
          final TaskPayloadResponse response = indexingServiceClient.getTaskPayload(status.getId());
          if (response == null) {
            throw new ISE("WTH? got a null paylord from overlord for task[%s]", status.getId());
          }
          if (COMPACT_TASK_TYPE.equals(response.getPayload().getType())) {
            final ClientCompactQuery compactQuery = (ClientCompactQuery) response.getPayload();
            final Interval interval = JodaUtils.umbrellaInterval(
                compactQuery.getSegments()
                            .stream()
                            .map(DataSegment::getInterval)
                            .sorted(Comparators.intervalsByStartThenEnd())
                            .collect(Collectors.toList())
            );
            compactTaskIntervals.computeIfAbsent(status.getDataSource(), k -> new ArrayList<>()).add(interval);
          } else {
            throw new ISE("WTH? task[%s] is not a compactTask?", status.getId());
          }
        }

        final CompactionSegmentIterator iterator = policy.reset(compactionConfigs, dataSources, compactTaskIntervals);

        final int compactionTaskCapacity = (int) Math.min(
            indexingServiceClient.getTotalWorkerCapacity() * dynamicConfig.getCompactionTaskSlotRatio(),
            dynamicConfig.getMaxCompactionTaskSlots()
        );
        final int numNonCompleteCompactionTasks = compactTasks.size();
        final int numAvailableCompactionTaskSlots = numNonCompleteCompactionTasks > 0
                                                    ? Math.max(0, compactionTaskCapacity - numNonCompleteCompactionTasks)
                                                    // compactionTaskCapacity might be 0 if totalWorkerCapacity is low.
                                                    // This guarantees that at least one slot is available if
                                                    // compaction is enabled and numRunningCompactTasks is 0.
                                                    : Math.max(1, compactionTaskCapacity);
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

  @SafeVarargs
  private static List<TaskStatusPlus> filterNonCompactTasks(List<TaskStatusPlus>...taskStatusStreams)
  {
    final List<TaskStatusPlus> allTaskStatusPlus = new ArrayList<>();
    Arrays.stream(taskStatusStreams).forEach(allTaskStatusPlus::addAll);

    return allTaskStatusPlus
        .stream()
        .filter(status -> {
          final String taskType = status.getType();
          // taskType can be null if middleManagers are running with an older version. Here, we consevatively regard
          // the tasks of the unknown taskType as the compactionTask. This is because it's important to not run
          // compactionTasks more than the configured limit at any time which might impact to the ingestion
          // performance.
          return taskType == null || COMPACT_TASK_TYPE.equals(taskType);
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

    for (; iterator.hasNext() && numSubmittedTasks < numAvailableCompactionTaskSlots; numSubmittedTasks++) {
      final List<DataSegment> segmentsToCompact = iterator.next();

      final String dataSourceName = segmentsToCompact.get(0).getDataSource();

      if (segmentsToCompact.size() > 1) {
        final DataSourceCompactionConfig config = compactionConfigs.get(dataSourceName);
        // make tuningConfig
        final String taskId = indexingServiceClient.compactSegments(
            segmentsToCompact,
            config.isKeepSegmentGranularity(),
            config.getTargetCompactionSizeBytes(),
            config.getTaskPriority(),
            ClientCompactQueryTuningConfig.from(config.getTuningConfig(), config.getMaxRowsPerSegment()),
            config.getTaskContext()
        );
        LOG.info(
            "Submitted a compactTask[%s] for segments %s",
            taskId,
            Iterables.transform(segmentsToCompact, DataSegment::getId)
        );
      } else if (segmentsToCompact.size() == 1) {
        throw new ISE("Found one segments[%s] to compact", segmentsToCompact);
      } else {
        throw new ISE("Failed to find segments for dataSource[%s]", dataSourceName);
      }
    }

    return makeStats(numSubmittedTasks, iterator);
  }

  private CoordinatorStats makeStats(int numCompactionTasks, CompactionSegmentIterator iterator)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(COMPACT_TASK_COUNT, numCompactionTasks);
    remainingSegmentSizeBytes = iterator.remainingSegmentSizeBytes();
    iterator.remainingSegmentSizeBytes().object2LongEntrySet().fastForEach(
        entry -> {
          final String dataSource = entry.getKey();
          final long numSegmentsWaitCompact = entry.getLongValue();
          stats.addToDataSourceStat(SEGMENT_SIZE_WAIT_COMPACT, dataSource, numSegmentsWaitCompact);
        }
    );
    return stats;
  }

  @Nullable
  public long getRemainingSegmentSizeBytes(String dataSource)
  {
    return remainingSegmentSizeBytes.getLong(dataSource);
  }
}
