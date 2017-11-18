/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import com.google.inject.Inject;
import io.druid.client.indexing.IndexingServiceClient;
import io.druid.client.indexing.QueryStatus;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.coordinator.CoordinatorCompactionConfig;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DruidCoordinatorSegmentCompactor implements DruidCoordinatorHelper
{
  static final String COMPACT_TASK_COUNT = "compactTaskCount";
  static final String SEGMENTS_WAIT_COMPACT = "segmentsWaitCompact";

  private static final Logger LOG = new Logger(DruidCoordinatorSegmentCompactor.class);

  private final CompactionSegmentSearchPolicy policy = new NewestSegmentFirstPolicy();
  private final IndexingServiceClient indexingServiceClient;
  private final Set<String> queryIds = new HashSet<>();

  @Inject
  public DruidCoordinatorSegmentCompactor(IndexingServiceClient indexingServiceClient)
  {
    this.indexingServiceClient = indexingServiceClient;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    LOG.info("Run coordinator segment compactor");

    Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources = params.getDataSources();
    List<CoordinatorCompactionConfig> compactionConfigList = params.getCoordinatorDynamicConfig()
                                                                   .getCompactionConfigs();

    final CoordinatorStats stats = new CoordinatorStats();
    if (compactionConfigList != null && !compactionConfigList.isEmpty()) {
      Map<String, CoordinatorCompactionConfig> compactionConfigs = compactionConfigList
          .stream()
          .collect(Collectors.toMap(CoordinatorCompactionConfig::getDataSource, Function.identity()));
      checkAndClearQueries();
      policy.reset(compactionConfigs, dataSources);

      final int compactionTaskCapacity = (int) (indexingServiceClient.getTotalWorkerCapacity() *
                                                params.getCoordinatorDynamicConfig().getCompactionTaskSlotRatio());
      final int numAvailableCompactionTaskSlots = queryIds.size() > 0 ?
                                                  compactionTaskCapacity - queryIds.size() :
                                                  Math.max(1, compactionTaskCapacity - queryIds.size());
      if (numAvailableCompactionTaskSlots > 0) {
        stats.accumulate(doRun(compactionConfigs, numAvailableCompactionTaskSlots));
      } else {
        stats.accumulate(makeStats(0));
      }
    } else {
      LOG.info("compactionConfig is empty. Skip.");
      stats.accumulate(makeStats(0));
    }

    return params.buildFromExisting()
                 .withCoordinatorStats(stats)
                 .build();
  }

  private void checkAndClearQueries()
  {
    final Iterator<String> iterator = queryIds.iterator();
    while (iterator.hasNext()) {
      final String queryId = iterator.next();
      final QueryStatus queryStatus = indexingServiceClient.queryStatus(queryId);
      if (queryStatus.isComplete()) {
        iterator.remove();
      }
    }

    LOG.info("[%d] queries are running after cleanup", queryIds.size());
  }

  private CoordinatorStats doRun(
      Map<String, CoordinatorCompactionConfig> compactionConfigs,
      int numAvailableCompactionTaskSlots
  )
  {
    int numSubmittedCompactionTasks = 0;

    List<DataSegment> segmentsToCompact;
    while ((segmentsToCompact = policy.nextSegments()) != null &&
           numSubmittedCompactionTasks < numAvailableCompactionTaskSlots) {
      if (segmentsToCompact.isEmpty()) {
        break;
      }

      final String dataSourceName = segmentsToCompact.get(0).getDataSource();

      if (segmentsToCompact.size() > 1) {
        final CoordinatorCompactionConfig config = compactionConfigs.get(dataSourceName);
        final String queryId = indexingServiceClient.compactSegments(
            segmentsToCompact,
            config.getTaskPriority(),
            config.getTuningConfig(),
            config.getTaskContext()
        );
        if (!queryIds.add(queryId)) {
          throw new ISE("Duplicated queryId[%s]", queryId);
        }
        LOG.info("Submit a compactTask[%s] for segments[%s]", queryId, segmentsToCompact);

        if (++numSubmittedCompactionTasks == numAvailableCompactionTaskSlots) {
          break;
        }
      } else if (segmentsToCompact.size() == 1) {
        throw new ISE("Found one segments[%s] to compact", segmentsToCompact);
      } else {
        throw new ISE("Failed to find segments for dataSource[%s]", dataSourceName);
      }
    }

    LOG.info("Running tasks [%d/%d]", numSubmittedCompactionTasks, numAvailableCompactionTaskSlots);

    return makeStats(numSubmittedCompactionTasks);
  }

  private CoordinatorStats makeStats(int numCompactionTasks)
  {
    final CoordinatorStats stats = new CoordinatorStats();
    stats.addToGlobalStat(COMPACT_TASK_COUNT, numCompactionTasks);
    policy.remainingSegments().object2LongEntrySet().fastForEach(
        entry -> {
          final String dataSource = entry.getKey();
          final long numSegmentsWaitCompact = entry.getLongValue();
          stats.addToDataSourceStat(SEGMENTS_WAIT_COMPACT, dataSource, numSegmentsWaitCompact);
        }
    );
    return stats;
  }

  public static class SegmentsToCompact
  {
    private final List<DataSegment> segments;
    private final long byteSize;

    SegmentsToCompact(List<DataSegment> segments, long byteSize)
    {
      this.segments = segments;
      this.byteSize = byteSize;
    }

    public List<DataSegment> getSegments()
    {
      return segments;
    }

    public long getByteSize()
    {
      return byteSize;
    }
  }
}
