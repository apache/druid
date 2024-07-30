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

package org.apache.druid.server.compaction;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.http.CompactionConfigUpdateRequest;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simulates runs of auto-compaction duty to obtain the expected list of
 * compaction tasks that would be submitted by the actual compaction duty.
 */
public class CompactionRunSimulator
{
  private final CoordinatorCompactionConfig compactionConfig;
  private final CompactionStatusTracker statusTracker;
  private final Map<String, SegmentTimeline> datasourceTimelines;
  private final OverlordClient emptyOverlordClient = new EmptyOverlordClient();

  public CompactionRunSimulator(
      CompactionStatusTracker statusTracker,
      CoordinatorCompactionConfig compactionConfig,
      Map<String, SegmentTimeline> datasourceTimelines
  )
  {
    this.statusTracker = statusTracker;
    this.datasourceTimelines = datasourceTimelines;
    this.compactionConfig = compactionConfig;
  }

  /**
   * Simulates a run of the compact segments duty with the given config update
   * assuming unlimited compaction task slots.
   */
  public CompactionSimulateResult simulateRunWithConfigUpdate(
      CompactionConfigUpdateRequest updateRequest
  )
  {
    final CompactionConfigUpdateRequest updateWithUnlimitedSlots = new CompactionConfigUpdateRequest(
        1.0,
        Integer.MAX_VALUE,
        updateRequest.getUseAutoScaleSlots(),
        updateRequest.getCompactionEngine(),
        updateRequest.getCompactionPolicy()
    );
    final CoordinatorCompactionConfig configWithUnlimitedTaskSlots
        = CoordinatorCompactionConfig.from(compactionConfig, updateWithUnlimitedSlots);

    final List<List<Object>> tableOfCompactibleIntervals = new ArrayList<>();
    final List<List<Object>> tableOfSkippedIntervals = new ArrayList<>();
    final CompactionStatusTracker simulationStatusTracker = new CompactionStatusTracker(null)
    {
      @Override
      public CompactionStatus computeCompactionStatus(
          SegmentsToCompact candidateSegments,
          DataSourceCompactionConfig config
      )
      {
        return statusTracker.computeCompactionStatus(candidateSegments, config);
      }

      @Override
      public void onSegmentsSkipped(SegmentsToCompact candidateSegments, CompactionStatus status)
      {
        // Add a row for each skipped interval
        tableOfSkippedIntervals.add(
            Arrays.asList(
                candidateSegments.getDataSource(),
                candidateSegments.getUmbrellaInterval(),
                candidateSegments.size(),
                candidateSegments.getTotalBytes(),
                status.getReason()
            )
        );
      }

      @Override
      public void onTaskSubmitted(ClientCompactionTaskQuery taskPayload, SegmentsToCompact candidateSegments)
      {
        // Add a row for each task in order of submission
        final CompactionStatus status = candidateSegments.getCompactionStatus();
        final String reason = status == null ? "" : status.getReason();
        tableOfCompactibleIntervals.add(
            Arrays.asList(
                candidateSegments.getDataSource(),
                candidateSegments.getUmbrellaInterval(),
                candidateSegments.size(),
                candidateSegments.getTotalBytes(),
                CompactSegments.findMaxNumTaskSlotsUsedByOneNativeCompactionTask(taskPayload.getTuningConfig()),
                reason
            )
        );
      }
    };

    final CoordinatorRunStats stats = new CoordinatorRunStats();
    new CompactSegments(simulationStatusTracker, emptyOverlordClient).run(
        configWithUnlimitedTaskSlots,
        datasourceTimelines,
        stats
    );

    // Add header rows
    if (!tableOfCompactibleIntervals.isEmpty()) {
      tableOfCompactibleIntervals.add(
          0,
          Arrays.asList("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact")
      );
    }
    if (!tableOfSkippedIntervals.isEmpty()) {
      tableOfSkippedIntervals.add(
          0,
          Arrays.asList("dataSource", "interval", "numSegments", "bytes", "reasonToSkip")
      );
    }

    return new CompactionSimulateResult(tableOfCompactibleIntervals, tableOfSkippedIntervals);
  }

  /**
   * Dummy overlord client that returns empty results for all APIs.
   */
  private static class EmptyOverlordClient implements OverlordClient
  {
    @Override
    public ListenableFuture<URI> findCurrentLeader()
    {
      return null;
    }

    @Override
    public ListenableFuture<Void> runTask(String taskId, Object taskObject)
    {
      return Futures.immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> cancelTask(String taskId)
    {
      return Futures.immediateVoidFuture();
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(Collections.emptyIterator()));
    }

    @Override
    public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
    {
      return Futures.immediateFuture(Collections.emptyMap());
    }

    @Override
    public ListenableFuture<TaskStatusResponse> taskStatus(String taskId)
    {
      return null;
    }

    @Override
    public ListenableFuture<TaskReport.ReportMap> taskReportAsMap(String taskId)
    {
      return null;
    }

    @Override
    public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
    {
      return Futures.immediateFuture(null);
    }

    @Override
    public ListenableFuture<CloseableIterator<SupervisorStatus>> supervisorStatuses()
    {
      return null;
    }

    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
    {
      return Futures.immediateFuture(Collections.emptyMap());
    }

    @Override
    public ListenableFuture<Integer> killPendingSegments(String dataSource, Interval interval)
    {
      return null;
    }

    @Override
    public ListenableFuture<List<IndexingWorkerInfo>> getWorkers()
    {
      return Futures.immediateFuture(Collections.emptyList());
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      return Futures.immediateFuture(
          new IndexingTotalWorkerCapacityInfo(Integer.MAX_VALUE, Integer.MAX_VALUE)
      );
    }

    @Override
    public OverlordClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
    {
      return this;
    }
  }
}
