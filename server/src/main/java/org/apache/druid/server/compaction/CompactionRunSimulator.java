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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Simulates runs of auto-compaction duty to obtain the expected list of
 * compaction tasks that would be submitted by the actual compaction duty.
 */
public class CompactionRunSimulator
{
  private final CompactionStatusTracker statusTracker;
  private final OverlordClient readOnlyOverlordClient;

  public CompactionRunSimulator(
      CompactionStatusTracker statusTracker,
      OverlordClient overlordClient
  )
  {
    this.statusTracker = statusTracker;
    this.readOnlyOverlordClient = new ReadOnlyOverlordClient(overlordClient);
  }

  /**
   * Simulates a run of the compact segments duty with the given compaction config
   * assuming unlimited compaction task slots.
   */
  public CompactionSimulateResult simulateRunWithConfig(
      DruidCompactionConfig compactionConfig,
      DataSourcesSnapshot dataSourcesSnapshot,
      CompactionEngine defaultEngine
  )
  {
    final Table compactedIntervals
        = Table.withColumnNames("dataSource", "interval", "numSegments", "bytes");
    final Table runningIntervals
        = Table.withColumnNames("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact");
    final Table queuedIntervals
        = Table.withColumnNames("dataSource", "interval", "numSegments", "bytes", "maxTaskSlots", "reasonToCompact");
    final Table skippedIntervals
        = Table.withColumnNames("dataSource", "interval", "numSegments", "bytes", "reasonToSkip");

    // Add a read-only wrapper over the actual status tracker so that we can
    // account for the active tasks
    final CompactionStatusTracker simulationStatusTracker = new CompactionStatusTracker(null)
    {
      @Override
      public CompactionStatus computeCompactionStatus(
          CompactionCandidate candidate,
          DataSourceCompactionConfig config,
          CompactionCandidateSearchPolicy searchPolicy
      )
      {
        return statusTracker.computeCompactionStatus(candidate, config, searchPolicy);
      }

      @Override
      public void onCompactionStatusComputed(
          CompactionCandidate candidateSegments,
          DataSourceCompactionConfig config
      )
      {
        final CompactionStatus status = candidateSegments.getCurrentStatus();
        if (status == null) {
          // do nothing
        } else if (status.getState() == CompactionStatus.State.COMPLETE) {
          compactedIntervals.addRow(
              createRow(candidateSegments, null, null)
          );
        } else if (status.getState() == CompactionStatus.State.RUNNING) {
          runningIntervals.addRow(
              createRow(candidateSegments, ClientCompactionTaskQueryTuningConfig.from(config), status.getReason())
          );
        } else if (status.getState() == CompactionStatus.State.SKIPPED) {
          skippedIntervals.addRow(
              createRow(candidateSegments, null, status.getReason())
          );
        }
      }

      @Override
      public void onTaskSubmitted(ClientCompactionTaskQuery taskPayload, CompactionCandidate candidateSegments)
      {
        // Add a row for each task in order of submission
        final CompactionStatus status = candidateSegments.getCurrentStatus();
        queuedIntervals.addRow(
            createRow(candidateSegments, taskPayload.getTuningConfig(), status == null ? "" : status.getReason())
        );
      }
    };

    // Unlimited task slots to ensure that simulator does not skip any interval
    final DruidCompactionConfig configWithUnlimitedTaskSlots = compactionConfig.withClusterConfig(
        new ClusterCompactionConfig(1.0, Integer.MAX_VALUE, null, null)
    );

    final CoordinatorRunStats stats = new CoordinatorRunStats();
    new CompactSegments(simulationStatusTracker, readOnlyOverlordClient).run(
        configWithUnlimitedTaskSlots,
        dataSourcesSnapshot,
        defaultEngine,
        stats
    );

    final Map<CompactionStatus.State, Table> compactionStates = new HashMap<>();
    if (!compactedIntervals.isEmpty()) {
      compactionStates.put(CompactionStatus.State.COMPLETE, compactedIntervals);
    }
    if (!runningIntervals.isEmpty()) {
      compactionStates.put(CompactionStatus.State.RUNNING, runningIntervals);
    }
    if (!queuedIntervals.isEmpty()) {
      compactionStates.put(CompactionStatus.State.PENDING, queuedIntervals);
    }
    if (!skippedIntervals.isEmpty()) {
      compactionStates.put(CompactionStatus.State.SKIPPED, skippedIntervals);
    }

    return new CompactionSimulateResult(compactionStates);
  }

  private Object[] createRow(
      CompactionCandidate candidate,
      ClientCompactionTaskQueryTuningConfig tuningConfig,
      String reason
  )
  {
    final List<Object> row = new ArrayList<>();
    row.add(candidate.getDataSource());
    row.add(candidate.getUmbrellaInterval());
    row.add(candidate.numSegments());
    row.add(candidate.getTotalBytes());
    if (tuningConfig != null) {
      row.add(CompactSegments.findMaxNumTaskSlotsUsedByOneNativeCompactionTask(tuningConfig));
    }
    if (reason != null) {
      row.add(reason);
    }

    return row.toArray(new Object[0]);
  }

  /**
   * Dummy overlord client that returns empty results for all APIs.
   */
  private static class ReadOnlyOverlordClient extends NoopOverlordClient
  {
    final OverlordClient delegate;

    ReadOnlyOverlordClient(OverlordClient delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return delegate.taskStatuses(state, dataSource, maxCompletedTasks);
    }

    @Override
    public ListenableFuture<Map<String, TaskStatus>> taskStatuses(Set<String> taskIds)
    {
      return delegate.taskStatuses(taskIds);
    }

    @Override
    public ListenableFuture<TaskPayloadResponse> taskPayload(String taskId)
    {
      return delegate.taskPayload(taskId);
    }

    @Override
    public ListenableFuture<Map<String, List<Interval>>> findLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
    {
      return delegate.findLockedIntervals(lockFilterPolicies);
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      // Unlimited worker capacity to ensure that simulator does not skip any interval
      return Futures.immediateFuture(
          new IndexingTotalWorkerCapacityInfo(Integer.MAX_VALUE, Integer.MAX_VALUE)
      );
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
  }
}
