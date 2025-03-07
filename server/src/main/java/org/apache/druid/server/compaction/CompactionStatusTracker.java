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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks status of recently submitted compaction tasks. Can be used by a segment
 * search policy to skip an interval if it has been recently compacted or if it
 * keeps failing repeatedly.
 */
public class CompactionStatusTracker
{
  private static final Duration MAX_STATUS_RETAIN_DURATION = Duration.standardHours(12);

  private final ObjectMapper objectMapper;
  private final ConcurrentHashMap<String, DatasourceStatus> datasourceStatuses
      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CompactionCandidate> submittedTaskIdToSegments
      = new ConcurrentHashMap<>();

  private final AtomicReference<DateTime> segmentSnapshotTime = new AtomicReference<>();

  @Inject
  public CompactionStatusTracker(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public void stop()
  {
    datasourceStatuses.clear();
  }

  public void removeDatasource(String datasource)
  {
    datasourceStatuses.remove(datasource);
  }

  public CompactionTaskStatus getLatestTaskStatus(CompactionCandidate candidates)
  {
    return datasourceStatuses
        .getOrDefault(candidates.getDataSource(), DatasourceStatus.EMPTY)
        .intervalToTaskStatus
        .get(candidates.getUmbrellaInterval());
  }

  /**
   * Set of submitted compaction task IDs which have not been marked completed
   * via {@link #onTaskFinished} yet.
   */
  public Set<String> getSubmittedTaskIds()
  {
    return submittedTaskIdToSegments.keySet();
  }

  public CompactionStatus computeCompactionStatus(
      CompactionCandidate candidate,
      DataSourceCompactionConfig config,
      CompactionCandidateSearchPolicy searchPolicy
  )
  {
    final CompactionStatus compactionStatus = CompactionStatus.compute(candidate, config, objectMapper);
    if (compactionStatus.isComplete()) {
      return compactionStatus;
    }

    // Skip intervals that violate max allowed input segment size
    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    if (candidate.getTotalBytes() > inputSegmentSize) {
      return CompactionStatus.skipped(
          "'inputSegmentSize' exceeded: Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
          candidate.getTotalBytes(), inputSegmentSize
      );
    }

    // Skip intervals that already have a running task
    final CompactionTaskStatus lastTaskStatus = getLatestTaskStatus(candidate);
    if (lastTaskStatus != null && lastTaskStatus.getState() == TaskState.RUNNING) {
      return CompactionStatus.skipped("Task for interval is already running");
    }

    // Skip intervals that have been recently compacted if segment timeline is not updated yet
    final DateTime snapshotTime = segmentSnapshotTime.get();
    if (lastTaskStatus != null
        && lastTaskStatus.getState() == TaskState.SUCCESS
        && snapshotTime != null && snapshotTime.isBefore(lastTaskStatus.getUpdatedTime())) {
      return CompactionStatus.skipped(
          "Segment timeline not updated since last compaction task succeeded"
      );
    }

    // Skip intervals that have been filtered out by the policy
    if (!searchPolicy.isEligibleForCompaction(candidate, compactionStatus, lastTaskStatus)) {
      return CompactionStatus.skipped("Rejected by search policy");
    }

    return compactionStatus;
  }

  public void onCompactionStatusComputed(
      CompactionCandidate candidateSegments,
      DataSourceCompactionConfig config
  )
  {
    // Nothing to do, used by simulator
  }

  public void onSegmentTimelineUpdated(DateTime snapshotTime)
  {
    this.segmentSnapshotTime.set(snapshotTime);
  }

  public void onCompactionConfigUpdated(DruidCompactionConfig compactionConfig)
  {
    final Set<String> compactionEnabledDatasources = new HashSet<>();
    if (compactionConfig.getCompactionConfigs() != null) {
      compactionConfig.getCompactionConfigs().forEach(config -> {
        getOrComputeDatasourceStatus(config.getDataSource())
            .cleanupStaleTaskStatuses();

        compactionEnabledDatasources.add(config.getDataSource());
      });
    }

    // Clean up state for datasources where compaction has been disabled
    final Set<String> allDatasources = new HashSet<>(datasourceStatuses.keySet());
    allDatasources.forEach(datasource -> {
      if (!compactionEnabledDatasources.contains(datasource)) {
        datasourceStatuses.remove(datasource);
      }
    });
  }

  public void onTaskSubmitted(
      ClientCompactionTaskQuery taskPayload,
      CompactionCandidate candidateSegments
  )
  {
    submittedTaskIdToSegments.put(taskPayload.getId(), candidateSegments);
    getOrComputeDatasourceStatus(taskPayload.getDataSource())
        .handleSubmittedTask(candidateSegments);
  }

  public void onTaskFinished(String taskId, TaskStatus taskStatus)
  {
    if (!taskStatus.isComplete()) {
      return;
    }

    final CompactionCandidate candidateSegments = submittedTaskIdToSegments.remove(taskId);
    if (candidateSegments == null) {
      // Nothing to do since we don't know the corresponding datasource or interval
      return;
    }

    final Interval compactionInterval = candidateSegments.getUmbrellaInterval();
    getOrComputeDatasourceStatus(candidateSegments.getDataSource())
        .handleCompletedTask(compactionInterval, taskStatus);
  }

  private DatasourceStatus getOrComputeDatasourceStatus(String datasource)
  {
    return datasourceStatuses.computeIfAbsent(datasource, ds -> new DatasourceStatus());
  }

  /**
   * Contains compaction task status of intervals of a datasource.
   */
  private static class DatasourceStatus
  {
    static final DatasourceStatus EMPTY = new DatasourceStatus();

    final ConcurrentHashMap<Interval, CompactionTaskStatus> intervalToTaskStatus
        = new ConcurrentHashMap<>();

    void handleCompletedTask(Interval compactionInterval, TaskStatus taskStatus)
    {
      final CompactionTaskStatus lastKnownStatus = intervalToTaskStatus.get(compactionInterval);
      final DateTime now = DateTimes.nowUtc();

      final CompactionTaskStatus updatedStatus;
      if (taskStatus.isSuccess()) {
        updatedStatus = new CompactionTaskStatus(TaskState.SUCCESS, now, 0);
      } else if (lastKnownStatus == null || lastKnownStatus.getState().isSuccess()) {
        // This is the first failure
        updatedStatus = new CompactionTaskStatus(TaskState.FAILED, now, 1);
      } else {
        updatedStatus = new CompactionTaskStatus(
            TaskState.FAILED,
            now,
            lastKnownStatus.getNumConsecutiveFailures() + 1
        );
      }
      intervalToTaskStatus.put(compactionInterval, updatedStatus);
    }

    void handleSubmittedTask(CompactionCandidate candidateSegments)
    {
      final Interval interval = candidateSegments.getUmbrellaInterval();
      final CompactionTaskStatus lastStatus = intervalToTaskStatus.get(interval);

      final DateTime now = DateTimes.nowUtc();
      if (lastStatus == null || !lastStatus.getState().isFailure()) {
        intervalToTaskStatus.put(interval, new CompactionTaskStatus(TaskState.RUNNING, now, 0));
      } else {
        intervalToTaskStatus.put(
            interval,
            new CompactionTaskStatus(TaskState.RUNNING, now, lastStatus.getNumConsecutiveFailures())
        );
      }
    }

    void cleanupStaleTaskStatuses()
    {
      final DateTime now = DateTimes.nowUtc();

      final Set<Interval> staleIntervals = new HashSet<>();
      intervalToTaskStatus.forEach((interval, taskStatus) -> {
        if (taskStatus.getUpdatedTime().plus(MAX_STATUS_RETAIN_DURATION).isBefore(now)) {
          staleIntervals.add(interval);
        }
      });

      staleIntervals.forEach(intervalToTaskStatus::remove);
    }
  }
}
