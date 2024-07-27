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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tracks status of both recently submitted compaction tasks and the compaction
 * state of segments. Can be used to check if a set of segments is currently
 * eligible for compaction.
 * <p>
 * TODO: Keep the interval sidelined until
 *  - no other interval to compact for the last 1 hour
 *  - some other intervals have been submitted
 *  - threshold has been crossed - 30% uncompacted bytes or 50% uncompacted segments
 *  - interval is uncompacted and has just 1 uncompacted segment out of 100
 *  - level of uncompaction is important to know
 *  - should this be a part of policy
 *
 *
 *  - A policy that picks up stuff only if it meets some thresholds and then
 */
public class CompactionStatusTracker
{
  private static final Logger log = new Logger(CompactionStatusTracker.class);

  private static final int MAX_FAILURE_RETRIES = 3;
  private static final int MAX_SKIPS_AFTER_SUCCESS = 5;
  private static final int MAX_SKIPS_AFTER_FAILURE = 5;

  private final ObjectMapper objectMapper;
  private final Map<String, DatasourceStatus> datasourceStatuses = new HashMap<>();
  private final Map<String, SegmentsToCompact> submittedTaskIdToSegments = new HashMap<>();

  @Inject
  public CompactionStatusTracker(
      ObjectMapper objectMapper
  )
  {
    this.objectMapper = objectMapper;
  }

  public CompactionStatus computeCompactionStatus(
      SegmentsToCompact candidate,
      DataSourceCompactionConfig config
  )
  {
    final CompactionStatus compactionStatus = CompactionStatus.compute(candidate, config, objectMapper);
    if (compactionStatus.isComplete()) {
      return compactionStatus;
    }

    final long inputSegmentSize = config.getInputSegmentSizeBytes();
    if (candidate.getTotalBytes() > inputSegmentSize) {
      return CompactionStatus.skipped(
          "'inputSegmentSize' exceeded: Total segment size[%d] is larger than allowed inputSegmentSize[%d]",
          candidate.getTotalBytes(), inputSegmentSize
      );
    }

    final Interval compactionInterval = candidate.getUmbrellaInterval();

    final IntervalStatus intervalStatus
        = datasourceStatuses.getOrDefault(config.getDataSource(), DatasourceStatus.EMPTY)
                            .getIntervalStatuses()
                            .get(compactionInterval);

    if (intervalStatus == null) {
      return compactionStatus;
    }

    switch (intervalStatus.state) {
      case TASK_SUBMITTED:
      case COMPACTED:
      case FAILED_ALL_RETRIES:
        return CompactionStatus.skipped(
            "recently submitted: current compaction state[%s]",
            intervalStatus.state
        );
      default:
        break;
    }

    return compactionStatus;
  }

  public void onCompactionConfigUpdated(CoordinatorCompactionConfig compactionConfig)
  {
    final Set<String> compactionEnabledDatasources = new HashSet<>();
    if (compactionConfig.getCompactionConfigs() != null) {
      compactionConfig.getCompactionConfigs().forEach(
          config -> compactionEnabledDatasources.add(config.getDataSource())
      );
    }

    // Clean up state for datasources where compaction has been freshly disabled
    final Set<String> allDatasources = new HashSet<>(datasourceStatuses.keySet());
    allDatasources.forEach(datasource -> {
      if (!compactionEnabledDatasources.contains(datasource)) {
        datasourceStatuses.remove(datasource);
      }
    });
  }

  public void onIntervalSkipped(
      SegmentsToCompact candidateSegments,
      CompactionStatus status
  )
  {
    // do nothing
  }

  public void onTaskSubmitted(
      ClientCompactionTaskQuery taskPayload,
      SegmentsToCompact candidateSegments
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

    final SegmentsToCompact candidateSegments = submittedTaskIdToSegments.remove(taskId);
    if (candidateSegments == null) {
      // Nothing to do since we don't know the corresponding datasource or interval
      return;
    }

    final Interval compactionInterval = candidateSegments.getUmbrellaInterval();
    getOrComputeDatasourceStatus(candidateSegments.getDataSource())
        .handleTaskStatus(compactionInterval, taskStatus);
  }

  public void reset()
  {
    datasourceStatuses.clear();
  }

  private DatasourceStatus getOrComputeDatasourceStatus(String datasource)
  {
    return datasourceStatuses.computeIfAbsent(datasource, ds -> new DatasourceStatus());
  }

  private static class DatasourceStatus
  {
    static final DatasourceStatus EMPTY = new DatasourceStatus();

    final Map<Interval, IntervalStatus> intervalStatus = new HashMap<>();

    void handleTaskStatus(Interval compactionInterval, TaskStatus taskStatus)
    {
      final IntervalStatus lastKnownStatus = intervalStatus.get(compactionInterval);

      if (taskStatus.isSuccess()) {
        intervalStatus.put(
            compactionInterval,
            new IntervalStatus(IntervalState.COMPACTED, MAX_SKIPS_AFTER_SUCCESS)
        );
      } else if (lastKnownStatus == null || !lastKnownStatus.isFailed()) {
        // This is the first failure
        intervalStatus.put(
            compactionInterval,
            new IntervalStatus(IntervalState.FAILED, 0)
        );
      } else if (++lastKnownStatus.retryCount >= MAX_FAILURE_RETRIES) {
        // Failure retries have been exhausted
        intervalStatus.put(
            compactionInterval,
            new IntervalStatus(IntervalState.FAILED_ALL_RETRIES, MAX_SKIPS_AFTER_FAILURE)
        );
      }
    }

    void handleSubmittedTask(SegmentsToCompact candidateSegments)
    {
      intervalStatus.computeIfAbsent(
          candidateSegments.getUmbrellaInterval(),
          i -> new IntervalStatus(IntervalState.TASK_SUBMITTED, 0)
      );

      final Set<Interval> readyIntervals = new HashSet<>();
      intervalStatus.forEach((interval, status) -> {
        status.turnsToSkip--;
        if (status.isReady()) {
          readyIntervals.add(interval);
        }
      });

      readyIntervals.forEach(intervalStatus::remove);
    }

    Map<Interval, IntervalStatus> getIntervalStatuses()
    {
      return intervalStatus;
    }
  }

  private static class IntervalStatus
  {
    final IntervalState state;
    int turnsToSkip;
    int retryCount;

    IntervalStatus(IntervalState state, int turnsToSkip)
    {
      this.state = state;
      this.turnsToSkip = turnsToSkip;
    }

    boolean isReady()
    {
      return turnsToSkip <= 0
             && (state == IntervalState.COMPACTED || state == IntervalState.FAILED_ALL_RETRIES);
    }

    boolean isFailed()
    {
      return state == IntervalState.FAILED || state == IntervalState.FAILED_ALL_RETRIES;
    }
  }

  private enum IntervalState
  {
    TASK_SUBMITTED, COMPACTED, FAILED, FAILED_ALL_RETRIES
  }
}
