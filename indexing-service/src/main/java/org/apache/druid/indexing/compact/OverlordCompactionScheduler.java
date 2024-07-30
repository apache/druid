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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.compaction.CompactionRunSimulator;
import org.apache.druid.server.compaction.CompactionScheduler;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.CompactionSchedulerConfig;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.http.CompactionConfigUpdateRequest;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Duration;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TODO: pending items
 *  - [x] add another policy - smallestSegmentFirst
 *  - [ ] come up with better policy names
 *  - [?] poll status of recently completed tasks in CompactSegments duty
 *  - [?] BasePolicy.shouldCompact(): locked and skip offset intervals will always be skipped
 *  - [ ] finalize logic of skipping turns of successful and failed
 *  - [ ] compaction status should have last compacted time
 *  - [ ] compaction status should have num uncompacted segments, bytes
 *  - [ ] test on cluster - standalone, coordinator-overlord
 *  - [ ] unit tests
 *  - [ ] integration tests
 */

/**
 * Compaction Scheduler that runs on the Overlord if
 * {@code druid.compaction.scheduler.enabled=true}.
 */
public class OverlordCompactionScheduler implements CompactionScheduler
{
  private static final Logger log = new Logger(OverlordCompactionScheduler.class);

  private final TaskMaster taskMaster;
  private final JacksonConfigManager configManager;
  private final SegmentsMetadataManager segmentManager;
  private final ServiceEmitter emitter;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private final ScheduledExecutorService executor;

  private final TaskRunnerListener taskStateListener;
  private final CompactionStatusTracker statusTracker;
  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private final CompactSegments duty;

  /**
   * The scheduler should enable/disable polling of segments only if the Overlord
   * is running in standalone mode, otherwise this is handled by the DruidCoordinator
   * class itself.
   */
  private final boolean shouldPollSegments;

  private final Stopwatch sinceStatsEmitted = Stopwatch.createStarted();
  private final CompactionSchedulerConfig schedulerConfig;

  @Inject
  public OverlordCompactionScheduler(
      TaskMaster taskMaster,
      TaskQueryTool taskQueryTool,
      SegmentsMetadataManager segmentManager,
      JacksonConfigManager configManager,
      CompactionSchedulerConfig schedulerConfig,
      CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      ScheduledExecutorFactory executorFactory,
      ServiceEmitter emitter,
      ObjectMapper objectMapper
  )
  {
    this.taskMaster = taskMaster;
    this.configManager = configManager;
    this.segmentManager = segmentManager;
    this.statusTracker = new CompactionStatusTracker(objectMapper);
    this.emitter = emitter;
    this.schedulerConfig = schedulerConfig;
    this.executor = executorFactory.create(1, "CompactionScheduler-%s");
    this.shouldPollSegments = segmentManager != null
                              && !coordinatorOverlordServiceConfig.isEnabled();
    this.duty = new CompactSegments(
        statusTracker,
        new LocalOverlordClient(taskMaster, taskQueryTool, objectMapper)
    );

    this.taskStateListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "CompactionScheduler";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {
        // Do nothing
      }

      @Override
      public void statusChanged(String taskId, TaskStatus status)
      {
        runOnExecutor(() -> statusTracker.onTaskFinished(taskId, status));
      }
    };
  }

  @Override
  public void becomeLeader()
  {
    if (isEnabled() && isLeader.compareAndSet(false, true)) {
      log.info("Starting compaction scheduler as we are now the leader.");
      runOnExecutor(() -> {
        initState();
        checkSchedulingStatus();
      });
    }
  }

  @Override
  public void stopBeingLeader()
  {
    if (isEnabled() && isLeader.compareAndSet(true, false)) {
      log.info("Stopping compaction scheduler as we are not the leader anymore.");
      runOnExecutor(this::cleanupState);
    }
  }

  private void runOnExecutor(Runnable runnable)
  {
    executor.submit(() -> {
      try {
        runnable.run();
      }
      catch (Throwable t) {
        log.error(t, "Error while executing runnable");
      }
    });
  }

  private synchronized void initState()
  {
    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(taskStateListener, executor);
    } else {
      log.warn("No TaskRunner. Unable to register callbacks.");
    }

    if (shouldPollSegments) {
      segmentManager.startPollingDatabasePeriodically();
    }
  }

  private synchronized void cleanupState()
  {
    statusTracker.reset();

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().unregisterListener(taskStateListener.getListenerId());
    }

    if (shouldPollSegments) {
      segmentManager.stopPollingDatabasePeriodically();
    }
  }

  private boolean isEnabled()
  {
    return schedulerConfig.isEnabled();
  }

  private synchronized void checkSchedulingStatus()
  {
    if (isLeader.get() && isEnabled()) {
      try {
        processCompactionQueue(getLatestConfig());
      }
      catch (Exception e) {
        log.error(e, "Error processing compaction queue. Continuing schedule.");
      }
      executor.schedule(this::checkSchedulingStatus, 5, TimeUnit.SECONDS);
    } else {
      cleanupState();
    }
  }

  private synchronized void processCompactionQueue(
      CoordinatorCompactionConfig currentConfig
  )
  {
    final CoordinatorRunStats stats = new CoordinatorRunStats();

    duty.run(
        currentConfig,
        getCurrentDatasourceTimelines(),
        stats
    );

    // Emit stats only every 5 minutes
    if (sinceStatsEmitted.hasElapsed(Duration.standardMinutes(5))) {
      stats.forEachStat(
          (stat, dimensions, value) -> {
            if (stat.shouldEmit()) {
              emitStat(stat, dimensions.getValues(), value);
            }
          }
      );
      sinceStatsEmitted.restart();
    }
  }

  private void emitStat(CoordinatorStat stat, Map<Dimension, String> dimensionValues, long value)
  {
    ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder();
    dimensionValues.forEach(
        (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
    );
    emitter.emit(eventBuilder.setMetric(stat.getMetricName(), value));
  }

  private CoordinatorCompactionConfig getLatestConfig()
  {
    return configManager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class,
        CoordinatorCompactionConfig.empty()
    ).get();
  }

  private Map<String, SegmentTimeline> getCurrentDatasourceTimelines()
  {
    return segmentManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                         .getUsedSegmentsTimelinesPerDataSource();
  }

  @Override
  public AutoCompactionSnapshot getCompactionSnapshot(String dataSource)
  {
    return duty.getAutoCompactionSnapshot(dataSource);
  }

  @Override
  public Long getTotalSizeOfSegmentsAwaitingCompaction(String dataSource)
  {
    return duty.getTotalSizeOfSegmentsAwaitingCompaction(dataSource);
  }

  @Override
  public Map<String, AutoCompactionSnapshot> getAllCompactionSnapshots()
  {
    return duty.getAutoCompactionSnapshot();
  }

  @Override
  public CompactionSimulateResult simulateRunWithConfigUpdate(CompactionConfigUpdateRequest updateRequest)
  {
    return new CompactionRunSimulator(
        statusTracker,
        getLatestConfig(),
        getCurrentDatasourceTimelines()
    ).simulateRunWithConfigUpdate(updateRequest);
  }
}
