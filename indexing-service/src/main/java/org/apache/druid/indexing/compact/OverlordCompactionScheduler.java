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
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.server.compaction.CompactionRunSimulator;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link CompactionScheduler}.
 * <p>
 * When {@link DruidCompactionConfig#isUseSupervisors()} is true, this class performs the
 * following responsibilities on the leader Overlord:
 * <ul>
 * <li>Poll segments from metadata</li>
 * <li>Check compaction status every 5 seconds</li>
 * <li>Submit compaction tasks for active datasources</li>
 * <li>Track status of submitted compaction tasks</li>
 * </ul>
 * Internally, this class uses an instance of {@link CompactSegments} duty.
 */
public class OverlordCompactionScheduler implements CompactionScheduler
{
  private static final Logger log = new Logger(OverlordCompactionScheduler.class);

  private static final long SCHEDULE_PERIOD_SECONDS = 5;
  private static final Duration METRIC_EMISSION_PERIOD = Duration.standardMinutes(5);

  private final SegmentsMetadataManager segmentManager;
  private final LocalOverlordClient overlordClient;
  private final ServiceEmitter emitter;
  private final TaskMaster taskMaster;

  private final Supplier<DruidCompactionConfig> compactionConfigSupplier;
  private final ConcurrentHashMap<String, DataSourceCompactionConfig> activeDatasourceConfigs;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private final ScheduledExecutorService executor;

  private final CompactionStatusTracker statusTracker;

  /**
   * Listener to watch task completion events and update CompactionStatusTracker.
   * This helps in avoiding unnecessary metadata store calls.
   */
  private final TaskRunnerListener taskRunnerListener;

  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final CompactSegments duty;

  /**
   * The scheduler should enable/disable polling of segments only if the Overlord
   * is running in standalone mode, otherwise this is handled by the DruidCoordinator
   * class itself.
   */
  private final boolean shouldPollSegments;

  private final Stopwatch sinceStatsEmitted = Stopwatch.createUnstarted();

  @Inject
  public OverlordCompactionScheduler(
      TaskMaster taskMaster,
      TaskQueryTool taskQueryTool,
      SegmentsMetadataManager segmentManager,
      Supplier<DruidCompactionConfig> compactionConfigSupplier,
      CompactionStatusTracker statusTracker,
      CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      ScheduledExecutorFactory executorFactory,
      ServiceEmitter emitter,
      ObjectMapper objectMapper
  )
  {
    this.segmentManager = segmentManager;
    this.emitter = emitter;
    this.taskMaster = taskMaster;
    this.compactionConfigSupplier = compactionConfigSupplier;

    this.executor = executorFactory.create(1, "CompactionScheduler-%s");
    this.statusTracker = statusTracker;
    this.shouldPollSegments = segmentManager != null
                              && !coordinatorOverlordServiceConfig.isEnabled();
    this.overlordClient = new LocalOverlordClient(taskMaster, taskQueryTool, objectMapper);
    this.duty = new CompactSegments(this.statusTracker, overlordClient);
    this.activeDatasourceConfigs = new ConcurrentHashMap<>();

    this.taskRunnerListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "OverlordCompactionScheduler";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {
        // Do nothing
      }

      @Override
      public void statusChanged(String taskId, TaskStatus status)
      {
        if (status.isComplete()) {
          statusTracker.onTaskFinished(taskId, status);
        }
      }
    };
  }

  @LifecycleStart
  public synchronized void start()
  {
    // Do nothing
  }

  @LifecycleStop
  public synchronized void stop()
  {
    executor.shutdownNow();
  }

  @Override
  public void becomeLeader()
  {
    if (isLeader.compareAndSet(false, true)) {
      scheduleOnExecutor(this::scheduledRun, SCHEDULE_PERIOD_SECONDS);
    }
  }

  @Override
  public void stopBeingLeader()
  {
    isLeader.set(false);
  }

  @Override
  public boolean isRunning()
  {
    return started.get();
  }

  @Override
  public CompactionConfigValidationResult validateCompactionConfig(DataSourceCompactionConfig compactionConfig)
  {
    if (compactionConfig == null) {
      return CompactionConfigValidationResult.failure("Cannot be null");
    } else {
      return ClientCompactionRunnerInfo.validateCompactionConfig(
          compactionConfig,
          getLatestConfig().getEngine()
      );
    }
  }

  @Override
  public void startCompaction(String dataSourceName, DataSourceCompactionConfig config)
  {
    // Track active datasources even if scheduler has not started yet because
    // SupervisorManager is started before the scheduler
    if (isEnabled()) {
      activeDatasourceConfigs.put(dataSourceName, config);
    }
  }

  @Override
  public void stopCompaction(String dataSourceName)
  {
    activeDatasourceConfigs.remove(dataSourceName);
    statusTracker.removeDatasource(dataSourceName);
  }

  /**
   * Initializes scheduler state if required.
   */
  private synchronized void initState()
  {
    if (!started.compareAndSet(false, true)) {
      return;
    }

    log.info("Starting compaction scheduler.");
    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().registerListener(taskRunnerListener, Execs.directExecutor());
    }
    if (shouldPollSegments) {
      segmentManager.startPollingDatabasePeriodically();
    }
  }

  /**
   * Cleans up scheduler state if required.
   */
  private synchronized void cleanupState()
  {
    if (!started.compareAndSet(true, false)) {
      return;
    }

    log.info("Stopping compaction scheduler.");
    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().unregisterListener(taskRunnerListener.getListenerId());
    }
    statusTracker.stop();
    activeDatasourceConfigs.clear();

    if (shouldPollSegments) {
      segmentManager.stopPollingDatabasePeriodically();
    }
  }

  @Override
  public boolean isEnabled()
  {
    return compactionConfigSupplier.get().isUseSupervisors();
  }

  /**
   * Periodic task which runs the compaction duty if we are leader and
   * useSupervisors is true. Otherwise, the scheduler state is cleaned up.
   */
  private synchronized void scheduledRun()
  {
    if (!isLeader.get()) {
      cleanupState();
      return;
    }

    if (isEnabled()) {
      initState();
      try {
        runCompactionDuty();
      }
      catch (Exception e) {
        log.error(e, "Error processing compaction queue. Continuing schedule.");
      }
      scheduleOnExecutor(this::scheduledRun, SCHEDULE_PERIOD_SECONDS);
    } else {
      cleanupState();
      scheduleOnExecutor(this::scheduledRun, SCHEDULE_PERIOD_SECONDS * 4);
    }
  }

  /**
   * Runs the compaction duty and emits stats if {@link #METRIC_EMISSION_PERIOD}
   * has elapsed.
   */
  private synchronized void runCompactionDuty()
  {
    final CoordinatorRunStats stats = new CoordinatorRunStats();
    duty.run(getLatestConfig(), getDatasourceSnapshot(), getLatestConfig().getEngine(), stats);

    // Emit stats only if emission period has elapsed
    if (!sinceStatsEmitted.isRunning() || sinceStatsEmitted.hasElapsed(METRIC_EMISSION_PERIOD)) {
      stats.forEachStat(
          (stat, dimensions, value) -> {
            if (stat.shouldEmit()) {
              emitStat(stat, dimensions.getValues(), value);
            }
          }
      );
      sinceStatsEmitted.restart();
    } else {
      // Always emit number of submitted tasks
      long numSubmittedTasks = stats.get(Stats.Compaction.SUBMITTED_TASKS);
      emitStat(Stats.Compaction.SUBMITTED_TASKS, Collections.emptyMap(), numSubmittedTasks);
    }
  }

  @Override
  public AutoCompactionSnapshot getCompactionSnapshot(String dataSource)
  {
    if (!activeDatasourceConfigs.containsKey(dataSource)) {
      return AutoCompactionSnapshot.builder(dataSource)
                                   .withStatus(AutoCompactionSnapshot.ScheduleStatus.NOT_ENABLED)
                                   .build();
    }

    final AutoCompactionSnapshot snapshot = duty.getAutoCompactionSnapshot(dataSource);
    if (snapshot == null) {
      final AutoCompactionSnapshot.ScheduleStatus status =
          isEnabled()
          ? AutoCompactionSnapshot.ScheduleStatus.AWAITING_FIRST_RUN
          : AutoCompactionSnapshot.ScheduleStatus.NOT_ENABLED;
      return AutoCompactionSnapshot.builder(dataSource).withStatus(status).build();
    } else {
      return snapshot;
    }
  }

  @Override
  public Map<String, AutoCompactionSnapshot> getAllCompactionSnapshots()
  {
    return duty.getAutoCompactionSnapshot();
  }

  @Override
  public CompactionSimulateResult simulateRunWithConfigUpdate(ClusterCompactionConfig updateRequest)
  {
    if (isRunning()) {
      return new CompactionRunSimulator(statusTracker, overlordClient).simulateRunWithConfig(
          getLatestConfig().withClusterConfig(updateRequest),
          getDatasourceSnapshot(),
          getLatestConfig().getEngine()
      );
    } else {
      return new CompactionSimulateResult(Collections.emptyMap());
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

  private DruidCompactionConfig getLatestConfig()
  {
    return DruidCompactionConfig
        .empty()
        .withClusterConfig(compactionConfigSupplier.get().clusterConfig())
        .withDatasourceConfigs(new ArrayList<>(activeDatasourceConfigs.values()));
  }

  private DataSourcesSnapshot getDatasourceSnapshot()
  {
    return segmentManager.getRecentDataSourcesSnapshot();
  }

  private void scheduleOnExecutor(Runnable runnable, long delaySeconds)
  {
    executor.schedule(
        () -> {
          try {
            runnable.run();
          }
          catch (Throwable t) {
            log.error(t, "Error while executing runnable");
          }
        },
        delaySeconds,
        TimeUnit.SECONDS
    );
  }
}
