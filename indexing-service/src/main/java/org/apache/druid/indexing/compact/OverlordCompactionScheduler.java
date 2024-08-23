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
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionRunSimulator;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionSupervisorsConfig;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.timeline.SegmentTimeline;
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
 * When {@link CompactionSupervisorsConfig} is enabled, this class performs the
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
  private final OverlordClient overlordClient;
  private final ServiceEmitter emitter;

  private final CompactionSupervisorsConfig schedulerConfig;
  private final Supplier<DruidCompactionConfig> compactionConfigSupplier;
  private final ConcurrentHashMap<String, DataSourceCompactionConfig> activeDatasourceConfigs;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private final ScheduledExecutorService executor;

  private final CompactionStatusTracker statusTracker;
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
      CompactionSupervisorsConfig schedulerConfig,
      CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      ScheduledExecutorFactory executorFactory,
      ServiceEmitter emitter,
      ObjectMapper objectMapper
  )
  {
    this.segmentManager = segmentManager;
    this.emitter = emitter;
    this.schedulerConfig = schedulerConfig;
    this.compactionConfigSupplier = compactionConfigSupplier;

    this.executor = executorFactory.create(1, "CompactionScheduler-%s");
    this.statusTracker = statusTracker;
    this.shouldPollSegments = segmentManager != null
                              && !coordinatorOverlordServiceConfig.isEnabled();
    this.overlordClient = new LocalOverlordClient(taskMaster, taskQueryTool, objectMapper);
    this.duty = new CompactSegments(this.statusTracker, overlordClient);
    this.activeDatasourceConfigs = new ConcurrentHashMap<>();
  }

  @Override
  public void start()
  {
    if (isEnabled() && started.compareAndSet(false, true)) {
      log.info("Starting compaction scheduler.");
      initState();
      scheduleOnExecutor(this::checkSchedulingStatus, SCHEDULE_PERIOD_SECONDS);
    }
  }

  @Override
  public void stop()
  {
    if (isEnabled() && started.compareAndSet(true, false)) {
      log.info("Stopping compaction scheduler.");
      cleanupState();
    }
  }

  @Override
  public boolean isRunning()
  {
    return isEnabled() && started.get();
  }

  @Override
  public void startCompaction(String dataSourceName, DataSourceCompactionConfig config)
  {
    if (isRunning()) {
      activeDatasourceConfigs.put(dataSourceName, config);
    }
  }

  @Override
  public void stopCompaction(String dataSourceName)
  {
    activeDatasourceConfigs.remove(dataSourceName);
    statusTracker.removeDatasource(dataSourceName);
  }

  private synchronized void initState()
  {
    if (shouldPollSegments) {
      segmentManager.startPollingDatabasePeriodically();
    }
  }

  private synchronized void cleanupState()
  {
    statusTracker.stop();
    activeDatasourceConfigs.clear();

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
    if (isRunning()) {
      try {
        runCompactionDuty();
      }
      catch (Exception e) {
        log.error(e, "Error processing compaction queue. Continuing schedule.");
      }
      scheduleOnExecutor(this::checkSchedulingStatus, SCHEDULE_PERIOD_SECONDS);
    } else {
      cleanupState();
    }
  }

  private synchronized void runCompactionDuty()
  {
    final CoordinatorRunStats stats = new CoordinatorRunStats();
    duty.run(getLatestConfig(), getCurrentDatasourceTimelines(), stats);

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
    }
  }

  @Override
  public AutoCompactionSnapshot getCompactionSnapshot(String dataSource)
  {
    return duty.getAutoCompactionSnapshot(dataSource);
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
          getCurrentDatasourceTimelines()
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

  private Map<String, SegmentTimeline> getCurrentDatasourceTimelines()
  {
    return segmentManager.getSnapshotOfDataSourcesWithAllUsedSegments()
                         .getUsedSegmentsTimelinesPerDataSource();
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
