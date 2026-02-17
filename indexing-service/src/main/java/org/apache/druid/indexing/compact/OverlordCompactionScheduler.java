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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueryTool;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.segment.metadata.IndexingStateCache;
import org.apache.druid.segment.metadata.IndexingStateStorage;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.CompactionRunSimulator;
import org.apache.druid.server.compaction.CompactionSimulateResult;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.compaction.CompactionTaskStatus;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCompactionConfig;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

  /**
   * Scheduler run period is 15 minutes. It has been kept high to avoid eager
   * recomputation of the queue as it may be a very compute-intensive operation
   * taking upto several minutes on clusters with a large number of used segments.
   * Jobs for a single supervisor may still be recomputed when the supervisor is updated.
   */
  private static final long DEFAULT_SCHEDULE_PERIOD_MILLIS = 15 * 60_000;

  private final SegmentsMetadataManager segmentManager;
  private final LocalOverlordClient overlordClient;
  private final BrokerClient brokerClient;
  private final ServiceEmitter emitter;
  private final ObjectMapper objectMapper;
  private final TaskMaster taskMaster;

  private final Supplier<DruidCompactionConfig> compactionConfigSupplier;
  private final ConcurrentHashMap<String, CompactionSupervisor> activeSupervisors;

  private final AtomicReference<Map<String, AutoCompactionSnapshot>> datasourceToCompactionSnapshot;
  private final AtomicBoolean isJobRefreshPending = new AtomicBoolean(false);

  /**
   * Compaction job queue built in the last invocation of {@link #resetCompactionJobQueue()}.
   * This is guarded to ensure that the operations performed on the queue are thread-safe.
   */
  @GuardedBy("this")
  private final AtomicReference<CompactionJobQueue> latestJobQueue;

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private final ScheduledExecutorService executor;

  private final CompactionStatusTracker statusTracker;
  private final TaskActionClientFactory taskActionClientFactory;
  private final DruidInputSourceFactory druidInputSourceFactory;
  private final GlobalTaskLockbox taskLockbox;

  /**
   * Listener to watch task completion events and update CompactionStatusTracker.
   * This helps in avoiding unnecessary metadata store calls.
   */
  private final TaskRunnerListener taskRunnerListener;

  private final AtomicBoolean isLeader = new AtomicBoolean(false);
  private final AtomicBoolean started = new AtomicBoolean(false);

  /**
   * The scheduler should enable/disable polling of segments only if the Overlord
   * is running in standalone mode, otherwise this is handled by the DruidCoordinator
   * class itself.
   */
  private final boolean shouldPollSegments;
  private final long schedulePeriodMillis;

  private final IndexingStateStorage indexingStateStorage;
  private final IndexingStateCache indexingStateCache;

  @Inject
  public OverlordCompactionScheduler(
      TaskMaster taskMaster,
      GlobalTaskLockbox taskLockbox,
      TaskQueryTool taskQueryTool,
      SegmentsMetadataManager segmentManager,
      SegmentsMetadataManagerConfig segmentManagerConfig,
      Supplier<DruidCompactionConfig> compactionConfigSupplier,
      CompactionStatusTracker statusTracker,
      CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      TaskActionClientFactory taskActionClientFactory,
      DruidInputSourceFactory druidInputSourceFactory,
      ScheduledExecutorFactory executorFactory,
      BrokerClient brokerClient,
      ServiceEmitter emitter,
      ObjectMapper objectMapper,
      IndexingStateStorage indexingStateStorage,
      IndexingStateCache indexingStateCache
  )
  {
    final long segmentPollPeriodMillis =
        segmentManagerConfig.getPollDuration().toStandardDuration().getMillis();
    this.schedulePeriodMillis = Math.min(DEFAULT_SCHEDULE_PERIOD_MILLIS, 10 * segmentPollPeriodMillis);

    this.segmentManager = segmentManager;
    this.emitter = emitter;
    this.objectMapper = objectMapper;
    this.taskMaster = taskMaster;
    this.taskLockbox = taskLockbox;
    this.compactionConfigSupplier = compactionConfigSupplier;

    this.executor = executorFactory.create(1, "CompactionScheduler-%s");
    this.statusTracker = statusTracker;
    this.shouldPollSegments = segmentManager != null
                              && !coordinatorOverlordServiceConfig.isEnabled();
    this.overlordClient = new LocalOverlordClient(taskMaster, taskQueryTool, objectMapper);
    this.brokerClient = brokerClient;
    this.activeSupervisors = new ConcurrentHashMap<>();
    this.datasourceToCompactionSnapshot = new AtomicReference<>(Map.of());
    this.latestJobQueue = new AtomicReference<>();

    this.taskActionClientFactory = taskActionClientFactory;
    this.druidInputSourceFactory = druidInputSourceFactory;
    this.indexingStateStorage = indexingStateStorage;
    this.indexingStateCache = indexingStateCache;
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
          scheduleOnExecutor(() -> onTaskFinished(taskId, status), 0L);
        }
      }
    };
  }

  @LifecycleStart
  public synchronized void start()
  {
    // Validate that if compaction supervisors are enabled, the segment metadata incremental cache must be enabled
    if (compactionConfigSupplier.get().isUseSupervisors()) {
      if (segmentManager != null && !segmentManager.isPollingDatabasePeriodically()) {
        throw DruidException
            .forPersona(DruidException.Persona.OPERATOR)
            .ofCategory(DruidException.Category.INVALID_INPUT)
            .build(
                "Compaction supervisors require segment metadata cache to be enabled. "
                + "Set 'druid.manager.segments.useIncrementalCache=always' or 'ifSynced'"
            );
      }
    }
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
      // Schedule first run after a small delay
      scheduleOnExecutor(this::scheduledRun, 1_000L);
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
          getLatestClusterConfig().getEngine()
      );
    }
  }

  @Override
  public void startCompaction(String dataSourceName, CompactionSupervisor supervisor)
  {
    // Track active supervisors even if scheduler is disabled or stopped,
    // so that the supervisors may be used if the scheduler is enabled later.
    activeSupervisors.put(dataSourceName, supervisor);

    if (isEnabled() && started.get()) {
      isJobRefreshPending.set(true);
      scheduleOnExecutor(() -> refreshJobs(dataSourceName, supervisor), 0L);
    }
  }

  @Override
  public void stopCompaction(String dataSourceName)
  {
    activeSupervisors.remove(dataSourceName);
    updateQueueIfComputed(queue -> queue.removeJobs(dataSourceName));
    statusTracker.removeDatasource(dataSourceName);
  }

  @Override
  public Map<CompactionStatus.State, List<CompactionJobStatus>> getJobsByStatus(String dataSource)
  {
    if (!activeSupervisors.containsKey(dataSource)) {
      return Map.of();
    }

    // Get the state of all the jobs from the queue
    final List<CompactionJob> allQueuedJobs = new ArrayList<>();
    final List<CompactionCandidate> fullyCompactedIntervals = new ArrayList<>();
    final List<CompactionCandidate> skippedIntervals = new ArrayList<>();

    final AtomicReference<CompactionCandidateSearchPolicy> searchPolicy = new AtomicReference<>();
    updateQueueIfComputed(queue -> {
      allQueuedJobs.addAll(queue.getQueuedJobs());
      fullyCompactedIntervals.addAll(queue.getFullyCompactedIntervals(dataSource));
      skippedIntervals.addAll(queue.getSkippedIntervals(dataSource));

      searchPolicy.set(queue.getSearchPolicy());
    });

    // Search policy would be null only if queue has not been computed yet
    if (searchPolicy.get() == null) {
      return Map.of();
    }

    // Sort and filter out the jobs for the required datasource
    final TreeSet<CompactionJob> sortedJobs = new TreeSet<>(
        (o1, o2) -> searchPolicy.get().compareCandidates(o1.getCandidate(), o2.getCandidate())
    );
    sortedJobs.addAll(allQueuedJobs);

    final Map<CompactionStatus.State, List<CompactionJobStatus>> jobsByStatus = new HashMap<>();

    int jobPositionInQueue = 0;
    for (CompactionJob job : sortedJobs) {
      if (job.getDataSource().equals(dataSource)) {
        final CompactionStatus currentStatus =
            statusTracker.computeCompactionStatus(job.getCandidate(), searchPolicy.get());
        jobsByStatus.computeIfAbsent(currentStatus.getState(), s -> new ArrayList<>())
                    .add(new CompactionJobStatus(job, currentStatus, jobPositionInQueue));
      }
      ++jobPositionInQueue;
    }

    // Add skipped jobs
    for (CompactionCandidate candidate : skippedIntervals) {
      final CompactionJob dummyJob = new CompactionJob(createDummyTask("", candidate), candidate, -1, null, null);
      final CompactionStatus currentStatus =
          statusTracker.computeCompactionStatus(candidate, searchPolicy.get());
      jobsByStatus.computeIfAbsent(currentStatus.getState(), s -> new ArrayList<>())
                  .add(new CompactionJobStatus(dummyJob, currentStatus, -1));
    }

    // Add recently completed jobs
    for (CompactionCandidate candidate : fullyCompactedIntervals) {
      final CompactionTaskStatus taskStatus = statusTracker.getLatestTaskStatus(candidate);
      final String taskId = taskStatus == null ? "" : taskStatus.getTaskId();
      final CompactionJob dummyJob = new CompactionJob(createDummyTask(taskId, candidate), candidate, -1, null, null);

      final CompactionStatus currentStatus = candidate.getCurrentStatus();
      jobsByStatus.computeIfAbsent(currentStatus.getState(), s -> new ArrayList<>())
                  .add(new CompactionJobStatus(dummyJob, currentStatus, -1));
    }

    return jobsByStatus;
  }

  private static ClientCompactionTaskQuery createDummyTask(String taskId, CompactionCandidate candidate)
  {
    return new ClientCompactionTaskQuery(
        taskId,
        candidate.getDataSource(),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  /**
   * Initializes scheduler state if required.
   */
  private synchronized void initState()
  {
    if (!started.compareAndSet(false, true)) {
      return;
    }

    log.info("Starting compaction scheduler with period [%d] millis.", schedulePeriodMillis);
    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().registerListener(taskRunnerListener, executor);
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
    activeSupervisors.clear();
    latestJobQueue.set(null);

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
        resetCompactionJobQueue();
      }
      catch (Exception e) {
        log.error(e, "Error processing compaction queue. Continuing schedule.");
      }
      scheduleOnExecutor(this::scheduledRun, schedulePeriodMillis);
    } else {
      // Schedule run again in case compaction supervisors get enabled
      cleanupState();
      scheduleOnExecutor(this::scheduledRun, schedulePeriodMillis);
    }
  }

  /**
   * Creates and launches eligible compaction jobs.
   */
  private synchronized void resetCompactionJobQueue()
  {
    // Remove the old queue so that no more jobs are added to it
    latestJobQueue.set(null);

    final Stopwatch runDuration = Stopwatch.createStarted();
    final DataSourcesSnapshot dataSourcesSnapshot = getDatasourceSnapshot();
    final CompactionJobQueue queue = new CompactionJobQueue(
        dataSourcesSnapshot,
        getLatestClusterConfig(),
        statusTracker,
        taskActionClientFactory,
        taskLockbox,
        overlordClient,
        brokerClient,
        objectMapper,
        indexingStateStorage,
        indexingStateCache
    );
    latestJobQueue.set(queue);

    statusTracker.resetActiveDatasources(activeSupervisors.keySet());
    statusTracker.onSegmentTimelineUpdated(dataSourcesSnapshot.getSnapshotTime());

    // Jobs for all active supervisors are being freshly created
    // recomputation will not be needed
    isJobRefreshPending.set(false);
    activeSupervisors.forEach(this::createAndEnqueueJobs);

    launchPendingJobs();
    queue.getRunStats().forEachStat(this::emitStat);
    emitStat(Stats.Compaction.SCHEDULER_RUN_TIME, RowKey.empty(), runDuration.millisElapsed());
  }

  /**
   * Launches pending compaction jobs if compaction task slots become available.
   * This method uses the jobs created by the last invocation of {@link #resetCompactionJobQueue()}.
   */
  private void launchPendingJobs()
  {
    updateQueueIfComputed(queue -> {
      queue.runReadyJobs();
      datasourceToCompactionSnapshot.set(queue.getSnapshots());
    });
  }

  /**
   * Refreshes compaction jobs for the given datasource if required.
   */
  private void refreshJobs(String dataSource, CompactionSupervisor supervisor)
  {
    if (isJobRefreshPending.get()) {
      createAndEnqueueJobs(dataSource, supervisor);
    }
  }

  private void createAndEnqueueJobs(String dataSource, CompactionSupervisor supervisor)
  {
    updateQueueIfComputed(
        queue -> queue.createAndEnqueueJobs(
            supervisor,
            druidInputSourceFactory.create(dataSource, Intervals.ETERNITY)
        )
    );
  }

  /**
   * Performs a thread-safe read or write operation on the {@link #latestJobQueue}
   * if it has already been computed.
   */
  private synchronized void updateQueueIfComputed(Consumer<CompactionJobQueue> operation)
  {
    final CompactionJobQueue queue = latestJobQueue.get();
    if (queue != null) {
      operation.accept(queue);
    }
  }

  private void onTaskFinished(String taskId, TaskStatus taskStatus)
  {
    statusTracker.onTaskFinished(taskId, taskStatus);

    updateQueueIfComputed(queue -> {
      queue.onTaskFinished(taskId, taskStatus);
      datasourceToCompactionSnapshot.set(queue.getSnapshots());
    });

    launchPendingJobs();
  }

  @Override
  public AutoCompactionSnapshot getCompactionSnapshot(String dataSource)
  {
    if (!activeSupervisors.containsKey(dataSource)) {
      return AutoCompactionSnapshot.builder(dataSource)
                                   .withStatus(AutoCompactionSnapshot.ScheduleStatus.NOT_ENABLED)
                                   .build();
    }

    final AutoCompactionSnapshot snapshot = datasourceToCompactionSnapshot.get().get(dataSource);
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
    return Map.copyOf(datasourceToCompactionSnapshot.get());
  }

  @Override
  public CompactionSimulateResult simulateRunWithConfigUpdate(ClusterCompactionConfig updateRequest)
  {
    if (isRunning()) {
      return new CompactionRunSimulator(statusTracker, overlordClient).simulateRunWithConfig(
          getLatestConfig().withClusterConfig(updateRequest),
          getDatasourceSnapshot(),
          updateRequest.getEngine()
      );
    } else {
      return new CompactionSimulateResult(Collections.emptyMap());
    }
  }

  private void emitStat(CoordinatorStat stat, RowKey rowKey, long value)
  {
    if (!stat.shouldEmit()) {
      return;
    }

    ServiceMetricEvent.Builder eventBuilder = new ServiceMetricEvent.Builder();
    rowKey.getValues().forEach(
        (dim, dimValue) -> eventBuilder.setDimension(dim.reportedName(), dimValue)
    );
    emitter.emit(eventBuilder.setMetric(stat.getMetricName(), value));
  }

  private DruidCompactionConfig getLatestConfig()
  {
    final List<DataSourceCompactionConfig> configs = activeSupervisors
        .values()
        .stream()
        .map(s -> s.getSpec().getSpec())
        .collect(Collectors.toList());
    return DruidCompactionConfig
        .empty()
        .withClusterConfig(getLatestClusterConfig())
        .withDatasourceConfigs(configs);
  }

  private ClusterCompactionConfig getLatestClusterConfig()
  {
    return compactionConfigSupplier.get().clusterConfig();
  }

  private DataSourcesSnapshot getDatasourceSnapshot()
  {
    return segmentManager.getRecentDataSourcesSnapshot();
  }

  private void scheduleOnExecutor(Runnable runnable, long delayMillis)
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
        delayMillis,
        TimeUnit.MILLISECONDS
    );
  }
}
