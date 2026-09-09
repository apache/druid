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

package org.apache.druid.indexing.overlord.duty;

import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.KillUnusedSegmentsTask;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DatasourceInterval;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link OverlordDuty} to delete unused segments from metadata store and the
 * deep storage. Launches {@link EmbeddedKillTask}s to clean unused segments
 * of a single datasource-interval. These tasks use EXCLUSIVE locks by default.
 *
 * @see SegmentsMetadataManagerConfig#getKillUnused() Config to enable the cleanup
 * @see org.apache.druid.server.coordinator.duty.KillUnusedSegments
 * Legacy mode of killing segments via Coordinator duties
 * @see KillUnusedSegmentsTask Behaviour of kill task with concurrent locks
 */
public class UnusedSegmentsKiller implements OverlordDuty
{
  private static final EmittingLogger log = new EmittingLogger(UnusedSegmentsKiller.class);

  private static final String TASK_ID_PREFIX = "overlord-issued";

  /**
   * Task type for embedded kill tasks. This type is not registered as a valid
   * JSON subtype of {@code Task} since embedded kill tasks are never serialized.
   */
  public static final String TASK_TYPE_EMBEDDED_KILL = "kill_embedded";

  private static final int INITIAL_KILL_QUEUE_SIZE = 1000;
  private static final int MAX_INTERVALS_TO_KILL = 10_000;
  private static final int MAX_SEGMENTS_TO_KILL_IN_BATCH = 1000;

  /**
   * Keep max segments to kill in a single kill task small so that the EXCLUSIVE
   * lock on the underlying interval is not held for too long.
   */
  private static final int MAX_SEGMENTS_TO_KILL_IN_TASK = 10 * MAX_SEGMENTS_TO_KILL_IN_BATCH;

  /**
   * Period after which the queue is reset even if there are existing jobs in queue.
   */
  private static final Duration QUEUE_RESET_PERIOD = Duration.standardDays(1);

  /**
   * Duration for which a kill task is allowed to run. Assuming that processing
   * each batch of segments takes around 30s, each kill task (upto 10 batches)
   * should normally finish in 5 minutes.
   */
  private static final Duration MAX_TASK_DURATION = Duration.standardMinutes(30);

  private final ServiceEmitter emitter;
  private final GlobalTaskLockbox taskLockbox;
  private final DruidLeaderSelector leaderSelector;
  private final DataSegmentKiller dataSegmentKiller;

  private final UnusedSegmentKillerConfig killConfig;
  private final TaskActionClientFactory taskActionClientFactory;
  private final IndexerMetadataStorageCoordinator storageCoordinator;

  /**
   * Single-threaded executor to process kill jobs.
   */
  private final ScheduledExecutorService exec;
  private int previousLeaderTerm;
  private final AtomicReference<DateTime> lastResetTime = new AtomicReference<>(null);

  private final AtomicReference<TaskInfo> currentTaskInfo = new AtomicReference<>(null);

  /**
   * Queue of kill candidates. Use a PriorityBlockingQueue to ensure thread-safety
   * since this queue is accessed by both {@link #run()} and {@link #startNextJobInKillQueue}.
   */
  private final PriorityBlockingQueue<KillCandidate> killQueue;

  @Inject
  public UnusedSegmentsKiller(
      SegmentsMetadataManagerConfig config,
      TaskActionClientFactory taskActionClientFactory,
      IndexerMetadataStorageCoordinator storageCoordinator,
      @IndexingService DruidLeaderSelector leaderSelector,
      ScheduledExecutorFactory executorFactory,
      DataSegmentKiller dataSegmentKiller,
      GlobalTaskLockbox taskLockbox,
      ServiceEmitter emitter
  )
  {
    this.emitter = emitter;
    this.taskLockbox = taskLockbox;
    this.leaderSelector = leaderSelector;
    this.dataSegmentKiller = dataSegmentKiller;
    this.storageCoordinator = storageCoordinator;
    this.taskActionClientFactory = taskActionClientFactory;

    this.killConfig = config.getKillUnused();

    if (isEnabled()) {
      this.exec = executorFactory.create(1, "UnusedSegmentsKiller-%s");
      this.killQueue = new PriorityBlockingQueue<>(
          INITIAL_KILL_QUEUE_SIZE,
          Ordering.from(Comparators.intervalsByEndThenStart())
                  .onResultOf(KillCandidate::interval)
      );
    } else {
      this.exec = null;
      this.killQueue = null;
    }
  }

  @Override
  public boolean isEnabled()
  {
    return killConfig.isEnabled();
  }

  /**
   * Ensures that things are moving along and the kill queue is not stuck.
   * Updates the state if leadership changes or if the queue needs to be reset.
   */
  @Override
  public void run()
  {
    if (!isEnabled()) {
      return;
    }

    updateStateIfNewLeader();
    if (shouldRebuildKillQueue()) {
      // Clear the killQueue to stop further processing of already queued jobs
      killQueue.clear();
      exec.submit(() -> {
        rebuildKillQueue();
        startNextJobInKillQueue();
      });
    }

    // Cancel the current task if it has been running for too long
    final TaskInfo taskInfo = currentTaskInfo.get();
    if (taskInfo != null && !taskInfo.future.isDone()
        && taskInfo.sinceTaskStarted.hasElapsed(MAX_TASK_DURATION)) {
      log.warn(
          "Cancelling kill task[%s] as it has been running for [%,d] millis.",
          taskInfo.taskId, taskInfo.sinceTaskStarted.millisElapsed()
      );
      taskInfo.future.cancel(true);
    }
  }

  @Override
  public DutySchedule getSchedule()
  {
    if (isEnabled()) {
      // Schedule the first run after some delay since the segment metadata cache
      // might take time for the sync to finish if this Overlord has just started.
      final long periodMillis = killConfig.getDutyPeriod().toStandardDuration().getMillis();
      final long initialDelayMillis = periodMillis / 2;

      log.info(
          "Unused segment killer is enabled and will start after [%d] millis."
          + " Kill queue will be rebuilt every [%s] if empty.",
          initialDelayMillis, killConfig.getDutyPeriod()
      );

      return new DutySchedule(periodMillis, initialDelayMillis);
    } else {
      return new DutySchedule(0, 0);
    }
  }

  private void updateStateIfNewLeader()
  {
    final int currentLeaderTerm = leaderSelector.localTerm();
    if (currentLeaderTerm != previousLeaderTerm) {
      previousLeaderTerm = currentLeaderTerm;
      killQueue.clear();
      lastResetTime.set(null);
    }
  }

  /**
   * Returns true if the kill queue is empty or if the queue has not been reset
   * yet or if {@code (lastResetTime + resetPeriod) < (now + 1)}.
   */
  private boolean shouldRebuildKillQueue()
  {
    final DateTime now = DateTimes.nowUtc().plus(1);

    return killQueue.isEmpty()
           || lastResetTime.get() == null
           || lastResetTime.get().plus(QUEUE_RESET_PERIOD).isBefore(now);
  }

  /**
   * Clears the kill queue and adds fresh jobs.
   * This method need not handle race conditions as it is always run on
   * {@link #exec} which is single-threaded.
   */
  private void rebuildKillQueue()
  {
    final Stopwatch resetDuration = Stopwatch.createStarted();
    try {
      killQueue.clear();
      if (!leaderSelector.isLeader()) {
        log.info("Not rebuilding kill queue as we are not leader anymore.");
        return;
      }

      final Map<String, Integer> dataSourceToIntervalCounts = new HashMap<>();

      // Identify intervals with unused segments which are eligible for kill
      final Map<DatasourceInterval, Integer> eligibleIntervals =
          storageCoordinator.retrieveSomeUnusedSegmentIntervals(
              killConfig.getMaxUpdatedTimeOfKillableSegment(),
              MAX_INTERVALS_TO_KILL,
              killConfig.getMaxSegmentsToKill()
          );

      // Add kill candidates to the queue
      eligibleIntervals.forEach((entry, numEligibleSegments) -> {
        dataSourceToIntervalCounts.merge(entry.dataSource(), 1, Integer::sum);

        // Queue multiple candidates for the same datasource-interval if the
        // number of segments to kill is large to avoid holding a lock for too long
        int remainingSegmentsToKill = numEligibleSegments;
        while (remainingSegmentsToKill > 0) {
          int numSegmentsToKill
              = Math.min(remainingSegmentsToKill, MAX_SEGMENTS_TO_KILL_IN_TASK);
          remainingSegmentsToKill -= numSegmentsToKill;

          // If only a few segments remain, add them to the same candidate
          if (remainingSegmentsToKill < MAX_SEGMENTS_TO_KILL_IN_BATCH) {
            numSegmentsToKill += remainingSegmentsToKill;
            remainingSegmentsToKill = 0;
          }

          killQueue.offer(
              new KillCandidate(entry.dataSource(), entry.interval(), numSegmentsToKill)
          );
        }

        emitMetric(
            Metric.ELIGIBLE_UNUSED_SEGMENTS,
            numEligibleSegments,
            Map.of(
                DruidMetrics.DATASOURCE, entry.dataSource(),
                DruidMetrics.INTERVAL, entry.interval().toString()
            )
        );
      });

      lastResetTime.set(DateTimes.nowUtc());
      log.info(
          "Queued [%d] kill jobs for [%d] datasources in [%d] millis.",
          killQueue.size(), dataSourceToIntervalCounts.size(), resetDuration.millisElapsed()
      );
      dataSourceToIntervalCounts.forEach(
          (dataSource, intervalCount) -> emitMetric(
              Metric.UNUSED_SEGMENT_INTERVALS,
              intervalCount,
              Map.of(DruidMetrics.DATASOURCE, dataSource)
          )
      );
      emitMetric(Metric.QUEUE_RESET_TIME, resetDuration.millisElapsed(), null);
    }
    catch (Throwable t) {
      log.makeAlert(t, "Error while resetting kill queue.");
    }
  }

  /**
   * Launches an {@link EmbeddedKillTask} on the {@link #exec} for the next
   * {@link KillCandidate} in the {@link #killQueue}. This method returns
   * immediately.
   */
  private void startNextJobInKillQueue()
  {
    if (!isEnabled() || !leaderSelector.isLeader()) {
      return;
    }

    if (killQueue.isEmpty()) {
      // If the last entry has been processed, emit the total processing time and exit
      final DateTime lastQueueResetTime = lastResetTime.get();
      if (lastQueueResetTime != null) {
        long processTimeMillis = DateTimes.nowUtc().getMillis() - lastQueueResetTime.getMillis();
        emitMetric(Metric.QUEUE_PROCESS_TIME, processTimeMillis, null);
      }
      return;
    }

    try {
      final KillCandidate candidate = killQueue.poll();
      if (candidate == null) {
        return;
      }

      final String taskId = IdUtils.newTaskId(
          TASK_ID_PREFIX,
          KillUnusedSegmentsTask.TYPE,
          candidate.dataSource(),
          candidate.interval()
      );

      final Future<?> taskFuture = exec.submit(() -> {
        runKillTask(candidate, taskId);
        startNextJobInKillQueue();
      });
      currentTaskInfo.set(new TaskInfo(taskId, taskFuture));
    }
    catch (Throwable t) {
      log.makeAlert(t, "Error while processing kill queue.");
      currentTaskInfo.set(null);
    }
  }

  /**
   * Launches an embedded kill task for the given candidate.
   */
  private void runKillTask(KillCandidate candidate, String taskId)
  {
    final Stopwatch taskRunTime = Stopwatch.createStarted();
    final EmbeddedKillTask killTask = new EmbeddedKillTask(
        taskId,
        candidate,
        killConfig.getMaxUpdatedTimeOfKillableSegment()
    );

    final TaskActionClient taskActionClient = taskActionClientFactory.create(killTask);
    final TaskToolbox taskToolbox = KillTaskToolbox.create(taskActionClient, dataSegmentKiller, emitter);

    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, killTask);
    metricBuilder.setDimension(DruidMetrics.INTERVAL, candidate.interval());

    try {
      taskLockbox.add(killTask);
      final boolean isReady = killTask.isReady(taskActionClient);
      if (!isReady) {
        emitter.emit(metricBuilder.setMetric(Metric.SKIPPED_INTERVALS, 1L));
        return;
      }

      final TaskStatus status = killTask.runTask(taskToolbox);

      IndexTaskUtils.setTaskStatusDimensions(metricBuilder, status);
      emitter.emit(metricBuilder.setMetric(TaskMetrics.RUN_DURATION, taskRunTime.millisElapsed()));
    }
    catch (Throwable t) {
      log.error(t, "Embedded kill task[%s] failed.", killTask.getId());

      IndexTaskUtils.setTaskStatusDimensions(metricBuilder, TaskStatus.failure(taskId, "Unknown error"));
      emitter.emit(metricBuilder.setMetric(TaskMetrics.RUN_DURATION, taskRunTime.millisElapsed()));
    }
    finally {
      cleanupLocksSilently(killTask);
      emitMetric(Metric.PROCESSED_KILL_JOBS, 1L, Map.of(DruidMetrics.DATASOURCE, candidate.dataSource()));
    }
  }

  private void cleanupLocksSilently(EmbeddedKillTask killTask)
  {
    try {
      taskLockbox.remove(killTask);
    }
    catch (Throwable t) {
      log.error(t, "Error while cleaning up locks for kill task[%s].", killTask.getId());
    }
  }

  private void emitMetric(String metricName, long value, Map<String, String> dimensions)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    if (dimensions != null) {
      dimensions.forEach(builder::setDimension);
    }
    emitter.emit(builder.setMetric(metricName, value));
  }

  /**
   * Represents a single candidate interval that contains unused segments.
   */
  private record KillCandidate(String dataSource, Interval interval, int numSegmentsToKill)
  {

  }

  /**
   * Info of the currently running task.
   */
  private static class TaskInfo
  {
    private final Stopwatch sinceTaskStarted;
    private final String taskId;
    private final Future<?> future;

    private TaskInfo(String taskId, Future<?> future)
    {
      this.future = future;
      this.taskId = taskId;
      this.sinceTaskStarted = Stopwatch.createStarted();
    }
  }

  /**
   * Embedded kill task. Unlike other task types, this task is not persisted and
   * does not run on a worker or indexer. Hence, it doesn't take up any task slots.
   * To ensure that locks are held very briefly over short segment intervals,
   * this kill task processes:
   * <ul>
   * <li>only 1 unused segment interval</li>
   * <li>only 1 batch of upto 1000 unused segments</li>
   * </ul>
   */
  private class EmbeddedKillTask extends KillUnusedSegmentsTask
  {
    private EmbeddedKillTask(
        String taskId,
        KillCandidate candidate,
        DateTime maxUpdatedTimeOfEligibleSegment
    )
    {
      super(
          taskId,
          candidate.dataSource(),
          candidate.interval(),
          null,
          Map.of(
              Tasks.PRIORITY_KEY, Tasks.DEFAULT_EMBEDDED_KILL_TASK_PRIORITY,
              Tasks.TASK_LOCK_TYPE, TaskLockType.KILL.name()
          ),
          MAX_SEGMENTS_TO_KILL_IN_BATCH,
          candidate.numSegmentsToKill(),
          maxUpdatedTimeOfEligibleSegment
      );
    }

    @Override
    public String getType()
    {
      return TASK_TYPE_EMBEDDED_KILL;
    }

    @Override
    protected List<DataSegmentPlus> fetchNextBatchOfUnusedSegments(TaskToolbox toolbox, int nextBatchSize)
    {
      return storageCoordinator.retrieveUnusedSegmentsWithExactInterval(
          getDataSource(),
          getInterval(),
          getMaxUsedStatusLastUpdatedTime(),
          nextBatchSize
      );
    }

    @Override
    protected Map<String, String> fetchParentIdsForSegments(
        TaskToolbox toolbox,
        Map<String, DataSegmentPlus> unusedIdToSegmentPlus
    )
    {
      // No need to make another DB call, the parent IDs have already been fetched
      // in fetchNextBatchOfUnusedSegments
      final Map<String, String> unusedSegmentIdToParentId = new HashMap<>();
      for (DataSegmentPlus segment : unusedIdToSegmentPlus.values()) {
        if (segment.getUpgradedFromSegmentId() != null) {
          unusedSegmentIdToParentId.put(
              segment.getDataSegment().getId().toString(),
              segment.getUpgradedFromSegmentId()
          );
        }
      }

      return unusedSegmentIdToParentId;
    }

    @Override
    protected void logInfo(String message, Object... args)
    {
      // Reduce the level of embedded task info logs to reduce noise on the Overlord
      log.debug(message, args);
    }
  }

  public static class Metric
  {
    public static final String QUEUE_RESET_TIME = "segment/kill/queueReset/time";
    public static final String QUEUE_PROCESS_TIME = "segment/kill/queueProcess/time";
    public static final String PROCESSED_KILL_JOBS = "segment/kill/jobsProcessed/count";

    public static final String SKIPPED_INTERVALS = "segment/kill/skippedIntervals/count";
    public static final String UNUSED_SEGMENT_INTERVALS = "segment/kill/unusedIntervals/count";
    public static final String ELIGIBLE_UNUSED_SEGMENTS = "segment/kill/eligibleSegments/count";
  }
}
