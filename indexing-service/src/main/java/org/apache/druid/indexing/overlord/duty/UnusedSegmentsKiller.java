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
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.KillUnusedSegmentsTask;
import org.apache.druid.indexing.common.task.TaskMetrics;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SegmentsMetadataManagerConfig;
import org.apache.druid.metadata.UnusedSegmentKillerConfig;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link OverlordDuty} to delete unused segments from metadata store and the
 * deep storage. Launches {@link EmbeddedKillTask}s to clean unused segments
 * of a single datasource-interval.
 * <p>
 * @see SegmentsMetadataManagerConfig to enable the cleanup
 */
public class UnusedSegmentsKiller implements OverlordDuty
{
  private static final Logger log = new Logger(UnusedSegmentsKiller.class);

  private static final String TASK_ID_PREFIX = "overlord-issued";

  /**
   * Period after which the queue is reset even if there are existing jobs in queue.
   */
  private static final Duration QUEUE_RESET_PERIOD = Duration.standardDays(1);

  private final ServiceEmitter emitter;
  private final TaskLockbox taskLockbox;
  private final DruidLeaderSelector leaderSelector;
  private final DataSegmentKiller dataSegmentKiller;

  private final UnusedSegmentKillerConfig killConfig;
  private final TaskActionClientFactory taskActionClientFactory;
  private final IndexerMetadataStorageCoordinator storageCoordinator;

  private final ScheduledExecutorService exec;
  private int previousLeaderTerm;
  private final AtomicReference<DateTime> lastResetTime = new AtomicReference<>(null);

  /**
   * Queue of kill candidates. Use a PriorityBlockingQueue to ensure thread-safety
   * since this queue is accessed by both {@link #run()} and {@link #processKillQueue()}.
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
      TaskLockbox taskLockbox,
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
          1000,
          Ordering.from(Comparators.intervalsByEndThenStart())
                  .onResultOf(candidate -> candidate.interval)
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
    if (shouldResetKillQueue()) {
      // Clear the killQueue to stop further processing of already queued jobs
      killQueue.clear();
      exec.submit(this::resetKillQueue);
    }

    // TODO: do something if a single job has been stuck since the last duty run
  }

  @Override
  public DutySchedule getSchedule()
  {
    if (isEnabled()) {
      // Check every hour that the kill queue is being processed normally
      return new DutySchedule(Duration.standardHours(1).getMillis(), Duration.standardMinutes(1).getMillis());
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

  private boolean shouldResetKillQueue()
  {
    // Perform reset if (lastResetTime + resetPeriod < now + 1)
    final DateTime now = DateTimes.nowUtc().plus(1);

    return killQueue.isEmpty()
           || lastResetTime.get() == null
           || lastResetTime.get().plus(QUEUE_RESET_PERIOD).isBefore(now);
  }

  /**
   * Resets the kill queue with fresh jobs.
   */
  private void resetKillQueue()
  {
    final Stopwatch resetDuration = Stopwatch.createStarted();
    try {
      killQueue.clear();

      final Set<String> dataSources = storageCoordinator.retrieveAllDatasourceNames();

      final Map<String, Integer> dataSourceToIntervalCounts = new HashMap<>();
      for (String dataSource : dataSources) {
        storageCoordinator.retrieveUnusedSegmentIntervals(dataSource, 10_000).forEach(
            interval -> {
              dataSourceToIntervalCounts.merge(dataSource, 1, Integer::sum);
              killQueue.offer(new KillCandidate(dataSource, interval));
            }
        );
      }

      lastResetTime.set(DateTimes.nowUtc());
      log.info(
          "Queued kill jobs for [%d] intervals across [%d] datasources in [%d] millis.",
          killQueue.size(), dataSources.size(), resetDuration.millisElapsed()
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
      log.error(
          t,
          "Failed while queueing kill jobs after [%d] millis. There are [%d] jobs in queue.",
          resetDuration.millisElapsed(), killQueue.size()
      );
    }

    if (!killQueue.isEmpty()) {
      processKillQueue();
    }
  }

  /**
   * Cleans up all unused segments added to the {@link #killQueue} by the last
   * {@link #run()} of this duty. An {@link EmbeddedKillTask} is launched for
   * each eligible unused interval of a datasource
   */
  public void processKillQueue()
  {
    if (!isEnabled()) {
      return;
    }

    final Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      while (!killQueue.isEmpty() && leaderSelector.isLeader()) {
        final KillCandidate candidate = killQueue.poll();
        if (candidate == null) {
          return;
        }

        final String taskId = IdUtils.newTaskId(
            TASK_ID_PREFIX,
            KillUnusedSegmentsTask.TYPE,
            candidate.dataSource,
            candidate.interval
        );
        runKillTask(candidate, taskId);
      }
    }
    catch (Throwable t) {
      log.error(t, "Error while processing queued kill jobs.");
    }
    finally {
      emitMetric(Metric.QUEUE_PROCESS_TIME, stopwatch.millisElapsed(), null);
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
        DateTimes.nowUtc().minus(killConfig.getBufferPeriod())
    );

    final TaskActionClient taskActionClient = taskActionClientFactory.create(killTask);
    final TaskToolbox taskToolbox = KillTaskToolbox.create(taskActionClient, dataSegmentKiller, emitter);

    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, killTask);

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
      log.error(t, "Embedded kill task[%s] failed", killTask.getId());

      IndexTaskUtils.setTaskStatusDimensions(metricBuilder, TaskStatus.failure(taskId, "Unknown error"));
      emitter.emit(metricBuilder.setMetric(TaskMetrics.RUN_DURATION, taskRunTime.millisElapsed()));
    }
    finally {
      taskLockbox.remove(killTask);
      emitMetric(Metric.PROCESSED_KILL_JOBS, 1L, Map.of(DruidMetrics.DATASOURCE, candidate.dataSource));
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
  private static class KillCandidate
  {
    private final String dataSource;
    private final Interval interval;

    private KillCandidate(String dataSource, Interval interval)
    {
      this.dataSource = dataSource;
      this.interval = interval;
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
          candidate.dataSource,
          candidate.interval,
          null,
          Map.of(Tasks.PRIORITY_KEY, 25),
          null,
          null,
          maxUpdatedTimeOfEligibleSegment
      );
    }

    @Nullable
    @Override
    protected Integer getNumTotalBatches()
    {
      // Do everything in a single batch so that locks are not held for very long
      return 1;
    }

    @Override
    protected List<DataSegment> fetchNextBatchOfUnusedSegments(TaskToolbox toolbox, int nextBatchSize)
    {
      // Kill only 1000 segments in the batch so that locks are not held for very long
      return storageCoordinator.retrieveUnusedSegmentsWithExactInterval(
          getDataSource(),
          getInterval(),
          getMaxUsedStatusLastUpdatedTime(),
          1000
      );
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
  }
}
