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
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.overlord.GlobalTaskLockbox;
import org.apache.druid.indexing.template.BatchIndexingJob;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionCandidateSearchPolicy;
import org.apache.druid.server.compaction.CompactionSlotManager;
import org.apache.druid.server.compaction.CompactionSnapshotBuilder;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.server.compaction.CompactionStatusTracker;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionConfigValidationResult;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Iterates over all eligible compaction jobs in order of their priority.
 * A fresh instance of this class must be used in every run of the
 * {@link CompactionScheduler}.
 * <p>
 * Unlike the Coordinator duty {@code CompactSegments}, the job queue currently
 * does not cancel running compaction tasks even if their target segment
 * granularity has changed. This has not been done here for simplicity since the
 * {@code CompactionJobQueue} uses compaction templates and may have a different
 * target segment granulariy for different intervals of the same datasource.
 * The cancellation of invalid tasks has been left as a future enhancement.
 */
@NotThreadSafe
public class CompactionJobQueue
{
  private static final Logger log = new Logger(CompactionJobQueue.class);

  private final CompactionJobParams jobParams;
  private final CompactionCandidateSearchPolicy searchPolicy;
  private final ClusterCompactionConfig clusterCompactionConfig;

  private final ObjectMapper objectMapper;
  private final CompactionStatusTracker statusTracker;
  private final TaskActionClientFactory taskActionClientFactory;
  private final OverlordClient overlordClient;
  private final GlobalTaskLockbox taskLockbox;
  private final BrokerClient brokerClient;

  private final CompactionSnapshotBuilder snapshotBuilder;
  private final PriorityQueue<CompactionJob> queue;
  private final CoordinatorRunStats runStats;

  private final Set<String> activeSupervisors;
  private final Map<String, CompactionJob> submittedTaskIdToJob;

  public CompactionJobQueue(
      DataSourcesSnapshot dataSourcesSnapshot,
      ClusterCompactionConfig clusterCompactionConfig,
      CompactionStatusTracker statusTracker,
      TaskActionClientFactory taskActionClientFactory,
      GlobalTaskLockbox taskLockbox,
      OverlordClient overlordClient,
      BrokerClient brokerClient,
      ObjectMapper objectMapper
  )
  {
    this.runStats = new CoordinatorRunStats();
    this.snapshotBuilder = new CompactionSnapshotBuilder(runStats);
    this.clusterCompactionConfig = clusterCompactionConfig;
    this.searchPolicy = clusterCompactionConfig.getCompactionPolicy();
    this.queue = new PriorityQueue<>(
        (o1, o2) -> searchPolicy.compareCandidates(o1.getCandidate(), o2.getCandidate())
    );
    this.submittedTaskIdToJob = new HashMap<>();
    this.activeSupervisors = new HashSet<>();
    this.jobParams = new CompactionJobParams(
        DateTimes.nowUtc(),
        clusterCompactionConfig,
        dataSourcesSnapshot.getUsedSegmentsTimelinesPerDataSource()::get,
        snapshotBuilder
    );

    this.taskActionClientFactory = taskActionClientFactory;
    this.overlordClient = overlordClient;
    this.brokerClient = brokerClient;
    this.statusTracker = statusTracker;
    this.objectMapper = objectMapper;
    this.taskLockbox = taskLockbox;
  }

  /**
   * Creates jobs for the given {@link CompactionSupervisor} and adds them to
   * the job queue.
   * <p>
   * This method is idempotent. If jobs for the given supervisor already exist
   * in the queue, the method does nothing.
   */
  public void createAndEnqueueJobs(
      CompactionSupervisor supervisor,
      DruidInputSource source
  )
  {
    final Stopwatch jobCreationTime = Stopwatch.createStarted();
    final String supervisorId = supervisor.getSpec().getId();
    try {
      if (supervisor.shouldCreateJobs() && !activeSupervisors.contains(supervisorId)) {
        // Queue fresh jobs
        final List<CompactionJob> jobs = supervisor.createJobs(source, jobParams);
        jobs.forEach(job -> snapshotBuilder.addToPending(job.getCandidate()));

        queue.addAll(jobs);
        activeSupervisors.add(supervisorId);

        runStats.add(
            Stats.Compaction.CREATED_JOBS,
            RowKey.of(Dimension.DATASOURCE, source.getDataSource()),
            jobs.size()
        );
      } else {
        log.debug("Skipping job creation for supervisor[%s]", supervisorId);
      }
    }
    catch (Exception e) {
      log.error(e, "Error while creating jobs for supervisor[%s]", supervisorId);
    }
    finally {
      runStats.add(
          Stats.Compaction.JOB_CREATION_TIME,
          RowKey.of(Dimension.DATASOURCE, source.getDataSource()),
          jobCreationTime.millisElapsed()
      );
    }
  }

  /**
   * Removes all existing jobs for the given datasource from the queue.
   */
  public void removeJobs(String dataSource)
  {
    final List<CompactionJob> jobsToRemove = queue
        .stream()
        .filter(job -> job.getDataSource().equals(dataSource))
        .collect(Collectors.toList());

    queue.removeAll(jobsToRemove);
    log.info("Removed [%d] jobs for datasource[%s] from queue.", jobsToRemove.size(), dataSource);
  }

  /**
   * Submits jobs which are ready to either the Overlord or a Broker (if it is
   * an MSQ SQL job).
   */
  public void runReadyJobs()
  {
    final CompactionSlotManager slotManager = new CompactionSlotManager(
        overlordClient,
        statusTracker,
        clusterCompactionConfig
    );
    slotManager.reserveTaskSlotsForRunningCompactionTasks();

    final List<CompactionJob> pendingJobs = new ArrayList<>();
    while (!queue.isEmpty()) {
      final CompactionJob job = queue.poll();
      if (startJobIfPendingAndReady(job, searchPolicy, pendingJobs, slotManager)) {
        runStats.add(Stats.Compaction.SUBMITTED_TASKS, RowKey.of(Dimension.DATASOURCE, job.getDataSource()), 1);
      }
    }

    // Requeue pending jobs so that they can be launched when slots become available
    queue.addAll(pendingJobs);
  }

  /**
   * Notifies completion of the given so that the compaction snapshots may be
   * updated.
   */
  public void onTaskFinished(String taskId, TaskStatus taskStatus)
  {
    final CompactionJob job = submittedTaskIdToJob.remove(taskId);
    if (job == null || !taskStatus.getStatusCode().isComplete()) {
      // This is an unknown task ID
      return;
    }

    if (taskStatus.getStatusCode() == TaskState.FAILED) {
      // Add this job back to the queue
      queue.add(job);
    } else {
      snapshotBuilder.moveFromPendingToCompleted(job.getCandidate());
    }
  }

  public CoordinatorRunStats getRunStats()
  {
    return runStats;
  }

  /**
   * Builds compaction snapshots for all the datasources being tracked by this
   * queue.
   */
  public Map<String, AutoCompactionSnapshot> getSnapshots()
  {
    return snapshotBuilder.build();
  }

  /**
   * List of all jobs currently in queue. The order of the jobs is not guaranteed.
   */
  public List<CompactionJob> getQueuedJobs()
  {
    return List.copyOf(queue);
  }

  public List<CompactionCandidate> getFullyCompactedIntervals()
  {
    return snapshotBuilder.getCompleted();
  }

  public List<CompactionCandidate> getSkippedIntervals()
  {
    return snapshotBuilder.getSkipped();
  }

  public CompactionCandidateSearchPolicy getSearchPolicy()
  {
    return searchPolicy;
  }

  /**
   * Starts a job if it is ready and is not already in progress.
   *
   * @return true if the job was submitted successfully for execution
   */
  private boolean startJobIfPendingAndReady(
      CompactionJob job,
      CompactionCandidateSearchPolicy policy,
      List<CompactionJob> pendingJobs,
      CompactionSlotManager slotManager
  )
  {
    // Check if the job is a valid compaction job
    final CompactionCandidate candidate = job.getCandidate();
    final CompactionConfigValidationResult validationResult = validateCompactionJob(job);
    if (!validationResult.isValid()) {
      log.error("Skipping invalid compaction job[%s] due to reason[%s].", job, validationResult.getReason());
      snapshotBuilder.moveFromPendingToSkipped(candidate);
      return false;
    }

    // Check if the job is already running, completed or skipped
    final CompactionStatus compactionStatus = getCurrentStatusForJob(job, policy);
    switch (compactionStatus.getState()) {
      case RUNNING:
        return false;
      case COMPLETE:
        snapshotBuilder.moveFromPendingToCompleted(candidate);
        return false;
      case SKIPPED:
        snapshotBuilder.moveFromPendingToSkipped(candidate);
        return false;
      default:
        break;
    }

    // Check if enough compaction task slots are available
    if (job.getMaxRequiredTaskSlots() > slotManager.getNumAvailableTaskSlots()) {
      pendingJobs.add(job);
      return false;
    }

    // Reserve task slots and try to start the task
    slotManager.reserveTaskSlots(job.getMaxRequiredTaskSlots());
    final String taskId = startTaskIfReady(job);
    if (taskId == null) {
      // Mark the job as skipped for now as the intervals might be locked by other tasks
      snapshotBuilder.moveFromPendingToSkipped(candidate);
      return false;
    } else {
      statusTracker.onTaskSubmitted(taskId, job.getCandidate());
      submittedTaskIdToJob.put(taskId, job);
      return true;
    }
  }

  /**
   * Starts the given job if the underlying Task is able to acquire locks.
   *
   * @return Non-null taskId if the Task was submitted successfully.
   */
  @Nullable
  private String startTaskIfReady(CompactionJob job)
  {
    // Assume MSQ jobs to be always ready
    if (job.isMsq()) {
      try {
        return FutureUtils.getUnchecked(brokerClient.submitSqlTask(job.getNonNullMsqQuery()), true)
                          .getTaskId();
      }
      catch (Exception e) {
        log.error(e, "Error while submitting query[%s] to Broker", job.getNonNullMsqQuery());
      }
    }

    final ClientTaskQuery taskQuery = job.getNonNullTask();
    final Task task = objectMapper.convertValue(taskQuery, Task.class);

    log.debug(
        "Checking readiness of task[%s] with interval[%s]",
        task.getId(), job.getCandidate().getCompactionInterval()
    );
    try {
      taskLockbox.add(task);
      if (task.isReady(taskActionClientFactory.create(task))) {
        // Hold the locks acquired by task.isReady() as we will reacquire them anyway
        FutureUtils.getUnchecked(overlordClient.runTask(task.getId(), task), true);
        return task.getId();
      } else {
        taskLockbox.unlockAll(task);
        return null;
      }
    }
    catch (Exception e) {
      log.error(e, "Error while submitting task[%s] to Overlord", task.getId());
      taskLockbox.unlockAll(task);
      return null;
    }
  }

  private CompactionStatus getCurrentStatusForJob(CompactionJob job, CompactionCandidateSearchPolicy policy)
  {
    return statusTracker.computeCompactionStatus(job.getCandidate(), policy);
  }

  public static CompactionConfigValidationResult validateCompactionJob(BatchIndexingJob job)
  {
    // For MSQ jobs, do not perform any validation
    if (job.isMsq()) {
      return CompactionConfigValidationResult.success();
    }

    final ClientTaskQuery task = job.getNonNullTask();
    if (!(task instanceof ClientCompactionTaskQuery)) {
      return CompactionConfigValidationResult.failure("Invalid task type[%s]", task.getType());
    }

    return CompactionConfigValidationResult.success();
  }
}
