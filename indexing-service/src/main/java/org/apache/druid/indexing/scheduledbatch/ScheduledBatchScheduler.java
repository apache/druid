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

package org.apache.druid.indexing.scheduledbatch;

import com.cronutils.model.time.ExecutionTime;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Metrics:
 * 1. Number of supervisor schedulers started and stopped.
 * 2. Number of jobs submitted per supervisor
 * 3. Number of jobs submitted completed per supervisor
 * 4. Number of jobs errored out per supervisor
 */
// Metrics: 1. number of supervisors increment when start/stop is called. 2. number of jobs 3. number of
public class ScheduledBatchScheduler
{
  private static final Logger log = new EmittingLogger(ScheduledBatchScheduler.class);

  private final TaskRunnerListener taskRunnerListener;
  private final TaskMaster taskMaster;
  private final ScheduledBatchStatusTracker statusTracker;
  private final BrokerClient brokerClient;
  private final ServiceEmitter emitter;

  /**
   * Single-threaded executor to process the cron jobs queue.
   */
  private final ScheduledExecutorService cronExecutor;

  private final ConcurrentHashMap<String, SchedulerManager> supervisorToManager
      = new ConcurrentHashMap<>();

  @Inject
  public ScheduledBatchScheduler(
      final TaskMaster taskMaster,
      final ScheduledExecutorFactory executorFactory,
      final BrokerClient brokerClient,
      final ServiceEmitter emitter
  )
  {
    this.taskMaster = taskMaster;
    this.cronExecutor = executorFactory.create(1, "ScheduledBatchCronScheduler-%s");
    this.brokerClient = brokerClient;
    this.emitter = emitter;
    this.statusTracker = new ScheduledBatchStatusTracker(); // this can perhaps be injected and used for test verification.

    this.taskRunnerListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "ScheduledBatchScheduler";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {
        // Do nothing
      }

      @Override
      public void statusChanged(final String taskId, final TaskStatus taskStatus)
      {
        if (taskStatus.isComplete()) {
          statusTracker.onTaskCompleted(taskId, taskStatus);
        }
      }
    };
  }

  public void startScheduledIngestion(
      final String supervisorId,
      final CronSchedulerConfig cronSchedulerConfig,
      final SqlQuery spec
  )
  {
    log.info(
        "Starting scheduled batch ingestion for supervisorId[%s] with cronSchedule[%s] and spec[%s].",
        supervisorId, cronSchedulerConfig, spec
    );
    final SchedulerManager manager = new SchedulerManager(supervisorId, cronSchedulerConfig, spec);
    manager.startScheduling();
    supervisorToManager.put(supervisorId, manager);
  }

  public void stopScheduledIngestion(final String supervisorId)
  {
    log.info("Stopping scheduled batch ingestion for supervisorId[%s].", supervisorId);
    final SchedulerManager manager = supervisorToManager.get(supervisorId);
    if (manager != null) {
      // Don't remove the supervisorId from supervisorToManager. We want to be able to track the status
      // for suspended supervisors to track any in-flight tasks. stop() will clean up all state completely.
      manager.stopScheduling();
    }
  }

  public void start()
  {
    log.info("Starting scheduled batch scheduler.");
    final com.google.common.base.Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().registerListener(taskRunnerListener, Execs.directExecutor());
    } else {
      log.warn("Task runner not registered for scheduled batch scheduler.");
    }
  }

  public void stop()
  {
    log.info("Stopping scheduled batch scheduler.");
    final com.google.common.base.Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().unregisterListener(taskRunnerListener.getListenerId());
      log.info("Task runner unregistered.");
    }

    supervisorToManager.clear();
  }

  @Nullable
  public ScheduledBatchSupervisorSnapshot getSchedulerSnapshot(final String supervisorId)
  {
    final SchedulerManager manager = supervisorToManager.get(supervisorId);
    if (manager == null) {
      return null;
    }

    final ScheduledBatchStatusTracker.BatchSupervisorTaskStatus tasks = statusTracker.getSupervisorTasks(supervisorId);
    return new ScheduledBatchSupervisorSnapshot(
        supervisorId,
        manager.getSchedulerStatus(),
        manager.getLastTaskSubmittedTime(),
        manager.getNextTaskSubmissionTime(),
        manager.getTimeUntilNextTaskSubmission(),
        tasks.getSubmittedTasks(),
        tasks.getCompletedTasks()
    );
  }

  private void enqueueTask(final Runnable runnable)
  {
    cronExecutor.submit(runnable);
  }

  private void submitSqlTask(final String supervisorId, final SqlQuery spec)
      throws ExecutionException, InterruptedException
  {
    log.info("Submitting a new task with spec[%s] for supervisor[%s].", spec, supervisorId);
    final ListenableFuture<SqlTaskStatus> sqlTaskStatusListenableFuture = brokerClient.submitTask(spec);
    final SqlTaskStatus sqlTaskStatus;
    sqlTaskStatus = sqlTaskStatusListenableFuture.get();
    statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus);
  }

  private class SchedulerManager
  {
    private final String supervisorId;
    private final SqlQuery spec;
    private final ExecutionTime executionTime;
    private final ScheduledExecutorService executorService;

    private ScheduledBatchSupervisorPayload.BatchSupervisorStatus status;

    /**
     * Note that the last task submitted per supervisor should eventually be persisted in the metadata store,
     * along with any other scheduler state, so that the batch supervisor can recover gracefully and
     * avoid missing task submissions during rolling restarts, etc.
     */
    private DateTime lastTaskSubmittedTime;

    private SchedulerManager(
        final String supervisorId,
        final CronSchedulerConfig cronSchedulerConfig,
        final SqlQuery spec
    )
    {
      this.supervisorId = supervisorId;
      this.spec = spec;
      this.executionTime = ExecutionTime.forCron(cronSchedulerConfig.getCron());
      this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    private synchronized void startScheduling()
    {
      if (executorService.isTerminated() || executorService.isShutdown()) {
        return;
      }

      status = ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING;
      final Duration timeUntilNextSubmission = getTimeUntilNextTaskSubmission();
      if (timeUntilNextSubmission == null) {
        log.info("No more tasks will be submitted for supervisor[%s].", supervisorId);
        return;
      }

      executorService.schedule(
          () -> {
            enqueueTask(() -> {
              try {
                lastTaskSubmittedTime = DateTimes.nowUtc();
                submitSqlTask(supervisorId, spec);
                emitMetric("batchSupervisor/tasks/submit/success", 1);
              }
              catch (Exception e) {
                emitMetric("batchSupervisor/tasks/submit/failed", 1);
                log.error(e, "Error submitting task for supervisor[%s]. Continuing schedule.", supervisorId);
              }
            });
            startScheduling();
          },
          timeUntilNextSubmission.getMillis(),
          TimeUnit.MILLISECONDS
      );
    }

    private synchronized void stopScheduling()
    {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        log.error(e, "Forcing shutdown of executor service for supervisor[%s].", supervisorId);
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
      status = ScheduledBatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN;
    }

    private void emitMetric(final String metricName, final int value)
    {
      emitter.emit(
          ServiceMetricEvent.builder()
              .setDimension("supervisorId", supervisorId)
              .setMetric(metricName, value)
      );
    }

    @Nullable
    private DateTime getLastTaskSubmittedTime()
    {
      return lastTaskSubmittedTime;
    }

    @Nullable
    private DateTime getNextTaskSubmissionTime()
    {
      final Optional<ZonedDateTime> zonedDateTime = executionTime.nextExecution(ZonedDateTime.now());
      if (zonedDateTime.isPresent()) {
        final ZonedDateTime zdt = zonedDateTime.get();
        final Instant instant = zdt.toInstant();
        return new DateTime(instant.toEpochMilli(), DateTimes.inferTzFromString(zdt.getZone().getId()));
      } else {
        return null;
      }
    }

    @Nullable
    private Duration getTimeUntilNextTaskSubmission()
    {
      final Optional<java.time.Duration> duration = executionTime.timeToNextExecution(ZonedDateTime.now());
      return duration.map(value -> Duration.millis(value.toMillis())).orElse(null);
    }

    private ScheduledBatchSupervisorPayload.BatchSupervisorStatus getSchedulerStatus()
    {
      return status;
    }
  }
}
