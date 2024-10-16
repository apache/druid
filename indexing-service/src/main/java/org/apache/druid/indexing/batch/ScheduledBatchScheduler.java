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

package org.apache.druid.indexing.batch;

import com.cronutils.model.time.ExecutionTime;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlTaskStatus;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduledBatchScheduler
{
  private static final Logger log = new EmittingLogger(ScheduledBatchScheduler.class);

  private final ObjectMapper objectMapper;
  private final TaskRunnerListener taskRunnerListener;
  private final TaskMaster taskMaster;
  private final BatchSupervisorStatusTracker statusTracker;

  private final ConcurrentHashMap<String, SchedulerManager> supervisorToManager
      = new ConcurrentHashMap<>();

  /**
   * Single-threaded executor to process the compaction queue.
   */
  private final ScheduledExecutorService cronExecutor;
  private final BrokerClient brokerClient;

  @Inject
  public ScheduledBatchScheduler(
      final TaskMaster taskMaster,
      final ScheduledExecutorFactory executorFactory,
      final ServiceEmitter emitter,
      final ObjectMapper objectMapper,
      final BrokerClient brokerClient2
  )
  {
    this.objectMapper = objectMapper;
    this.taskMaster = taskMaster;
    this.cronExecutor = executorFactory.create(1, "GlobalBatchCronScheduler-%s");
    this.statusTracker = new BatchSupervisorStatusTracker();
    this.brokerClient = brokerClient2;

    this.taskRunnerListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "GlobalBatchCronScheduler";
      }

      @Override
      public void locationChanged(String taskId, TaskLocation newLocation)
      {
        // Do nothing
      }

      @Override
      public void statusChanged(final String taskId, final TaskStatus taskStatus)
      {
        log.info("Task[%s] status changed to [%s]", taskId, taskStatus);
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
    final SchedulerManager state = supervisorToManager.get(supervisorId);
    if (state != null) {
      // Don't remove the supervisorId from supervisorToManager. We want to be able to track the status
      // for suspended supervisors to track any in-flight tasks. stop() will cleanup any state completely.
      state.stopScheduling();
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
  public BatchSupervisorSnapshot getSchedulerSnapshot(final String supervisorId)
  {
    final SchedulerManager state = supervisorToManager.get(supervisorId);
    if (state == null) {
      return null;
    }
    final DateTime previousExecutionTime = state.getPreviousExecutionTime();
    final Optional<DateTime> nextExecutionTime = state.getNextExecutionTime();
    final Optional<Duration> timeToNextExecution = state.getDurationBeforeNextRun();

    final BatchSupervisorStatusTracker.BatchSupervisorTaskStatus tasks = statusTracker.getSupervisorTasks(supervisorId);

    return new BatchSupervisorSnapshot(
        supervisorId,
        state.getSchedulerStatus(),
        previousExecutionTime != null ? previousExecutionTime.toString() : "Not run",
        nextExecutionTime.isPresent() ? nextExecutionTime.get().toString() : "N/A",
        timeToNextExecution.isPresent() ? timeToNextExecution.get().toString() : "N/A",
        tasks.getSubmittedTasks(),
        tasks.getCompletedTasks()
    );
  }

  private void enqueueWork(Runnable task)
  {
    cronExecutor.submit(task);
  }

  private void postMsqTaskPayloadViaNewBrokerClient(final String supervisorId, final SqlQuery spec)
  {
    final ListenableFuture<SqlTaskStatus> sqlTaskStatusListenableFuture = brokerClient.submitTask(spec);
    final SqlTaskStatus sqlTaskStatus;
    try {
      sqlTaskStatus = sqlTaskStatusListenableFuture.get();
    }
    catch (Exception e) {
      log.error(e, "Error getting taskId for supervisorId[%s] and spec[%s].", supervisorId, spec);
      return;
    }
    log.info("Received task status[%s] for supervisor[%s].", sqlTaskStatus, supervisorId);
    statusTracker.onTaskSubmitted(supervisorId, sqlTaskStatus);
  }

  private class SchedulerManager
  {
    private final String supervisorId;
    private final CronSchedulerConfig cronSchedulerConfig;
    private final SqlQuery spec;
    private final ExecutionTime executionTime;
    private final ScheduledExecutorService executorService;

    private BatchSupervisorPayload.BatchSupervisorStatus status;
    // Track the last know run time.
    private DateTime lastRunTime;

    private SchedulerManager(
        final String supervisorId,
        final CronSchedulerConfig cronSchedulerConfig,
        final SqlQuery spec
    )
    {
      this.supervisorId = supervisorId;
      this.cronSchedulerConfig = cronSchedulerConfig;
      this.spec = spec;
      this.executionTime = ExecutionTime.forCron(cronSchedulerConfig.getCron());
      this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    private synchronized void startScheduling()
    {
      if (executorService.isTerminated() || executorService.isShutdown()) {
        log.info(
            "Executor service for [%s] is shutdown[%s] or terminated[%s]",
            supervisorId,
            executorService.isShutdown(),
            executorService.isTerminated()
        );
        return;
      }

      status = BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_RUNNING;
      Optional<Duration> duration = executionTime.timeToNextExecution(ZonedDateTime.now());
      if (!duration.isPresent()) {
        // Should be a defensive exception that should never happen
        return;
      }
      final long nextExecutionMillis = duration.get().toMillis();
      log.info("Time to next execution for millis[%s] for supervisor[%s].", nextExecutionMillis, supervisorId);

      executorService.schedule(
          () -> {
            enqueueWork(() -> {
              try {
                lastRunTime = DateTimes.nowUtc();
                log.info("Submitting a new task with spec[%s] for supervisor[%s]!", spec, supervisorId);
                postMsqTaskPayloadViaNewBrokerClient(supervisorId, spec);
              }
              catch (Exception e) {
                log.error(e, "Exception submitting task for supervisor[%s]!", supervisorId);
              }
            });
            log.info("Supervisor[%s] enqueud a Runnable work [%s] now...", supervisorId, spec);
            startScheduling();
          },
          nextExecutionMillis,
          TimeUnit.MILLISECONDS
      );
    }

    private synchronized void stopScheduling()
    {
      status = BatchSupervisorPayload.BatchSupervisorStatus.SCHEDULER_SHUTDOWN;
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      }
      catch (InterruptedException e) {
        log.error(e, "Error shutting down supervisor[%s] executor pool. Interrupting the threads.", supervisorId);
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    private DateTime getPreviousExecutionTime()
    {
      return lastRunTime;
    }

    private Optional<DateTime> getNextExecutionTime()
    {
      final Optional<ZonedDateTime> zonedDateTime = executionTime.nextExecution(ZonedDateTime.now());

      if (zonedDateTime.isPresent()) {
        final ZonedDateTime zdt = zonedDateTime.get();
        final Instant instant = zdt.toInstant();
        return Optional.of(new DateTime(instant.toEpochMilli(), DateTimes.inferTzFromString(zdt.getZone().getId())));
      } else {
        return Optional.empty();
      }
    }

    private Optional<Duration> getDurationBeforeNextRun()
    {
      return executionTime.timeToNextExecution(ZonedDateTime.now());
    }

    private BatchSupervisorPayload.BatchSupervisorStatus getSchedulerStatus()
    {
      return status;
    }
  }
}
