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

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.common.guava.FutureUtils;
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
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.query.http.SqlTaskStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for task management for the scheduled batch supervisor.
 */
public class ScheduledBatchTaskManager
{
  private static final Logger log = new EmittingLogger(ScheduledBatchTaskManager.class);

  private final TaskRunnerListener taskRunnerListener;
  private final TaskMaster taskMaster;
  private final ScheduledBatchStatusTracker statusTracker;
  private final BrokerClient brokerClient;
  private final ServiceEmitter emitter;

  /**
   * Single-threaded executor to schedule jobs that is shared across scheduled batch supervisors.
   */
  private final ScheduledExecutorService jobsExecutor;

  private final ConcurrentHashMap<String, SchedulerManager> supervisorToManager
      = new ConcurrentHashMap<>();

  @Inject
  public ScheduledBatchTaskManager(
      final TaskMaster taskMaster,
      final ScheduledExecutorFactory executorFactory,
      final BrokerClient brokerClient,
      final ServiceEmitter emitter,
      final ScheduledBatchStatusTracker statusTracker
  )
  {
    this.taskMaster = taskMaster;
    this.brokerClient = brokerClient;
    this.emitter = emitter;
    this.statusTracker = statusTracker;

    this.jobsExecutor = executorFactory.create(1, "ScheduledBatchJobsExecutor-%s");

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

  /**
   * Initializes and starts scheduled ingestion for the specified {@code supervisorId}, scheduling tasks
   * using the provided {@code spec} and {@code cronSchedulerConfig}.
   */
  public void startScheduledIngestion(
      final String supervisorId,
      final String dataSource,
      final CronSchedulerConfig cronSchedulerConfig,
      final ClientSqlQuery spec
  )
  {
    log.info(
        "Starting scheduled batch ingestion for supervisorId[%s] with datasource[%s], cronSchedule[%s] and spec[%s].",
        supervisorId, dataSource, cronSchedulerConfig, spec
    );
    final SchedulerManager manager = new SchedulerManager(supervisorId, dataSource, cronSchedulerConfig, spec);
    manager.startScheduling();
    supervisorToManager.put(supervisorId, manager);
  }

  /**
   * Stops the scheduler for the specified {@code supervisorId}, pausing task submissions
   * while retaining the ability to track the supervisor's status.
   */
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
    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().registerListener(taskRunnerListener, Execs.directExecutor());
    } else {
      log.warn("Task runner not registered for scheduled batch scheduler.");
    }
  }

  public void stop()
  {
    log.info("Stopping scheduled batch scheduler.");
    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().unregisterListener(taskRunnerListener.getListenerId());
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

  private void submitSqlTask(final String supervisorId, final ClientSqlQuery spec)
      throws ExecutionException, InterruptedException
  {
    log.debug("Submitting a new task with spec[%s] for supervisor[%s].", spec, supervisorId);
    final SqlTaskStatus taskStatus = FutureUtils.get(brokerClient.submitSqlTask(spec), true);
    statusTracker.onTaskSubmitted(supervisorId, taskStatus);
  }

  private class SchedulerManager
  {
    private final String supervisorId;
    private final String dataSource;
    private final ClientSqlQuery spec;
    private final CronSchedulerConfig cronSchedulerConfig;

    private ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus status;

    /**
     * TODO: Note that the last task submitted per supervisor should eventually be persisted in the metadata store,
     * along with any other scheduler state, so that the batch supervisor can recover gracefully and
     * avoid missing task submissions during rolling restarts, etc.
     */
    private volatile DateTime lastTaskSubmittedTime;

    private SchedulerManager(
        final String supervisorId,
        final String dataSource,
        final CronSchedulerConfig cronSchedulerConfig,
        final ClientSqlQuery spec
    )
    {
      this.supervisorId = supervisorId;
      this.dataSource = dataSource;
      this.cronSchedulerConfig = cronSchedulerConfig;
      this.spec = spec;
    }

    private synchronized void startScheduling()
    {
      if (jobsExecutor.isTerminated() || jobsExecutor.isShutdown()
          || status == ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_SHUTDOWN) {
        return;
      }

      status = ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_RUNNING;
      final Duration timeUntilNextSubmission = getTimeUntilNextTaskSubmission();
      if (timeUntilNextSubmission == null) {
        log.info("No more tasks will be submitted for supervisor[%s].", supervisorId);
        return;
      }

      jobsExecutor.schedule(
          () -> {
            try {
              // Check status inside the runnable again before submitting any tasks
              if (status == ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_SHUTDOWN) {
                return;
              }
              lastTaskSubmittedTime = DateTimes.nowUtc();
              submitSqlTask(supervisorId, spec);
              emitMetric("batchSupervisor/tasks/submit/success", 1);
            }
            catch (Exception e) {
              emitMetric("batchSupervisor/tasks/submit/failed", 1);
              log.error(e, "Error submitting task for supervisor[%s]. Continuing schedule.", supervisorId);
            }
            startScheduling(); // Schedule the next task
          },
          timeUntilNextSubmission.getMillis(),
          TimeUnit.MILLISECONDS
      );
    }

    private synchronized void stopScheduling()
    {
      status = ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus.SCHEDULER_SHUTDOWN;
    }

    private void emitMetric(final String metricName, final int value)
    {
      emitter.emit(
          ServiceMetricEvent.builder()
                            .setDimension("supervisorId", supervisorId)
                            .setDimension("dataSource", dataSource)
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
      return cronSchedulerConfig.getNextTaskStartTimeAfter(DateTimes.nowUtc());
    }

    @Nullable
    private Duration getTimeUntilNextTaskSubmission()
    {
      return cronSchedulerConfig.getDurationUntilNextTaskStartTimeAfter(DateTimes.nowUtc());
    }

    private ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus getSchedulerStatus()
    {
      return status;
    }
  }
}
