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
import org.apache.druid.query.DruidMetrics;
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
 * Manages the task execution and tracking for the scheduled batch supervisor.
 * <p>
 * It submits MSQ tasks to the Broker and tracks their execution status. It listens for task state changes and updates
 * the {@link ScheduledBatchStatusTracker} accordingly.
 * </p>
 * <p>
 * The manager maintains a mapping of scheduled batch supervisors and their associated task scheduler,
 * so each supervisor can schedule its tasks based on the {@link CronSchedulerConfig}.
 * It uses a shared single-threaded {@link ScheduledExecutorService} to schedule tasks across all batch supervisors.
 * </p>
 * <p>
 * Note that all task state tracking by the batch supervisor is currently maintained in memory
 * and is not persisted in the metadata store.
 * </p>
 */
public class ScheduledBatchTaskManager
{
  private static final Logger log = new EmittingLogger(ScheduledBatchTaskManager.class);

  private final TaskRunnerListener taskRunnerListener;
  private final TaskMaster taskMaster;
  private final ScheduledBatchStatusTracker statusTracker;
  private final BrokerClient brokerClient;
  private final ServiceEmitter emitter;

  private final ScheduledExecutorService tasksExecutor;

  private final ConcurrentHashMap<String, ScheduledBatchTask> supervisorToTaskScheduler = new ConcurrentHashMap<>();

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

    this.tasksExecutor = executorFactory.create(1, "ScheduledBatchTaskManager-%s");

    this.taskRunnerListener = new TaskRunnerListener()
    {
      @Override
      public String getListenerId()
      {
        return "ScheduledBatchTaskManager";
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
        "Starting scheduled batch ingestion into datasource[%s] with supervisorId[%s], cronSchedule[%s] and spec[%s].",
        supervisorId, dataSource, cronSchedulerConfig, spec
    );
    final ScheduledBatchTask taskScheduler = new ScheduledBatchTask(supervisorId, dataSource, cronSchedulerConfig, spec);
    taskScheduler.startScheduling();
    supervisorToTaskScheduler.put(supervisorId, taskScheduler);
  }

  /**
   * Stops the scheduler for the specified {@code supervisorId}, pausing task submissions
   * while retaining the ability to track the supervisor's status.
   */
  public void stopScheduledIngestion(final String supervisorId)
  {
    log.info("Stopping scheduled batch ingestion for supervisorId[%s].", supervisorId);
    final ScheduledBatchTask taskScheduler = supervisorToTaskScheduler.get(supervisorId);
    if (taskScheduler != null) {
      // Don't remove the supervisorId from supervisorToTaskScheduler. We want to be able to track the status
      // for suspended supervisors to track any in-flight tasks. stop() will clean up all state completely.
      taskScheduler.stopScheduling();
    }
  }

  /**
   * Starts the scheduled batch task manager by registering the {@link TaskRunnerListener}.
   * This allows tracking of any tasks submitted by the batch supervisor.
   * <p>
   * Should be invoked when the Overlord service starts or during leadership transitions.
   * </p>
   */
  public void start()
  {
    log.info("Starting scheduled batch task manager.");
    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().registerListener(taskRunnerListener, Execs.directExecutor());
    } else {
      log.warn("Task runner not registered for scheduled batch task manager.");
    }
  }

  /**
   * Stops the scheduled batch task manager by shutting down all scheduled batch supervisors and
   * unregistering the registered {@link TaskRunnerListener}.
   * <p>
   * Should be invoked when the Overlord service stops or during leadership transitions.
   * </p>
   */
  public void stop()
  {
    log.info("Stopping scheduled batch task manager.");
    supervisorToTaskScheduler.forEach((supervisorId, taskScheduler) -> {
      taskScheduler.stopScheduling();
    });
    supervisorToTaskScheduler.clear();

    final Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (taskRunnerOptional.isPresent()) {
      taskRunnerOptional.get().unregisterListener(taskRunnerListener.getListenerId());
    }
  }

  /**
   * Get the supervisor status for the specified {@code supervisorId} or null if there's no such
   * supervisor.
   */
  @Nullable
  public ScheduledBatchSupervisorStatus getSupervisorStatus(final String supervisorId)
  {
    final ScheduledBatchTask taskScheduler = supervisorToTaskScheduler.get(supervisorId);
    if (taskScheduler == null) {
      return null;
    }

    final BatchSupervisorTaskReport taskReport = statusTracker.getSupervisorTaskReport(supervisorId);
    return new ScheduledBatchSupervisorStatus(
        supervisorId,
        taskScheduler.getState(),
        taskScheduler.getLastTaskSubmittedTime(),
        taskScheduler.getNextTaskSubmissionTime(),
        taskReport
    );
  }

  private void submitSqlTask(final String supervisorId, final ClientSqlQuery spec)
      throws ExecutionException, InterruptedException
  {
    final SqlTaskStatus taskStatus = FutureUtils.get(brokerClient.submitSqlTask(spec), true);
    statusTracker.onTaskSubmitted(supervisorId, taskStatus);
    log.info("Submitted a new task[%s] for supervisor[%s].", taskStatus.getTaskId(), supervisorId);
  }

  /**
   * Handles the task scheduling for a specific batch supervisor.
   * <p>
   * This is responsible for periodically submitting MSQ tasks based on the schedule
   * defined in {@link CronSchedulerConfig}. It ensures that tasks are rescheduled upon completion
   * and maintains the last submission timestamp for tracking purposes.
   * </p>
   * <p>
   * The scheduler for a batch supervisor can be started and stopped dynamically by invoking
   * {@link #startScheduling()} and {@link #stopScheduling()} respectively.
   * </p>
   **/
  private class ScheduledBatchTask
  {
    private final String supervisorId;
    private final String dataSource;
    private final ClientSqlQuery spec;
    private final CronSchedulerConfig cronSchedulerConfig;

    private ScheduledBatchSupervisor.State state;

    /**
     * TODO: Note that the last task submitted per supervisor should eventually be persisted in the metadata store,
     * along with any other scheduler state, so that the batch supervisor can recover gracefully and
     * avoid missing task submissions during rolling restarts, etc.
     */
    private volatile DateTime lastTaskSubmittedTime;

    private ScheduledBatchTask(
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
      if (tasksExecutor.isTerminated() || tasksExecutor.isShutdown()
          || state == ScheduledBatchSupervisor.State.SUSPENDED) {
        return;
      }

      statusTracker.cleanupStaleTaskStatuses(supervisorId);

      state = ScheduledBatchSupervisor.State.RUNNING;
      final Duration timeUntilNextSubmission = getTimeUntilNextTaskSubmission();
      if (timeUntilNextSubmission == null) {
        log.info("No more tasks will be submitted for supervisor[%s].", supervisorId);
        return;
      } else {
        log.info("Next task for supervisor[%s] will be scheduled after duration[%s].", supervisorId, timeUntilNextSubmission);
      }

      tasksExecutor.schedule(
          () -> {
            try {
              // Check status inside the runnable again before submitting any tasks
              if (state == ScheduledBatchSupervisor.State.SUSPENDED) {
                return;
              }
              submitSqlTask(supervisorId, spec);
              lastTaskSubmittedTime = DateTimes.nowUtc();
              emitMetric("task/scheduledBatch/submit/success", 1);
            }
            catch (Exception e) {
              emitMetric("task/scheduledBatch/submit/failed", 1);
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
      state = ScheduledBatchSupervisor.State.SUSPENDED;
      statusTracker.cleanupStaleTaskStatuses(supervisorId);
    }

    private void emitMetric(final String metricName, final int value)
    {
      emitter.emit(
          ServiceMetricEvent.builder()
                            .setDimension(DruidMetrics.ID, supervisorId)
                            .setDimension(DruidMetrics.DATASOURCE, dataSource)
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

    private ScheduledBatchSupervisor.State getState()
    {
      return state;
    }
  }
}
