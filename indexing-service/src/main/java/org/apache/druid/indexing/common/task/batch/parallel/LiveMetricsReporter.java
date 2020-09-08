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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexing.stats.IngestionMetrics;
import org.apache.druid.indexing.stats.IngestionMetricsSnapshot;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This class is used in subtasks of {@link ParallelIndexSupervisorTask} for reporting live metrics during ingestion.
 * The live metrics are directly sent to the supervisor task over HTTP.
 *
 * For general metrics system, see {@link org.apache.druid.java.util.metrics.Monitor} and
 * {@link org.apache.druid.java.util.metrics.MonitorScheduler}.
 */
public class LiveMetricsReporter
{
  private static final Logger LOG = new Logger(LiveMetricsReporter.class);

  private final String supervisorTaskId;
  private final String subtaskId;
  private final ParallelIndexSupervisorTaskClient supervisorTaskClient;
  private final IngestionMetrics subtaskMetrics;

  // liveReportTimeoutMs in TaskMonitor is determined based on the below 2 configurations.
  private final long taskStatusCheckPeriodMs;
  private final int chatHandlerNumRetries;

  /**
   * A scheduledExecutorService which periodically runs {@link ReportRunnable}. It is initialized in {@link #start()}
   * and terminated in {@link #stop()}.
   */
  @MonotonicNonNull
  private ScheduledExecutorService scheduledExec;

  public LiveMetricsReporter(
      String supervisorTaskId,
      String subtaskId,
      ParallelIndexSupervisorTaskClient supervisorTaskClient,
      IngestionMetrics subtaskMetrics,
      long taskStatusCheckPeriodMs,
      int chatHandlerNumRetries
  )
  {
    this.supervisorTaskId = supervisorTaskId;
    this.subtaskId = subtaskId;
    this.supervisorTaskClient = supervisorTaskClient;
    this.subtaskMetrics = subtaskMetrics;
    this.taskStatusCheckPeriodMs = taskStatusCheckPeriodMs;
    this.chatHandlerNumRetries = chatHandlerNumRetries;
  }

  public void start()
  {
    LOG.info("Starting reporting live metrics to supervisor[%s]", supervisorTaskId);
    scheduledExec = Execs.scheduledSingleThreaded("live-metric-reporter-%d");
    scheduledExec.scheduleAtFixedRate(
        new ReportRunnable(),
        0,
        taskStatusCheckPeriodMs,
        TimeUnit.MILLISECONDS
    );
  }

  public void stop()
  {
    if (scheduledExec != null) {
      scheduledExec.shutdownNow();
      try {
        scheduledExec.awaitTermination(5, TimeUnit.SECONDS);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while stopping reporter", e);
      }
    }
    LOG.info("Stopped reporting live metrics to supervisor[%s]", supervisorTaskId);
  }

  /**
   * A runnable which is periodically run by {@link #scheduledExec}. This runnable creates a snapshot of metrics
   * and sends it to the supervisor task. Note that the metrics sent to the supervisor task is the snapshot
   * of the current metrics unlike the diff between snapshots is sent in {@link org.apache.druid.java.util.metrics.Monitor}.
   */
  @VisibleForTesting
  class ReportRunnable implements Runnable
  {
    private ReportRunnable()
    {
    }

    @Override
    public void run()
    {
      final IngestionMetricsSnapshot currentSnapshot = subtaskMetrics.snapshot();
      final SubTaskReport report = new RunningSubtaskReport(subtaskId, currentSnapshot);
      try {
        RetryUtils.retry(
            () -> {
              supervisorTaskClient.report(supervisorTaskId, report);
              return null;
            },
            t -> t instanceof Exception,
            chatHandlerNumRetries
        );
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * This methid is visible only for testing to easily test ReportRunnable
   * without starting/stopping LiveMetricsReporter.
   */
  @VisibleForTesting
  ReportRunnable newReportRunnable()
  {
    return new ReportRunnable();
  }
}
