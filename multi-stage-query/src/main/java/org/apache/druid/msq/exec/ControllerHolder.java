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

package org.apache.druid.msq.exec;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.http.StandardQueryState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Holder for a {@link Controller} that manages its lifecycle, including cancellation and timeouts.
 */
public class ControllerHolder
{
  private static final Logger log = new Logger(ControllerHolder.class);

  private final Controller controller;
  private final String sqlQueryId;

  @Nullable
  private final String sql;

  @Nullable
  private final AuthenticationResult authenticationResult;

  private final DateTime startTime;

  @GuardedBy("this")
  private State state = State.ACCEPTED;

  /**
   * Thread running the controller. Set inside the {@link #runAsync} runnable, cleared in its finally block.
   */
  @GuardedBy("this")
  private Thread controllerThread;

  /**
   * If cancel was called, the reason for cancellation. Used to deliver the cancellation to the controller thread
   * if {@link #cancel} is called before the controller starts running.
   */
  @GuardedBy("this")
  private CancellationReason cancelReason;

  public ControllerHolder(
      final Controller controller,
      final String sqlQueryId,
      @Nullable final String sql,
      @Nullable final AuthenticationResult authenticationResult,
      final DateTime startTime
  )
  {
    this.controller = Preconditions.checkNotNull(controller, "controller");
    this.sqlQueryId = Preconditions.checkNotNull(sqlQueryId, "sqlQueryId");
    this.sql = sql;
    this.authenticationResult = authenticationResult;
    this.startTime = Preconditions.checkNotNull(startTime, "startTime");
  }

  public Controller getController()
  {
    return controller;
  }

  @Nullable
  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  @Nullable
  public String getSql()
  {
    return sql;
  }

  public String getControllerHost()
  {
    return getControllerContext().selfNode().getHostAndPortToUse();
  }

  private ControllerContext getControllerContext()
  {
    return controller.getControllerContext();
  }

  @Nullable
  public AuthenticationResult getAuthenticationResult()
  {
    return authenticationResult;
  }

  public DateTime getStartTime()
  {
    return startTime;
  }

  public synchronized State getState()
  {
    return state;
  }

  /**
   * Runs {@link Controller#run(QueryListener)} in the provided executor. Optionally registers the controller with
   * the provided registry while it is running. Schedules a timeout on the provided {@code scheduledExec} based
   * on the query deadline from the controller's query context.
   *
   * @return future that resolves when the controller is done or canceled
   */
  public ListenableFuture<?> runAsync(
      final QueryListener listener,
      @Nullable final ControllerRegistry controllerRegistry,
      final ListeningExecutorService exec,
      final ScheduledExecutorService scheduledExec
  )
  {
    if (controllerRegistry != null) {
      // Register controller before submitting anything to the executor, so it shows up in
      // "active controllers" lists.
      controllerRegistry.register(this);
    }

    // Schedule timeout based on the query deadline. The scheduled task calls cancel(), which is
    // safe even if the controller has already finished (cancel is a no-op for terminal states).
    final ScheduledFuture<?> timeoutFuture = scheduleTimeout(scheduledExec);

    final ListenableFuture<?> future = exec.submit(() -> {
      final String threadName = Thread.currentThread().getName();
      Thread.currentThread().setName(makeThreadName());

      try {
        final CaptureReportQueryListener reportListener = new CaptureReportQueryListener(listener);

        try {
          if (transitionToRunning()) {
            try {
              controller.run(reportListener);
            }
            finally {
              synchronized (this) {
                controllerThread = null;
                // Clear any interrupt delivered during or after controller.run().
                //noinspection ResultOfMethodCallIgnored
                Thread.interrupted();
              }
            }

            updateStateOnQueryComplete(reportListener.getReport());
          } else {
            // Canceled before running.
            synchronized (this) {
              reportListener.onQueryComplete(makeCanceledReport(cancelReason));
            }
          }
        }
        catch (Throwable e) {
          log.warn(
              e,
              "Controller[%s] failed, queryId[%s], sqlQueryId[%s]",
              controller.queryId(),
              controller.getQueryContext().getString(BaseQuery.QUERY_ID),
              sqlQueryId
          );
        }
        finally {
          // Build report and then call "deregister".
          final MSQTaskReport taskReport;

          if (reportListener.hasReport()) {
            taskReport = new MSQTaskReport(controller.queryId(), reportListener.getReport());
          } else {
            taskReport = null;
          }

          final TaskReport.ReportMap reportMap = new TaskReport.ReportMap();
          reportMap.put(MSQTaskReport.REPORT_KEY, taskReport);

          if (controllerRegistry != null) {
            controllerRegistry.deregister(this, reportMap);
          }
        }
      }
      finally {
        Thread.currentThread().setName(threadName);

        if (timeoutFuture != null) {
          timeoutFuture.cancel(false);
        }
      }
    });

    // Must not cancel the above future, otherwise "deregister" may never get called. If a controller is canceled
    // before it runs, the runnable above stays in the queue until it gets a thread, then it exits without running
    // the controller.
    return Futures.nonCancellationPropagating(future);
  }

  /**
   * Places this holder into {@link State#CANCELED} and stops the controller.
   */
  public void cancel(final CancellationReason reason)
  {
    final State prevState;
    synchronized (this) {
      prevState = state;

      if (state == State.ACCEPTED || state == State.RUNNING) {
        state = State.CANCELED;
        cancelReason = reason;
      }
    }

    if (prevState == State.RUNNING) {
      controller.stop(reason);

      // Interrupt the controller thread as a failsafe, in case the controller is blocked on something.
      synchronized (this) {
        if (controllerThread != null) {
          controllerThread.interrupt();
        }
      }
    }
  }

  /**
   * Attempts to transition from {@link State#ACCEPTED} to {@link State#RUNNING} and capture the controller thread.
   * If a cancellation arrived between the state transition and capturing the thread, generates a canceled report
   * on the provided listener and returns false.
   *
   * @return true if the controller should proceed to run, false if it was canceled
   */
  private synchronized boolean transitionToRunning()
  {
    if (state != State.ACCEPTED) {
      return false;
    }

    state = State.RUNNING;
    controllerThread = Thread.currentThread();
    return true;
  }

  /**
   * Schedules a timeout task that cancels the controller when the query deadline elapses. If the deadline has
   * already passed, cancels immediately without scheduling. Returns null if no timeout is configured or if
   * an immediate cancellation was performed.
   */
  @Nullable
  private ScheduledFuture<?> scheduleTimeout(final ScheduledExecutorService scheduledExec)
  {
    final DateTime deadline = getQueryDeadline();

    if (deadline == null) {
      return null;
    }

    final long delayMs = deadline.getMillis() - DateTimes.nowUtc().getMillis();

    if (delayMs <= 0) {
      // Deadline has already passed. Cancel immediately rather than scheduling, so the cancellation
      // takes effect even when using a direct executor for the controller thread.
      cancel(CancellationReason.QUERY_TIMEOUT);
      return null;
    }

    return scheduledExec.schedule(
        () -> cancel(CancellationReason.QUERY_TIMEOUT),
        delayMs,
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Retrieves the query deadline from the controller's query context. Returns null if no timeout is configured.
   */
  @Nullable
  private DateTime getQueryDeadline()
  {
    final QueryContext queryContext = controller.getQueryContext();
    DateTime deadline = MultiStageQueryContext.getQueryDeadline(queryContext);

    if (deadline == null) {
      // Newer Brokers set the deadline, but older ones might not. Fall back to startTime and timeout in this case.
      final long timeout = queryContext.getTimeout(QueryContexts.NO_TIMEOUT);
      if (timeout != QueryContexts.NO_TIMEOUT) {
        deadline = MultiStageQueryContext.getStartTime(queryContext).plus(timeout);
      }
    }

    return deadline;
  }

  /**
   * If {@link #state} is {@link State#RUNNING}, update it based on the outcome of a query.
   * Otherwise do nothing.
   */
  private synchronized void updateStateOnQueryComplete(final MSQTaskReportPayload report)
  {
    if (state != State.RUNNING) {
      return;
    }

    switch (report.getStatus().getStatus()) {
      case SUCCESS:
        state = State.SUCCESS;
        break;

      case FAILED:
        state = State.FAILED;
        break;
    }
  }

  /**
   * Generate a name for the thread that {@link #runAsync} uses.
   */
  private String makeThreadName()
  {
    if (sqlQueryId != null) {
      return StringUtils.format(
          "%s[%s]-sqlQueryId[%s]",
          Thread.currentThread().getName(),
          controller.queryId(),
          sqlQueryId
      );
    } else {
      return StringUtils.format(
          "%s[%s]",
          Thread.currentThread().getName(),
          controller.queryId()
      );
    }
  }

  private MSQTaskReportPayload makeCanceledReport(@Nullable final CancellationReason reason)
  {
    final MSQErrorReport errorReport =
        MSQErrorReport.fromFault(
            controller.queryId(),
            null,
            null,
            new CanceledFault(reason != null ? reason : CancellationReason.UNKNOWN)
        );
    final MSQStatusReport statusReport =
        new MSQStatusReport(TaskState.FAILED, errorReport, null, null, 0, Map.of(), 0, 0, null, null);
    return new MSQTaskReportPayload(statusReport, null, null, null);
  }

  public enum State
  {
    /**
     * Query has been accepted, but not yet {@link Controller#run(QueryListener)}.
     */
    ACCEPTED(StandardQueryState.ACCEPTED),

    /**
     * Query has had {@link Controller#run(QueryListener)} called.
     */
    RUNNING(StandardQueryState.RUNNING),

    /**
     * Query has been canceled.
     */
    CANCELED(StandardQueryState.CANCELED),

    /**
     * Query has exited successfully.
     */
    SUCCESS(StandardQueryState.SUCCESS),

    /**
     * Query has failed.
     */
    FAILED(StandardQueryState.FAILED);

    private final String statusString;

    State(final String statusString)
    {
      this.statusString = statusString;
    }

    public String getStatusString()
    {
      return statusString;
    }
  }
}
