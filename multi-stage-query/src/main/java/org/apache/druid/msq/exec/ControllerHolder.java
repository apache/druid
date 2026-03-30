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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.http.StandardQueryState;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holder for a {@link Controller} that manages its lifecycle.
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
  private final AtomicReference<State> state = new AtomicReference<>(State.ACCEPTED);

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

  public State getState()
  {
    return state.get();
  }

  /**
   * Runs {@link Controller#run(QueryListener)} in the provided executor. Optionally registers the controller with
   * the provided registry while it is running.
   *
   * @return future that resolves when the controller is done or canceled
   */
  public ListenableFuture<?> runAsync(
      final QueryListener listener,
      @Nullable final ControllerRegistry controllerRegistry,
      final ListeningExecutorService exec
  )
  {
    if (controllerRegistry != null) {
      // Register controller before submitting anything to the executor, so it shows up in
      // "active controllers" lists.
      controllerRegistry.register(this);
    }

    final ListenableFuture<?> future = exec.submit(() -> {
      final String threadName = Thread.currentThread().getName();
      Thread.currentThread().setName(makeThreadName());

      try {
        final CaptureReportQueryListener reportListener = new CaptureReportQueryListener(listener);

        try {
          if (state.compareAndSet(State.ACCEPTED, State.RUNNING)) {
            synchronized (this) {
              // Capture the controller thread.
              controllerThread = Thread.currentThread();

              // It is possible cancel() was called between "state.compareAndSet" and this synchronized block.
              // In that case, the controllerThread would not have been interrupted by cancel(). Check for
              // cancelReason here to cover this case.
              if (cancelReason != null) {
                reportListener.onQueryComplete(makeCanceledReport(cancelReason));
                return; // Skip controller.run, go straight to deregister.
              }
            }

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
            final CancellationReason reason;
            synchronized (this) {
              reason = cancelReason;
            }
            reportListener.onQueryComplete(makeCanceledReport(reason));
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
      }
    });

    // Must not cancel the above future, otherwise "deregister" may never get called. If a controller is canceled
    // before it runs, the runnable above stays in the queue until it gets a thread, then it exits without running
    // the controller.
    return Futures.nonCancellationPropagating(future);
  }

  /**
   * Places this holder into {@link State#CANCELED}. Interrupts the controller thread if currently running,
   * and calls {@link Controller#stop} as a failsafe.
   */
  public void cancel(final CancellationReason reason)
  {
    if (state.compareAndSet(State.ACCEPTED, State.CANCELED)) {
      synchronized (this) {
        cancelReason = reason;
      }
    } else if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
      // Primary mechanism of stopping the controller: interrupt the controller thread.
      synchronized (this) {
        cancelReason = reason;
        if (controllerThread != null) {
          controllerThread.interrupt();
        }
      }

      // Failsafe: call "stop", which throws a CanceledFault into the controller thread.
      controller.stop(reason);
    }
  }

  private void updateStateOnQueryComplete(final MSQTaskReportPayload report)
  {
    switch (report.getStatus().getStatus()) {
      case SUCCESS:
        state.compareAndSet(State.RUNNING, State.SUCCESS);
        break;

      case FAILED:
        state.compareAndSet(State.RUNNING, State.FAILED);
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
