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

package org.apache.druid.msq.dart.controller;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.CaptureReportQueryListener;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.QueryListener;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.http.StandardQueryState;
import org.joda.time.DateTime;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Holder for {@link Controller}, stored in {@link DartControllerRegistry}.
 */
public class ControllerHolder
{
  private static final Logger log = new Logger(ControllerHolder.class);

  private final Controller controller;
  private final String sqlQueryId;
  private final String sql;
  private final AuthenticationResult authenticationResult;
  private final DateTime startTime;
  private final AtomicReference<State> state = new AtomicReference<>(State.ACCEPTED);

  public ControllerHolder(
      final Controller controller,
      final String sqlQueryId,
      final String sql,
      final AuthenticationResult authenticationResult,
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

  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

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
   * Call when a worker has gone offline. Closes its client and sends a {@link Controller#workerError}
   * to the controller.
   */
  public void workerOffline(final WorkerId workerId)
  {
    final String workerIdString = workerId.toString();

    ControllerContext controllerContext = getControllerContext();
    if (controllerContext instanceof DartControllerContext) {
      DartControllerContext dartControllerContext = (DartControllerContext) controllerContext;
      // For DartControllerContext, newWorkerClient() returns the same instance every time.
      // This will always be DartControllerContext in production; the instanceof check is here because certain
      // tests use a different context class.
      dartControllerContext.newWorkerClient().closeClient(workerId.getHostAndPort());
    }

    if (controller.hasWorker(workerIdString)) {
      controller.workerError(
          MSQErrorReport.fromFault(
              workerIdString,
              workerId.getHostAndPort(),
              null,
              new WorkerFailedFault(workerIdString, "Worker went offline")
          )
      );
    }
  }

  /**
   * Places this holder into {@link State#CANCELED}. Calls {@link Controller#stop(CancellationReason)} if it was
   * previously in state {@link State#RUNNING}.
   */
  public void cancel(CancellationReason reason)
  {
    if (state.compareAndSet(State.ACCEPTED, State.CANCELED)) {
      // No need to call stop() since run() wasn't called.
      return;
    }

    if (state.compareAndSet(State.RUNNING, State.CANCELED)) {
      controller.stop(reason);
    }
  }

  /**
   * Runs {@link Controller#run(QueryListener)} in the provided executor. Registers the controller with the provided
   * registry while it is running.
   *
   * @return future that resolves when the controller is done or canceled.
   */
  public ListenableFuture<?> runAsync(
      final QueryListener listener,
      final DartControllerRegistry controllerRegistry,
      final ControllerThreadPool threadPool
  )
  {
    // Register controller before submitting anything to controllerExeuctor, so it shows up in
    // "active controllers" lists.
    controllerRegistry.register(this);

    final ListenableFuture<?> future = threadPool.getExecutorService().submit(() -> {
      final String threadName = Thread.currentThread().getName();
      Thread.currentThread().setName(makeThreadName());

      try {
        final CaptureReportQueryListener reportListener = new CaptureReportQueryListener(listener);

        try {
          if (state.compareAndSet(State.ACCEPTED, State.RUNNING)) {
            controller.run(reportListener);
            updateStateOnQueryComplete(reportListener.getReport());
          } else {
            // Canceled before running.
            reportListener.onQueryComplete(makeCanceledReport());
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
          controllerRegistry.deregister(this, reportMap);
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
    return StringUtils.format(
        "%s[%s]-sqlQueryId[%s]",
        Thread.currentThread().getName(),
        controller.queryId(),
        sqlQueryId
    );
  }

  private MSQTaskReportPayload makeCanceledReport()
  {
    final MSQErrorReport errorReport =
        MSQErrorReport.fromFault(controller.queryId(), null, null, CanceledFault.userRequest());
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

    State(String statusString)
    {
      this.statusString = statusString;
    }

    public String getStatusString()
    {
      return statusString;
    }
  }
}
