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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.Controller;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Registry for actively-running {@link Controller} and recently-completed {@link TaskReport}.
 */
@ManageLifecycle
public class DartControllerRegistry
{
  /**
   * Minimum frequency for checking if {@link #cleanupExpiredReports()} needs to be run.
   */
  private static final long MIN_CLEANUP_CHECK_MILLIS = 10_000;

  private final DartControllerConfig config;

  /**
   * Map of Dart query ID -> controller for currently-running queries.
   */
  private final ConcurrentHashMap<String, ControllerHolder> controllerMap = new ConcurrentHashMap<>();

  /**
   * Map of Dart query ID -> timestamped report for completed queries.
   */
  @GuardedBy("completeReports")
  private final LinkedHashMap<String, QueryInfoAndReport> completeReports = new LinkedHashMap<>();

  /**
   * Map of SQL query ID -> Dart query ID. Used by {@link #getQueryInfoAndReportBySqlQueryId(String)}. Contains an
   * entry for every query in either {@link #controllerMap} or {@link #completeReports}.
   *
   * It is possible for the same SQL query ID to map to multiple Dart query IDs, because SQL query IDs can be set
   * by the user, and uniqueness is not a required. If this occurs case, we go with the first one encountered
   * and ignore the others.
   */
  private final ConcurrentHashMap<String, String> sqlQueryIdToDartQueryId = new ConcurrentHashMap<>();

  /**
   * Executor for cleaning up reports older than {@link DartControllerConfig#getMaxRetainedReportDuration()}.
   */
  private ScheduledExecutorService cleanupExec;

  @Inject
  public DartControllerRegistry(final DartControllerConfig config)
  {
    this.config = config;
  }

  @LifecycleStart
  public void start()
  {
    // Schedule periodic cleanup of expired reports.
    if (!config.getMaxRetainedReportDuration().equals(Period.ZERO)) {
      final String threadNameFormat = StringUtils.format("%s-ReportCleanupExec-%%s", getClass().getSimpleName());
      final long cleanupPeriodMs = Math.max(
          MIN_CLEANUP_CHECK_MILLIS,
          config.getMaxRetainedReportDuration().toStandardDuration().getMillis() / 10
      );
      cleanupExec = Execs.scheduledSingleThreaded(threadNameFormat);
      cleanupExec.scheduleAtFixedRate(
          this::cleanupExpiredReports,
          cleanupPeriodMs,
          cleanupPeriodMs,
          TimeUnit.MILLISECONDS
      );
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (cleanupExec != null) {
      cleanupExec.shutdown();
    }
  }

  /**
   * Add a controller. Throws {@link DruidException} if a controller with the same {@link Controller#queryId()} is
   * already registered.
   */
  public void register(ControllerHolder holder)
  {
    final String dartQueryId = holder.getController().queryId();
    if (controllerMap.putIfAbsent(dartQueryId, holder) != null) {
      throw DruidException.defensive("Controller[%s] already registered", dartQueryId);
    }
    sqlQueryIdToDartQueryId.putIfAbsent(holder.getSqlQueryId(), dartQueryId);
  }

  /**
   * Remove a controller from the registry. Optionally registers a report that will be available for some
   * time afterwards, based on {@link DartControllerConfig#getMaxRetainedReportCount()} and
   * {@link DartControllerConfig#getMaxRetainedReportDuration()}.
   */
  public void deregister(ControllerHolder holder, @Nullable TaskReport.ReportMap completeReport)
  {
    final String dartQueryId = holder.getController().queryId();

    // Remove only if the current mapping for the queryId is this specific controller.
    final boolean didRemove = controllerMap.remove(dartQueryId, holder);

    // Add completeReport to completeReports, if present, and if we actually did deregister this specific controller.
    if (didRemove && completeReport != null && config.getMaxRetainedReportCount() > 0) {
      synchronized (completeReports) {
        // Remove reports if size is greater than maxRetainedReportCount - 1.
        int reportsToRemove = completeReports.size() - config.getMaxRetainedReportCount() + 1;
        if (reportsToRemove > 0) {
          for (Iterator<Map.Entry<String, QueryInfoAndReport>> it = completeReports.entrySet().iterator();
               it.hasNext() && reportsToRemove > 0;
               reportsToRemove--) {
            final QueryInfoAndReport evictedReport = it.next().getValue();
            it.remove();
            sqlQueryIdToDartQueryId.remove(
                evictedReport.getQueryInfo().getSqlQueryId(),
                evictedReport.getQueryInfo().getDartQueryId()
            );
          }
        }

        completeReports.put(
            dartQueryId,
            new QueryInfoAndReport(
                DartQueryInfo.fromControllerHolder(holder),
                completeReport,
                DateTimes.nowUtc()
            )
        );
      }
    } else if (didRemove) {
      // Report not retained, but controller was removed; clean up the SQL query ID mapping.
      sqlQueryIdToDartQueryId.remove(holder.getSqlQueryId(), dartQueryId);
    }
  }

  /**
   * Return a specific controller holder by Dart query ID, or null if it doesn't exist.
   */
  @Nullable
  public ControllerHolder getController(final String queryId)
  {
    return controllerMap.get(queryId);
  }

  /**
   * Returns all actively-running {@link Controller}.
   */
  public Collection<ControllerHolder> getAllControllers()
  {
    return controllerMap.values();
  }

  /**
   * Gets execution details and report for a query.
   */
  @Nullable
  public QueryInfoAndReport getQueryInfoAndReport(final String queryId)
  {
    final ControllerHolder runningController = getController(queryId);

    if (runningController != null) {
      final TaskReport.ReportMap liveReportMap = runningController.getController().liveReports();
      if (liveReportMap != null) {
        return new QueryInfoAndReport(
            DartQueryInfo.fromControllerHolder(runningController),
            liveReportMap,
            DateTimes.nowUtc()
        );
      } else {
        return null;
      }
    } else {
      synchronized (completeReports) {
        return completeReports.get(queryId);
      }
    }
  }

  /**
   * Gets execution details and report for a query by SQL query ID.
   */
  @Nullable
  public QueryInfoAndReport getQueryInfoAndReportBySqlQueryId(final String sqlQueryId)
  {
    final String dartQueryId = sqlQueryIdToDartQueryId.get(sqlQueryId);
    if (dartQueryId == null) {
      return null;
    }
    return getQueryInfoAndReport(dartQueryId);
  }

  /**
   * Removes reports that have exceeded {@link DartControllerConfig#getMaxRetainedReportDuration()}.
   */
  private void cleanupExpiredReports()
  {
    final long thresholdTimestamp = DateTimes.nowUtc().minus(config.getMaxRetainedReportDuration()).getMillis();

    synchronized (completeReports) {
      final Iterator<Map.Entry<String, QueryInfoAndReport>> it = completeReports.entrySet().iterator();
      while (it.hasNext()) {
        final QueryInfoAndReport report = it.next().getValue();
        if (report.getTimestamp().getMillis() < thresholdTimestamp) {
          it.remove();
          sqlQueryIdToDartQueryId.remove(report.getQueryInfo().getSqlQueryId(), report.getQueryInfo().getDartQueryId());
        } else {
          // Entries are added in order of increasing timestamp, so we can stop looking once we
          // find a non-expired entry.
          break;
        }
      }
    }
  }
}
