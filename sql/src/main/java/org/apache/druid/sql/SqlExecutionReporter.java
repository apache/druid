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

package org.apache.druid.sql;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Side-car class which reports logs and metrics for an
 * {@link HttpStatement}. This separate class cleanly separates the logic
 * for running a query from the logic for reporting on that run. A query
 * can end either with a success or error. This object is created in
 * the request thread, with the remaining method called either from the
 * request or response thread, but not both.
 */
public class SqlExecutionReporter
{
  private static final Logger log = new Logger(SqlExecutionReporter.class);

  private final AbstractStatement stmt;
  private final String remoteAddress;
  private final long startMs;
  private final long startNs;
  private Throwable e;
  private long bytesWritten;
  private long planningTimeNanos;

  public SqlExecutionReporter(
      final AbstractStatement stmt,
      final String remoteAddress
  )
  {
    this.stmt = stmt;
    this.remoteAddress = remoteAddress;
    this.startMs = System.currentTimeMillis();
    this.startNs = System.nanoTime();
  }

  public void failed(Throwable e)
  {
    this.e = e;
  }

  public void succeeded(final long bytesWritten)
  {
    this.bytesWritten = bytesWritten;
  }

  public void planningTimeNanos(final long planningTimeNanos)
  {
    this.planningTimeNanos = planningTimeNanos;
  }

  public void emit()
  {
    final boolean success = e == null;
    final long queryTimeNs = System.nanoTime() - startNs;

    ServiceEmitter emitter = stmt.sqlToolbox.emitter;
    PlannerContext plannerContext = stmt.plannerContext;
    try {
      ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
      if (plannerContext != null) {
        metricBuilder.setDimension("id", plannerContext.getSqlQueryId());
        metricBuilder.setDimension("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
      }
      if (stmt.authResult != null) {
        // Note: the dimension is "dataSource" (sic), so we log only the SQL resource
        // actions. Even here, for external tables, those actions are not always
        // datasources.
        metricBuilder.setDimension(
            "dataSource",
            stmt.authResult.sqlResourceActions
                            .stream()
                            .map(action -> action.getResource().getName())
                            .collect(Collectors.toList())
                            .toString()
        );
      }
      metricBuilder.setDimension("remoteAddress", StringUtils.nullToEmptyNonDruidDataString(remoteAddress));
      metricBuilder.setDimension("success", String.valueOf(success));
      emitter.emit(metricBuilder.build("sqlQuery/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs)));
      if (bytesWritten >= 0) {
        emitter.emit(metricBuilder.build("sqlQuery/bytes", bytesWritten));
      }
      if (planningTimeNanos >= 0) {
        emitter.emit(metricBuilder.build(
            "sqlQuery/planningTimeMs",
            TimeUnit.NANOSECONDS.toMillis(planningTimeNanos)
        ));
      }

      final Map<String, Object> statsMap = new LinkedHashMap<>();
      statsMap.put("sqlQuery/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs));
      statsMap.put("sqlQuery/planningTimeMs", TimeUnit.NANOSECONDS.toMillis(planningTimeNanos));
      statsMap.put("sqlQuery/bytes", bytesWritten);
      statsMap.put("success", success);
      Map<String, Object> queryContext = stmt.queryContext;
      if (plannerContext != null) {
        statsMap.put("identity", plannerContext.getAuthenticationResult().getIdentity());
        queryContext.put("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
      }
      statsMap.put("context", queryContext);
      if (e != null) {
        statsMap.put("exception", e.toString());

        if (e instanceof QueryInterruptedException || e instanceof QueryTimeoutException) {
          statsMap.put("interrupted", true);
          statsMap.put("reason", e.toString());
        }
      }

      stmt.sqlToolbox.requestLogger.logSqlQuery(
          RequestLogLine.forSql(
              stmt.queryPlus.sql(),
              queryContext,
              DateTimes.utc(startMs),
              remoteAddress,
              new QueryStats(statsMap)
          )
      );
    }
    catch (Exception ex) {
      log.error(ex, "Unable to log SQL [%s]!", stmt.queryPlus.sql());
    }
  }
}
