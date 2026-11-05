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

package org.apache.druid.server.log;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import jakarta.validation.constraints.NotNull;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.server.RequestLogLine;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("filtered")
public class FilteredRequestLoggerProvider implements RequestLoggerProvider
{
  private static final Logger log = new Logger(FilteredRequestLoggerProvider.class);

  @JsonProperty
  @NotNull
  private RequestLoggerProvider delegate = null;

  @JsonProperty
  private long queryTimeThresholdMs = 0;

  @JsonProperty
  private long sqlQueryTimeThresholdMs = 0;

  @JsonProperty
  private Set<String> mutedQueryTypes = Set.of();

  @JsonProperty
  private boolean alwaysLogFailures = false;

  @JsonProperty
  private boolean muteSqlPlannedNativeQueries = false;

  @Override
  public RequestLogger get()
  {
    FilteredRequestLogger logger = new FilteredRequestLogger(
        delegate.get(),
        queryTimeThresholdMs,
        sqlQueryTimeThresholdMs,
        mutedQueryTypes,
        alwaysLogFailures,
        muteSqlPlannedNativeQueries
    );
    log.debug(new Exception("Stack trace"), "Creating %s at", logger);
    return logger;
  }

  public static class FilteredRequestLogger implements RequestLogger
  {
    private final RequestLogger logger;
    private final long queryTimeThresholdMs;
    private final long sqlQueryTimeThresholdMs;
    private final Set<String> mutedQueryTypes;
    private final boolean alwaysLogFailures;
    private final boolean muteSqlPlannedNativeQueries;

    /**
     * @param alwaysLogFailures           if true, failed queries are logged regardless of every other filter
     * @param muteSqlPlannedNativeQueries if true, native queries with a {@code sqlQueryId} in their context are not
     *                                    logged
     */
    public FilteredRequestLogger(
        RequestLogger logger,
        long queryTimeThresholdMs,
        long sqlQueryTimeThresholdMs,
        Set<String> mutedQueryTypes,
        boolean alwaysLogFailures,
        boolean muteSqlPlannedNativeQueries
    )
    {
      this.logger = logger;
      this.queryTimeThresholdMs = queryTimeThresholdMs;
      this.sqlQueryTimeThresholdMs = sqlQueryTimeThresholdMs;
      this.mutedQueryTypes = mutedQueryTypes;
      this.alwaysLogFailures = alwaysLogFailures;
      this.muteSqlPlannedNativeQueries = muteSqlPlannedNativeQueries;
    }

    public long getQueryTimeThresholdMs()
    {
      return queryTimeThresholdMs;
    }

    public boolean isAlwaysLogFailures()
    {
      return alwaysLogFailures;
    }

    public boolean isMuteSqlPlannedNativeQueries()
    {
      return muteSqlPlannedNativeQueries;
    }

    public RequestLogger getDelegate()
    {
      return logger;
    }

    @LifecycleStart
    @Override
    public void start() throws Exception
    {
      logger.start();
    }

    @LifecycleStop
    @Override
    public void stop()
    {
      logger.stop();
    }

    @Override
    public void logNativeQuery(RequestLogLine requestLogLine) throws IOException
    {
      if (shouldLog(requestLogLine, "query/time", queryTimeThresholdMs, muteSqlPlannedNativeQueries)) {
        logger.logNativeQuery(requestLogLine);
      }
    }

    @Override
    public void logSqlQuery(RequestLogLine requestLogLine) throws IOException
    {
      if (shouldLog(requestLogLine, "sqlQuery/time", sqlQueryTimeThresholdMs, false)) {
        logger.logSqlQuery(requestLogLine);
      }
    }

    private boolean shouldLog(
        RequestLogLine requestLogLine,
        String timeMetric,
        long timeThresholdMs,
        boolean muteSqlPlanned
    )
    {
      final Map<String, Object> stats = requestLogLine.getQueryStats().getStats();
      if (alwaysLogFailures && Boolean.FALSE.equals(stats.get("success"))) {
        return true;
      }

      final Object queryTime = stats.get(timeMetric);
      if (queryTime == null || ((Number) queryTime).longValue() < timeThresholdMs) {
        return false;
      }

      final Query<?> query = requestLogLine.getQuery();
      if (query == null) {
        return true;
      }
      return !mutedQueryTypes.contains(query.getType()) && !(muteSqlPlanned && query.getSqlQueryId() != null);
    }

    @Override
    public String toString()
    {
      return "FilteredRequestLogger{" +
             "logger=" + logger +
             ", queryTimeThresholdMs=" + queryTimeThresholdMs +
             ", sqlQueryTimeThresholdMs=" + sqlQueryTimeThresholdMs +
             ", mutedQueryTypes=" + mutedQueryTypes +
             ", alwaysLogFailures=" + alwaysLogFailures +
             ", muteSqlPlannedNativeQueries=" + muteSqlPlannedNativeQueries +
             '}';
    }
  }
}
