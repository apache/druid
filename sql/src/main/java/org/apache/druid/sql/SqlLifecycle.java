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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link org.apache.druid.server.QueryLifecycle}, this class manages the lifecycle of a SQL query.
 * It ensures that a SQL query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(String, Map)})</li>
 * <li>Planning ({@link #plan(HttpServletRequest)} or {@link #plan(AuthenticationResult)})</li>
 * <li>Authorization ({@link #authorize()})</li>
 * <li>Execution ({@link #execute()})</li>
 * <li>Logging ({@link #emitLogsAndMetrics(Throwable, String, long)})</li>
 * </ol>
 *
 * <p>Unlike QueryLifecycle, this class is designed to be <b>thread safe</b> so that it can be used in multi-threaded
 * scenario (JDBC) without external synchronization.
 */
public class SqlLifecycle
{
  private static final Logger log = new Logger(SqlLifecycle.class);

  private final PlannerFactory plannerFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final long startMs;
  private final long startNs;
  private final Object lock = new Object();

  private State state = State.NEW;

  // init during intialize
  private String sql;
  private Map<String, Object> queryContext;
  // init during plan
  @Nullable private HttpServletRequest req;
  private PlannerContext plannerContext;
  private PlannerResult plannerResult;

  public SqlLifecycle(
      PlannerFactory plannerFactory,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      long startMs,
      long startNs
  )
  {
    this.plannerFactory = plannerFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.startMs = startMs;
    this.startNs = startNs;
  }

  public String initialize(String sql, Map<String, Object> queryContext)
  {
    synchronized (lock) {
      transition(State.NEW, State.INITIALIZED);
      this.sql = sql;
      this.queryContext = contextWithSqlId(queryContext);
      return sqlQueryId();
    }
  }

  private Map<String, Object> contextWithSqlId(Map<String, Object> queryContext)
  {
    Map<String, Object> newContext = new HashMap<>();
    if (queryContext != null) {
      newContext.putAll(queryContext);
    }
    if (!newContext.containsKey(PlannerContext.CTX_SQL_QUERY_ID)) {
      newContext.put(PlannerContext.CTX_SQL_QUERY_ID, UUID.randomUUID().toString());
    }
    return newContext;
  }

  private String sqlQueryId()
  {
    return (String) this.queryContext.get(PlannerContext.CTX_SQL_QUERY_ID);
  }

  public PlannerContext plan(AuthenticationResult authenticationResult)
      throws ValidationException, RelConversionException, SqlParseException
  {
    synchronized (lock) {
      transition(State.INITIALIZED, State.PLANNED);
      try (DruidPlanner planner = plannerFactory.createPlanner(queryContext, authenticationResult)) {
        this.plannerContext = planner.getPlannerContext();
        this.plannerResult = planner.plan(sql);
      }
      return plannerContext;
    }
  }

  public PlannerContext plan(HttpServletRequest req)
      throws SqlParseException, RelConversionException, ValidationException
  {
    synchronized (lock) {
      this.req = req;
      return plan(AuthorizationUtils.authenticationResultFromRequest(req));
    }
  }

  public RelDataType rowType()
  {
    synchronized (lock) {
      Preconditions.checkState(plannerResult != null,
                               "must be called after sql has been planned");
      return plannerResult.rowType();
    }
  }

  public Access authorize()
  {
    synchronized (lock) {
      transition(State.PLANNED, State.AUTHORIZING);

      if (req != null) {
        return doAuthorize(
            AuthorizationUtils.authorizeAllResourceActions(
                req,
                Iterables.transform(
                    plannerResult.datasourceNames(),
                    AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
                ),
                plannerFactory.getAuthorizerMapper()
            )
        );
      }

      return doAuthorize(
          AuthorizationUtils.authorizeAllResourceActions(
              plannerContext.getAuthenticationResult(),
              Iterables.transform(plannerResult.datasourceNames(), AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
              plannerFactory.getAuthorizerMapper()
          )
      );
    }
  }

  private Access doAuthorize(final Access authorizationResult)
  {
    if (!authorizationResult.isAllowed()) {
      // Not authorized; go straight to Jail, do not pass Go.
      transition(State.AUTHORIZING, State.UNAUTHORIZED);
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
    }
    return authorizationResult;
  }

  public PlannerContext planAndAuthorize(final AuthenticationResult authenticationResult)
      throws SqlParseException, RelConversionException, ValidationException
  {
    PlannerContext plannerContext = plan(authenticationResult);
    Access access = authorize();
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }
    return plannerContext;
  }

  public PlannerContext planAndAuthorize(final HttpServletRequest req)
      throws SqlParseException, RelConversionException, ValidationException
  {
    PlannerContext plannerContext = plan(req);
    Access access = authorize();
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }
    return plannerContext;
  }

  public Sequence<Object[]> execute()
  {
    synchronized (lock) {
      transition(State.AUTHORIZED, State.EXECUTING);
      return plannerResult.run();
    }
  }

  public Sequence<Object[]> runSimple(
      String sql,
      Map<String, Object> queryContext,
      AuthenticationResult authenticationResult
  ) throws ValidationException, RelConversionException, SqlParseException
  {
    Sequence<Object[]> result;

    initialize(sql, queryContext);
    try {
      planAndAuthorize(authenticationResult);
      result = execute();
    }
    catch (Throwable e) {
      emitLogsAndMetrics(e, null, -1);
      throw e;
    }

    return Sequences.wrap(result, new SequenceWrapper()
    {
      @Override
      public void after(boolean isDone, Throwable thrown)
      {
        emitLogsAndMetrics(thrown, null, -1);
      }
    });
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  public void emitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    synchronized (lock) {
      if (sql == null) {
        // Never initialized, don't log or emit anything.
        return;
      }

      if (state == State.DONE) {
        log.warn("Tried to emit logs and metrics twice for query[%s]!", sqlQueryId());
      }

      state = State.DONE;

      final boolean success = e == null;
      final long queryTimeNs = System.nanoTime() - startNs;

      try {
        ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
        if (plannerContext != null) {
          metricBuilder.setDimension("id", plannerContext.getSqlQueryId());
          metricBuilder.setDimension("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
        }
        if (plannerResult != null) {
          metricBuilder.setDimension("dataSource", plannerResult.datasourceNames().toString());
        }
        metricBuilder.setDimension("remoteAddress", StringUtils.nullToEmptyNonDruidDataString(remoteAddress));
        metricBuilder.setDimension("success", String.valueOf(success));
        emitter.emit(metricBuilder.build("sqlQuery/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs)));
        if (bytesWritten >= 0) {
          emitter.emit(metricBuilder.build("sqlQuery/bytes", bytesWritten));
        }

        final Map<String, Object> statsMap = new LinkedHashMap<>();
        statsMap.put("sqlQuery/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs));
        statsMap.put("sqlQuery/bytes", bytesWritten);
        statsMap.put("success", success);
        statsMap.put("context", queryContext);
        if (plannerContext != null) {
          statsMap.put("identity", plannerContext.getAuthenticationResult().getIdentity());
          queryContext.put("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
        }
        if (e != null) {
          statsMap.put("exception", e.toString());

          if (e instanceof QueryInterruptedException) {
            statsMap.put("interrupted", true);
            statsMap.put("reason", e.toString());
          }
        }

        requestLogger.logSqlQuery(
            RequestLogLine.forSql(
                sql,
                queryContext,
                DateTimes.utc(startMs),
                remoteAddress,
                new QueryStats(statsMap)
            )
        );
      }
      catch (Exception ex) {
        log.error(ex, "Unable to log sql [%s]!", sql);
      }
    }
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s] because current state[%s] is not [%s].", from, to, state, from);
    }

    state = to;
  }

  enum State
  {
    NEW,
    INITIALIZED,
    PLANNED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }
}
