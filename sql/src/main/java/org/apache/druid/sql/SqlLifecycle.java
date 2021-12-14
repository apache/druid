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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.calcite.avatica.remote.TypedValue;
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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QueryStats;
import org.apache.druid.server.RequestLogLine;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.PrepareResult;
import org.apache.druid.sql.calcite.planner.ValidationResult;
import org.apache.druid.sql.http.SqlParameter;
import org.apache.druid.sql.http.SqlQuery;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Similar to {@link org.apache.druid.server.QueryLifecycle}, this class manages the lifecycle of a SQL query.
 * It ensures that a SQL query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(String, Map)})</li>
 * <li>Validation and Authorization ({@link #validateAndAuthorize(HttpServletRequest)} or {@link #validateAndAuthorize(AuthenticationResult)})</li>
 * <li>Planning ({@link #plan()})</li>
 * <li>Execution ({@link #execute()})</li>
 * <li>Logging ({@link #finalizeStateAndEmitLogsAndMetrics(Throwable, String, long)})</li>
 * </ol>
 *
 * Every method in this class must be called by the same thread except for {@link #cancel()}.
 */
public class SqlLifecycle
{
  private static final Logger log = new Logger(SqlLifecycle.class);

  private final PlannerFactory plannerFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final QueryScheduler queryScheduler;
  private final long startMs;
  private final long startNs;

  /**
   * This lock coordinates the access to {@link #state} as there is a happens-before relationship
   * between {@link #cancel} and {@link #transition}.
   */
  private final Object stateLock = new Object();
  @GuardedBy("stateLock")
  private State state = State.NEW;

  // init during intialize
  private String sql;
  private Map<String, Object> queryContext;
  private List<TypedValue> parameters;
  // init during plan
  private PlannerContext plannerContext;
  private ValidationResult validationResult;
  private PrepareResult prepareResult;
  private PlannerResult plannerResult;

  public SqlLifecycle(
      PlannerFactory plannerFactory,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryScheduler queryScheduler,
      long startMs,
      long startNs
  )
  {
    this.plannerFactory = plannerFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryScheduler = queryScheduler;
    this.startMs = startMs;
    this.startNs = startNs;
    this.parameters = Collections.emptyList();
  }

  /**
   * Initialize the query lifecycle, setting the raw string SQL, initial query context, and assign a sql query id.
   *
   * If successful (it will be), it will transition the lifecycle to {@link State#INITIALIZED}.
   */
  public String initialize(String sql, Map<String, Object> queryContext)
  {
    transition(State.NEW, State.INITIALIZED);
    this.sql = sql;
    this.queryContext = contextWithSqlId(queryContext);
    return sqlQueryId();
  }

  private Map<String, Object> contextWithSqlId(Map<String, Object> queryContext)
  {
    Map<String, Object> newContext = new HashMap<>();
    if (queryContext != null) {
      newContext.putAll(queryContext);
    }
    // "bySegment" results are never valid to use with SQL because the result format is incompatible
    // so, overwrite any user specified context to avoid exceptions down the line
    if (newContext.remove(QueryContexts.BY_SEGMENT_KEY) != null) {
      log.warn("'bySegment' results are not supported for SQL queries, ignoring query context parameter");
    }
    newContext.computeIfAbsent(PlannerContext.CTX_SQL_QUERY_ID, k -> UUID.randomUUID().toString());
    return newContext;
  }

  private String sqlQueryId()
  {
    return (String) this.queryContext.get(PlannerContext.CTX_SQL_QUERY_ID);
  }

  /**
   * Assign dynamic parameters to be used to substitute values during query exection. This can be performed at any
   * part of the lifecycle.
   */
  public void setParameters(List<TypedValue> parameters)
  {
    this.parameters = parameters;
    if (this.plannerContext != null) {
      this.plannerContext.setParameters(parameters);
    }
  }

  /**
   * Validate SQL query and authorize against any datasources or views which will take part in the query.
   *
   * If successful, the lifecycle will first transition from {@link State#INITIALIZED} first to
   * {@link State#AUTHORIZING} and then to either {@link State#AUTHORIZED} or {@link State#UNAUTHORIZED}.
   */
  public void validateAndAuthorize(AuthenticationResult authenticationResult)
  {
    synchronized (stateLock) {
      if (state == State.AUTHORIZED) {
        return;
      }
    }
    transition(State.INITIALIZED, State.AUTHORIZING);
    validate(authenticationResult);
    Access access = doAuthorize(
        AuthorizationUtils.authorizeAllResourceActions(
            authenticationResult,
            validationResult.getResourceActions(),
            plannerFactory.getAuthorizerMapper()
        )
    );
    checkAccess(access);
  }

  /**
   * Validate SQL query and authorize against any datasources or views which the query. Like
   * {@link #validateAndAuthorize(AuthenticationResult)} but for a {@link HttpServletRequest}.
   *
   * If successful, the lifecycle will first transition from {@link State#INITIALIZED} first to
   * {@link State#AUTHORIZING} and then to either {@link State#AUTHORIZED} or {@link State#UNAUTHORIZED}.
   */
  public void validateAndAuthorize(HttpServletRequest req)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);
    AuthenticationResult authResult = AuthorizationUtils.authenticationResultFromRequest(req);
    validate(authResult);
    Access access = doAuthorize(
        AuthorizationUtils.authorizeAllResourceActions(
            req,
            validationResult.getResourceActions(),
            plannerFactory.getAuthorizerMapper()
        )
    );
    checkAccess(access);
  }

  private ValidationResult validate(AuthenticationResult authenticationResult)
  {
    try (DruidPlanner planner = plannerFactory.createPlanner(sql, queryContext)) {
      // set planner context for logs/metrics in case something explodes early
      this.plannerContext = planner.getPlannerContext();
      this.plannerContext.setAuthenticationResult(authenticationResult);
      // set parameters on planner context, if parameters have already been set
      this.plannerContext.setParameters(parameters);
      this.validationResult = planner.validate();
      return validationResult;
    }
    // we can't collapse catch clauses since SqlPlanningException has type-sensitive constructors.
    catch (SqlParseException e) {
      throw new SqlPlanningException(e);
    }
    catch (ValidationException e) {
      throw new SqlPlanningException(e);
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

  private void checkAccess(Access access)
  {
    plannerContext.setAuthorizationResult(access);
    if (!access.isAllowed()) {
      throw new ForbiddenException(access.toString());
    }
  }

  /**
   * Prepare the query lifecycle for execution, without completely planning into something that is executable, but
   * including some initial parsing and validation and any dyanmic parameter type resolution, to support prepared
   * statements via JDBC.
   */
  public PrepareResult prepare() throws RelConversionException
  {
    synchronized (stateLock) {
      if (state != State.AUTHORIZED) {
        throw new ISE("Cannot prepare because current state[%s] is not [%s].", state, State.AUTHORIZED);
      }
    }
    Preconditions.checkNotNull(plannerContext, "Cannot prepare, plannerContext is null");
    try (DruidPlanner planner = plannerFactory.createPlannerWithContext(plannerContext)) {
      this.prepareResult = planner.prepare();
      return prepareResult;
    }
    // we can't collapse catch clauses since SqlPlanningException has type-sensitive constructors.
    catch (SqlParseException e) {
      throw new SqlPlanningException(e);
    }
    catch (ValidationException e) {
      throw new SqlPlanningException(e);
    }
  }

  /**
   * Plan the query to enable execution.
   *
   * If successful, the lifecycle will first transition from {@link State#AUTHORIZED} to {@link State#PLANNED}.
   */
  public void plan() throws RelConversionException
  {
    transition(State.AUTHORIZED, State.PLANNED);
    Preconditions.checkNotNull(plannerContext, "Cannot plan, plannerContext is null");
    try (DruidPlanner planner = plannerFactory.createPlannerWithContext(plannerContext)) {
      this.plannerResult = planner.plan();
    }
    // we can't collapse catch clauses since SqlPlanningException has type-sensitive constructors.
    catch (SqlParseException e) {
      throw new SqlPlanningException(e);
    }
    catch (ValidationException e) {
      throw new SqlPlanningException(e);
    }
  }

  /**
   * This method must be called after {@link #plan()}.
   */
  public SqlRowTransformer createRowTransformer()
  {
    assert plannerContext != null;
    assert plannerResult != null;

    return new SqlRowTransformer(plannerContext.getTimeZone(), plannerResult.rowType());
  }

  @VisibleForTesting
  PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  /**
   * Execute the fully planned query.
   *
   * If successful, the lifecycle will first transition from {@link State#PLANNED} to {@link State#EXECUTING}.
   */
  public Sequence<Object[]> execute()
  {
    transition(State.PLANNED, State.EXECUTING);
    return plannerResult.run();
  }

  @VisibleForTesting
  public Sequence<Object[]> runSimple(
      String sql,
      Map<String, Object> queryContext,
      List<SqlParameter> parameters,
      AuthenticationResult authenticationResult
  ) throws RelConversionException
  {
    Sequence<Object[]> result;

    initialize(sql, queryContext);
    try {
      setParameters(SqlQuery.getParameterList(parameters));
      validateAndAuthorize(authenticationResult);
      plan();
      result = execute();
    }
    catch (Throwable e) {
      if (!(e instanceof ForbiddenException)) {
        finalizeStateAndEmitLogsAndMetrics(e, null, -1);
      }
      throw e;
    }

    return Sequences.wrap(result, new SequenceWrapper()
    {
      @Override
      public void after(boolean isDone, Throwable thrown)
      {
        finalizeStateAndEmitLogsAndMetrics(thrown, null, -1);
      }
    });
  }


  @VisibleForTesting
  public ValidationResult runAnalyzeResources(AuthenticationResult authenticationResult)
  {
    return validate(authenticationResult);
  }

  public Set<ResourceAction> getRequiredResourceActions()
  {
    return Preconditions.checkNotNull(validationResult, "validationResult").getResourceActions();
  }

  /**
   * Cancel all native queries associated to this lifecycle.
   *
   * This method is thread-safe.
   */
  public void cancel()
  {
    synchronized (stateLock) {
      if (state == State.CANCELLED) {
        return;
      }
      state = State.CANCELLED;
    }

    final CopyOnWriteArrayList<String> nativeQueryIds = plannerContext.getNativeQueryIds();

    for (String nativeQueryId : nativeQueryIds) {
      log.debug("canceling native query [%s]", nativeQueryId);
      queryScheduler.cancelQuery(nativeQueryId);
    }
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  public void finalizeStateAndEmitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    if (sql == null) {
      // Never initialized, don't log or emit anything.
      return;
    }

    synchronized (stateLock) {
      assert state != State.UNAUTHORIZED; // should not emit below metrics when the query fails to authorize

      if (state != State.CANCELLED) {
        if (state == State.DONE) {
          log.warn("Tried to emit logs and metrics twice for query[%s]!", sqlQueryId());
        }

        state = State.DONE;
      }
    }

    final boolean success = e == null;
    final long queryTimeNs = System.nanoTime() - startNs;

    try {
      ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
      if (plannerContext != null) {
        metricBuilder.setDimension("id", plannerContext.getSqlQueryId());
        metricBuilder.setDimension("nativeQueryIds", plannerContext.getNativeQueryIds().toString());
      }
      if (validationResult != null) {
        metricBuilder.setDimension(
            "dataSource",
            validationResult.getResourceActions()
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

        if (e instanceof QueryInterruptedException || e instanceof QueryTimeoutException) {
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
      log.error(ex, "Unable to log SQL [%s]!", sql);
    }
  }

  @VisibleForTesting
  public State getState()
  {
    synchronized (stateLock) {
      return state;
    }
  }

  @VisibleForTesting
  Map<String, Object> getQueryContext()
  {
    return queryContext;
  }

  private void transition(final State from, final State to)
  {
    synchronized (stateLock) {
      if (state == State.CANCELLED) {
        throw new QueryInterruptedException(
            QueryInterruptedException.QUERY_CANCELLED,
            StringUtils.format("Query is canceled [%s]", sqlQueryId()),
            null,
            null
        );
      }
      if (state != from) {
        throw new ISE(
            "Cannot transition from[%s] to[%s] because current state[%s] is not [%s].",
            from,
            to,
            state,
            from
        );
      }

      state = to;
    }
  }

  enum State
  {
    NEW,
    INITIALIZED,
    AUTHORIZING,
    AUTHORIZED,
    PLANNED,
    EXECUTING,

    // final states
    UNAUTHORIZED,
    CANCELLED, // query is cancelled. can be transitioned to this state only after AUTHORIZED.
    DONE // query could either succeed or fail
  }
}
