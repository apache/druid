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

package org.apache.druid.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.MultiQueryMetricsCollector;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SingleQueryMetricsCollector;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Class that helps a Druid server (broker, historical, etc) manage the lifecycle of a query that it is handling. It
 * ensures that a query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(Query)})</li>
 * <li>Authorization ({@link #authorize(HttpServletRequest)}</li>
 * <li>Execution ({@link #execute()}</li>
 * <li>Logging ({@link #emitLogsAndMetrics(Throwable)}</li>
 * </ol>
 *
 * This object is not thread-safe.
 * <p>
 * Druid accumulates a number of types of metrics and statistics for each query.
 * The lifecycle object also gathers the query-level information into a single
 * place, the <code>LifecycleStats</code>, the distributes it out to the multiple
 * users.
 */
public class QueryLifecycle
{
  private static final Logger log = new Logger(QueryLifecycle.class);

  /**
   * Gathers query-level statistics then distributes them to the metrics system,
   * logs, response trailer and query profile.
   */
  public class LifecycleStats
  {
    private DruidNode node;
    private String remoteAddress;
    private Query<?> receivedQuery;
    private Set<String> trailerOptions;
    private long endTimeNs;
    private long bytesWritten = -1;
    private long rowCount;
    private ResponseContext responseContext;
    private Throwable e;

    /**
     * Called by the query resource to provide the host on which this query is
     * running, and the remote address that sent the request.
     */
    public void onStart(DruidNode node, String remoteAddress, Query<?> receivedQuery)
    {
      this.node = node;
      this.remoteAddress = remoteAddress;
      this.receivedQuery = receivedQuery;
      this.trailerOptions = QueryContexts.getTrailerContents(receivedQuery);

      // The "trailer" field controls the format of the response. Even
      // if the value is invalid, if the key exists, use the trailer
      // format.
      if (trailerOptions == null && receivedQuery.getContextValue(QueryContexts.TRAILER_KEY) != null) {
        trailerOptions = new HashSet<>();
        trailerOptions.add(QueryContexts.TRAILER_METRICS);
      }
    }

    /**
     * Provides the response context when it becomes available. The actual values
     * will continue to change as the query runs until a call to
     * {#link onResultsSent()}.
     */
    public void setResponseContext(ResponseContext responseContext)
    {
      this.responseContext = responseContext;
    }

    /**
     * Partial completion: results written, about to emit the trailer.
     * Accumulate run time and bytes written up to now. Final result will
     * differ a bit.
     */
    public void onResultsSent(long rowCount, long bytesWritten)
    {
      this.rowCount = rowCount;
      this.bytesWritten = bytesWritten;
      this.endTimeNs = System.nanoTime();
    }

    /**
     * Final completion: all results sent, including the possible
     * trailer.
     */
    public void onCompletion(Throwable e)
    {
      this.e = e;
      this.endTimeNs = System.nanoTime();
    }

    public String queryId()
    {
      return baseQuery.getId();
    }

    public long runTimeNs()
    {
      return endTimeNs - startNs;
    }

    public long runTimeMs()
    {
      return TimeUnit.NANOSECONDS.toMillis(runTimeNs());
    }

    public boolean succeeded()
    {
      return e == null;
    }

    public String identity()
    {
      return authenticationResult == null ? null : authenticationResult.getIdentity();
    }

    public boolean wasInterrupted()
    {
      if (e == null) {
        return false;
      }
      return (e instanceof QueryInterruptedException || e instanceof QueryTimeoutException);
    }

    /**
     * Emit final metrics for the query.
     */
    public void emitMetrics()
    {
      @SuppressWarnings("unchecked")
      QueryMetrics<?> queryMetrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
          toolChest,
          baseQuery,
          StringUtils.nullToEmptyNonDruidDataString(stats.remoteAddress)
      );
      queryMetrics.success(succeeded());
      queryMetrics.reportQueryTime(runTimeNs());

      if (bytesWritten >= 0) {
        queryMetrics.reportQueryBytes(bytesWritten);
      }

      String identify = identity();
      if (identify != null) {
        queryMetrics.identity(identify);
      }
    }

    /**
     * Gather the query statistics for metrics.
     */
    public QueryStats queryStats()
    {
      final Map<String, Object> statsMap = new LinkedHashMap<>();
      statsMap.put("query/time", TimeUnit.NANOSECONDS.toMillis(runTimeNs()));
      statsMap.put("query/bytes", bytesWritten);
      statsMap.put("success", succeeded());

      String identity = identity();
      if (identity != null) {
        statsMap.put("identity", identity);
      }

      if (e != null) {
        statsMap.put("exception", e.toString());
        if (QueryContexts.isDebug(baseQuery)) {
          log.error(e, "Exception while processing queryId [%s]", baseQuery.getId());
        } else {
          log.noStackTrace().error(e, "Exception while processing queryId [%s]", baseQuery.getId());
        }
        if (wasInterrupted()) {
          // Mimic behavior from QueryResource, where this code was originally taken from.
          statsMap.put("interrupted", true);
          statsMap.put("reason", e.toString());
        }
      }
      return new QueryStats(statsMap);
    }

    public boolean includeTrailer()
    {
      return trailerOptions != null;
    }

    /**
     * Create the response trailer, if requested. Includes the query profile as well
     * as various other trailer fields.
     * @return
     */
    public ResponseContext trailer()
    {
      // Should not get here if no trailer options were selected
      assert trailerOptions != null;

      // Note: from here on down the code here is basically the same as
      // for SqlLifecycle. TODO: Factor out common code. Leaving for now
      // because this are is under development.

      // Include details?
      final ResponseContext trailerContext;
      if (trailerOptions.contains(QueryContexts.TRAILER_CONTEXT)) {
        trailerContext = responseContext.trailerCopy();
      } else {
        trailerContext = ResponseContext.createEmpty();
      }

      // Include metrics?
      if (trailerOptions.contains(QueryContexts.TRAILER_METRICS)) {
        trailerContext.add(
            ResponseContext.Keys.METRICS,
            MultiQueryMetricsCollector.newCollector().add(
                SingleQueryMetricsCollector
                    .newCollector()
                    .setQueryStart(getStartMs())
                    // This is not the same as query/time, its a bit shorter
                    .setQueryMs(runTimeMs())
                    .setResultRows(rowCount)
            )
        );
      }
      return trailerContext;
    }
  }

  private final QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker texasRanger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  @SuppressWarnings("unused")
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final AuthorizerMapper authorizerMapper;
  private final DefaultQueryConfig defaultQueryConfig;
  private final long startMs;
  private final long startNs;

  private State state = State.NEW;
  private AuthenticationResult authenticationResult;
  @SuppressWarnings("rawtypes")
  private QueryToolChest toolChest;
  private Query<?> baseQuery;
  private final LifecycleStats stats;

  public QueryLifecycle(
      final QueryToolChestWarehouse warehouse,
      final QuerySegmentWalker texasRanger,
      final GenericQueryMetricsFactory queryMetricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final AuthorizerMapper authorizerMapper,
      final DefaultQueryConfig defaultQueryConfig,
      final long startMs,
      final long startNs
  )
  {
    this.warehouse = warehouse;
    this.texasRanger = texasRanger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.authorizerMapper = authorizerMapper;
    this.defaultQueryConfig = defaultQueryConfig;
    this.startMs = startMs;
    this.startNs = startNs;
    this.stats = new LifecycleStats();
  }

  public LifecycleStats stats()
  {
    return stats;
  }

  /**
   * For callers who have already authorized their query, and where simplicity is desired over flexibility. This method
   * does it all in one call. Logs and metrics are emitted when the Sequence is either fully iterated or throws an
   * exception.
   *
   * @param query                 the query
   * @param authenticationResult  authentication result indicating identity of the requester
   * @param authorizationResult   authorization result of requester
   *
   * @return results
   */
  @SuppressWarnings("unchecked")
  public <T> QueryResponse<T> runSimple(
      final Query<T> query,
      final AuthenticationResult authenticationResult,
      final Access authorizationResult
  )
  {
    initialize(query);

    final QueryResponse<T> queryResponse;

    try {
      preAuthorized(authenticationResult, authorizationResult);
      if (!authorizationResult.isAllowed()) {
        throw new ISE("Unauthorized");
      }

      queryResponse = (QueryResponse<T>) execute();
    }
    catch (Throwable e) {
      emitLogsAndMetrics(e);
      throw e;
    }

    return queryResponse.wrap(
        new SequenceWrapper()
        {
          @Override
          public void after(final boolean isDone, final Throwable thrown)
          {
            emitLogsAndMetrics(thrown);
          }
        }
    );
  }

  /**
   * Initializes this object to execute a specific query. Does not actually execute the query.
   *
   * @param baseQuery the query
   */
  public void initialize(final Query<?> baseQuery)
  {
    transition(State.NEW, State.INITIALIZED);

    String queryId = baseQuery.getId();
    if (Strings.isNullOrEmpty(queryId)) {
      queryId = UUID.randomUUID().toString();
    }

    Map<String, Object> mergedUserAndConfigContext;
    if (baseQuery.getContext() != null) {
      mergedUserAndConfigContext = BaseQuery.computeOverriddenContext(
          defaultQueryConfig.getContext(),
          baseQuery.getContext()
      );
    } else {
      mergedUserAndConfigContext = defaultQueryConfig.getContext();
    }

    this.baseQuery = baseQuery.withOverriddenContext(mergedUserAndConfigContext).withId(queryId);
    this.toolChest = warehouse.getToolChest(baseQuery);
  }

  /**
   * Authorize the query. Will return an Access object denoting whether the query is authorized or not.
   *
   * @param req HTTP request object of the request. If provided, the auth-related fields in the HTTP request
   *            will be automatically set.
   *
   * @return authorization result
   */
  public Access authorize(HttpServletRequest req)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);
    return doAuthorize(
        AuthorizationUtils.authenticationResultFromRequest(req),
        AuthorizationUtils.authorizeAllResourceActions(
            req,
            Iterables.transform(
                baseQuery.getDataSource().getTableNames(),
                AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
            ),
            authorizerMapper
        )
    );
  }

  private void preAuthorized(final AuthenticationResult authenticationResult, final Access access)
  {
    // gotta transition those states, even if we are already authorized
    transition(State.INITIALIZED, State.AUTHORIZING);
    doAuthorize(authenticationResult, access);
  }

  private Access doAuthorize(final AuthenticationResult authenticationResult, final Access authorizationResult)
  {
    Preconditions.checkNotNull(authenticationResult, "authenticationResult");
    Preconditions.checkNotNull(authorizationResult, "authorizationResult");

    if (!authorizationResult.isAllowed()) {
      // Not authorized; go straight to Jail, do not pass Go.
      transition(State.AUTHORIZING, State.UNAUTHORIZED);
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
    }

    this.authenticationResult = authenticationResult;

    return authorizationResult;
  }

  /**
   * Execute the query. Can only be called if the query has been authorized. Note that query logs and metrics will
   * not be emitted automatically when the Sequence is fully iterated. It is the caller's responsibility to call
   * {@link #emitLogsAndMetrics(Throwable)} to emit logs and metrics.
   *
   * @return result sequence and response context
   */
  public QueryResponse<?> execute()
  {
    transition(State.AUTHORIZED, State.EXECUTING);

    final ConcurrentResponseContext responseContext = DirectDruidClient.makeResponseContextForQuery();

    //noinspection unchecked
    final Sequence<?> res =
        QueryPlus.wrap(baseQuery)
                 .withIdentity(authenticationResult.getIdentity())
                 .run(texasRanger, responseContext);

    return QueryResponse.create(res == null ? Sequences.empty() : res, responseContext);
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e exception that occurred while processing this query
   */
  public void emitLogsAndMetrics(
      @Nullable final Throwable e
  )
  {
    if (baseQuery == null) {
      // Never initialized, don't log or emit anything.
      return;
    }

    if (state == State.DONE) {
      log.warn("Tried to emit logs and metrics twice for query[%s]!", baseQuery.getId());
    }

    state = State.DONE;

    stats.onCompletion(e);
    try {
      stats.emitMetrics();

      requestLogger.logNativeQuery(
          RequestLogLine.forNative(
              baseQuery,
              DateTimes.utc(startMs),
              StringUtils.nullToEmptyNonDruidDataString(stats.remoteAddress),
              stats.queryStats()
          )
      );
    }
    catch (Exception ex) {
      log.error(ex, "Unable to log query [%s]!", baseQuery);
    }
  }

  public Query<?> getQuery()
  {
    return baseQuery;
  }

  @SuppressWarnings("rawtypes")
  public QueryToolChest getToolChest()
  {
    if (state.compareTo(State.INITIALIZED) < 0) {
      throw new ISE("Not yet initialized");
    }

    //noinspection unchecked
    return toolChest;
  }

  public long getStartMs()
  {
    return startMs;
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s].", from, to);
    }

    state = to;
  }

  enum State
  {
    NEW,
    INITIALIZED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }
}
