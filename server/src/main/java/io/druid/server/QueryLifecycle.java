/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DirectDruidClient;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.DruidMetrics;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryPlus;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
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
 * <li>Logging ({@link #emitLogsAndMetrics(Throwable, String, long)}</li>
 * </ol>
 *
 * This object is not thread-safe.
 */
public class QueryLifecycle
{
  private static final Logger log = new Logger(QueryLifecycle.class);

  private final QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker texasRanger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final AuthorizerMapper authorizerMapper;
  private final long startMs;
  private final long startNs;

  private State state = State.NEW;
  private AuthenticationResult authenticationResult;
  private QueryToolChest toolChest;
  private QueryPlus queryPlus;

  public QueryLifecycle(
      final QueryToolChestWarehouse warehouse,
      final QuerySegmentWalker texasRanger,
      final GenericQueryMetricsFactory queryMetricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final AuthorizerMapper authorizerMapper,
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
    this.startMs = startMs;
    this.startNs = startNs;
  }

  /**
   * For callers where simplicity is desired over flexibility. This method does it all in one call. If the request
   * is unauthorized, an IllegalStateException will be thrown. Logs and metrics are emitted when the Sequence is
   * either fully iterated or throws an exception.
   *
   * @param query                the query
   * @param authenticationResult authentication result indicating identity of the requester
   * @param remoteAddress        remote address, for logging; or null if unknown
   *
   * @return results
   */
  @SuppressWarnings("unchecked")
  public <T> Sequence<T> runSimple(
      final Query<T> query,
      final AuthenticationResult authenticationResult,
      @Nullable final String remoteAddress
  )
  {
    initialize(query);

    final Sequence<T> results;

    try {
      final Access access = authorize(authenticationResult);
      if (!access.isAllowed()) {
        throw new ISE("Unauthorized");
      }

      final QueryLifecycle.QueryResponse queryResponse = execute();
      results = queryResponse.getResults();
    }
    catch (Throwable e) {
      emitLogsAndMetrics(e, remoteAddress, -1);
      throw e;
    }

    return Sequences.wrap(
        results,
        new SequenceWrapper()
        {
          @Override
          public void after(final boolean isDone, final Throwable thrown) throws Exception
          {
            emitLogsAndMetrics(thrown, remoteAddress, -1);
          }
        }
    );
  }

  /**
   * Initializes this object to execute a specific query. Does not actually execute the query.
   *
   * @param baseQuery the query
   */
  @SuppressWarnings("unchecked")
  public void initialize(final Query baseQuery)
  {
    transition(State.NEW, State.INITIALIZED);

    String queryId = baseQuery.getId();
    if (Strings.isNullOrEmpty(queryId)) {
      queryId = UUID.randomUUID().toString();
    }

    this.queryPlus = QueryPlus.wrap(baseQuery.withId(queryId));
    this.toolChest = warehouse.getToolChest(baseQuery);
  }

  /**
   * Authorize the query. Will return an Access object denoting whether the query is authorized or not.
   *
   * @param authenticationResult authentication result indicating the identity of the requester
   *
   * @return authorization result
   */
  public Access authorize(final AuthenticationResult authenticationResult)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);
    return doAuthorize(
        authenticationResult,
        AuthorizationUtils.authorizeAllResourceActions(
            authenticationResult,
            Iterables.transform(
                queryPlus.getQuery().getDataSource().getNames(),
                AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
            ),
            authorizerMapper
        )
    );
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
                queryPlus.getQuery().getDataSource().getNames(),
                AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
            ),
            authorizerMapper
        )
    );
  }

  private Access doAuthorize(final AuthenticationResult authenticationResult, final Access authorizationResult)
  {
    if (!authorizationResult.isAllowed()) {
      // Not authorized; go straight to Jail, do not pass Go.
      transition(State.AUTHORIZING, State.UNAUTHORIZED);
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
    }

    this.authenticationResult = authenticationResult;

    final QueryMetrics queryMetrics = queryPlus.getQueryMetrics();

    if (queryMetrics != null) {
      queryMetrics.identity(authenticationResult.getIdentity());
    }

    return authorizationResult;
  }

  /**
   * Execute the query. Can only be called if the query has been authorized. Note that query logs and metrics will
   * not be emitted automatically when the Sequence is fully iterated. It is the caller's responsibility to call
   * {@link #emitLogsAndMetrics(Throwable, String, long)} to emit logs and metrics.
   *
   * @return result sequence and response context
   */
  public QueryResponse execute()
  {
    transition(State.AUTHORIZED, State.EXECUTING);

    final Map<String, Object> responseContext = DirectDruidClient.makeResponseContextForQuery(
        queryPlus.getQuery(),
        System.currentTimeMillis()
    );

    final Sequence res = queryPlus.run(texasRanger, responseContext);

    return new QueryResponse(res == null ? Sequences.empty() : res, responseContext);
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  @SuppressWarnings("unchecked")
  public void emitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    if (queryPlus == null) {
      // Never initialized, don't log or emit anything.
      return;
    }

    if (state == State.DONE) {
      log.warn("Tried to emit logs and metrics twice for query[%s]!", queryPlus.getQuery().getId());
    }

    state = State.DONE;

    final Query query = queryPlus != null ? queryPlus.getQuery() : null;
    final boolean success = e == null;

    try {
      final long queryTimeNs = System.nanoTime() - startNs;
      QueryMetrics queryMetrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
          toolChest,
          queryPlus.getQuery(),
          Strings.nullToEmpty(remoteAddress)
      );
      queryMetrics.success(success);
      queryMetrics.reportQueryTime(queryTimeNs);

      if (bytesWritten >= 0) {
        queryMetrics.reportQueryBytes(bytesWritten);
      }

      queryMetrics.emit(emitter);

      final Map<String, Object> statsMap = new LinkedHashMap<>();
      statsMap.put("query/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs));
      statsMap.put("query/bytes", bytesWritten);
      statsMap.put("success", success);
      if (authenticationResult != null) {
        statsMap.put("identity", authenticationResult.getIdentity());
      }
      if (e != null) {
        statsMap.put("exception", e.toString());

        if (e instanceof QueryInterruptedException) {
          // Mimic behavior from QueryResource, where this code was originally taken from.
          log.warn(e, "Exception while processing queryId [%s]", queryPlus.getQuery().getId());
          statsMap.put("interrupted", true);
          statsMap.put("reason", e.toString());
        }
      }

      requestLogger.log(
          new RequestLogLine(
              DateTimes.utc(startMs),
              Strings.nullToEmpty(remoteAddress),
              queryPlus.getQuery(),
              new QueryStats(statsMap)
          )
      );
    }
    catch (Exception ex) {
      log.error(ex, "Unable to log query [%s]!", query);
    }
  }

  public Query getQuery()
  {
    return queryPlus.getQuery();
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

  public static class QueryResponse
  {
    private final Sequence results;
    private final Map<String, Object> responseContext;

    private QueryResponse(final Sequence results, final Map<String, Object> responseContext)
    {
      this.results = results;
      this.responseContext = responseContext;
    }

    public Sequence getResults()
    {
      return results;
    }

    public Map<String, Object> getResponseContext()
    {
      return responseContext;
    }
  }
}
