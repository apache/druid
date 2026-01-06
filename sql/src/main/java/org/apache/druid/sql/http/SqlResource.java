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

package org.apache.druid.sql.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.api.core.HttpContext;
import org.apache.druid.common.exception.ErrorResponseTransformStrategy;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryResultPusher;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.calcite.run.SqlEngine;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Path(SqlResource.PATH)
public class SqlResource
{
  public static final String PATH = "/druid/v2/sql/";
  public static final String SQL_QUERY_ID_RESPONSE_HEADER = "X-Druid-SQL-Query-Id";
  public static final String SQL_HEADER_RESPONSE_HEADER = "X-Druid-SQL-Header-Included";
  public static final String SQL_HEADER_VALUE = "yes";

  private static final Logger log = new Logger(SqlResource.class);
  public static final SqlResourceQueryMetricCounter QUERY_METRIC_COUNTER = new SqlResourceQueryMetricCounter();

  private final AuthorizerMapper authorizerMapper;
  private final SqlResourceQueryResultPusherFactory resultPusherFactory;
  private final SqlLifecycleManager sqlLifecycleManager;
  private final SqlEngineRegistry sqlEngineRegistry;
  private final DefaultQueryConfig defaultQueryConfig;
  private final ServerConfig serverConfig;

  @VisibleForTesting
  @Inject
  public SqlResource(
      final AuthorizerMapper authorizerMapper,
      final SqlLifecycleManager sqlLifecycleManager,
      final SqlEngineRegistry sqlEngineRegistry,
      final SqlResourceQueryResultPusherFactory resultPusherFactory,
      final DefaultQueryConfig defaultQueryConfig,
      final ServerConfig serverConfig
  )
  {
    this.resultPusherFactory = resultPusherFactory;
    this.sqlEngineRegistry = Preconditions.checkNotNull(sqlEngineRegistry, "sqlEngineRegistry");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.sqlLifecycleManager = Preconditions.checkNotNull(sqlLifecycleManager, "sqlLifecycleManager");
    this.defaultQueryConfig = Preconditions.checkNotNull(defaultQueryConfig, "defaultQueryConfig");
    this.serverConfig = serverConfig;
  }

  @GET
  @Path("/engines")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSupportedEngines(@Context final HttpServletRequest request)
  {
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    Set<EngineInfo> engines = sqlEngineRegistry.getSupportedEngines()
                                               .stream()
                                               .map(EngineInfo::new)
                                               .collect(Collectors.toSet());
    return Response.ok(new SupportedEnginesResponse(engines)).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes({
      MediaType.APPLICATION_JSON,
      MediaType.TEXT_PLAIN,
      MediaType.APPLICATION_FORM_URLENCODED,
  })
  @Nullable
  public Response doPost(
      @Context final HttpServletRequest req,
      @Context final HttpContext httpContext
  )
  {
    return doPost(SqlQuery.from(httpContext), req);
  }

  /**
   * API to list all running queries, for all engines that supports such listings.
   *
   * @param selfOnly if true, return queries running on this server. If false, return queries running on all servers.
   * @param request  http request.
   */
  @GET
  @Path("/queries")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetRunningQueries(
      @QueryParam("selfOnly") final String selfOnly,
      @Context final HttpServletRequest request
  )
  {
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(request);
    final AuthorizationResult stateReadAccess = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );

    final Collection<SqlEngine> engines = sqlEngineRegistry.getAllEngines();
    final List<QueryInfo> queries = new ArrayList<>();

    // Get running queries from all engines that support it.
    for (SqlEngine sqlEngine : engines) {
      queries.addAll(sqlEngine.getRunningQueries(selfOnly != null, authenticationResult, stateReadAccess).getQueries());
    }

    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    return Response.ok().entity(new GetQueriesResponse(queries)).build();
  }

  /**
   * API to get query reports, for all engines that support such reports.
   *
   * @param sqlQueryId SQL query ID
   * @param selfOnly   if true, check reports from this server only. If false, check reports on all servers.
   * @param request    http request.
   */
  @GET
  @Path("/queries/{sqlQueryId}/report")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetQueryReport(
      @PathParam("sqlQueryId") final String sqlQueryId,
      @QueryParam("selfOnly") final String selfOnly,
      @Context final HttpServletRequest request
  )
  {
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(request);
    final AuthorizationResult stateReadAccess = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );

    final Collection<SqlEngine> engines = sqlEngineRegistry.getAllEngines();

    // Get task report from the first engine that recognizes the SQL query ID.
    GetReportResponse retVal = null;
    for (SqlEngine sqlEngine : engines) {
      retVal = sqlEngine.getQueryReport(
          sqlQueryId,
          selfOnly != null,
          authenticationResult,
          stateReadAccess
      );

      if (retVal != null) {
        break;
      }
    }

    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    if (retVal == null) {
      return Response.status(Status.NOT_FOUND).entity(new GetReportResponse(null, null)).build();
    } else {
      return Response.ok().entity(retVal).build();
    }
  }

  /**
   * This method is defined as public so that tests can access it
   */
  public Response doPost(
      final SqlQuery sqlQuery,
      final HttpServletRequest req
  )
  {
    final HttpStatement stmt;
    final QueryContext queryContext;

    try {
      SqlQueryPlus sqlQueryPlus = makeSqlQueryPlus(sqlQuery, req, defaultQueryConfig.getContext());

      // Redefine queryContext to include SET parameters and default context.
      queryContext = new QueryContext(sqlQueryPlus.context());
      final String engineName = queryContext.getEngine();
      final SqlEngine engine = sqlEngineRegistry.getEngine(engineName);
      stmt = engine.getSqlStatementFactory().httpStatement(sqlQueryPlus, req);
    }
    catch (Exception e) {
      // Can't use the queryContext with SETs since it might not have been created yet. Use the original one.
      return handleExceptionBeforeStatementCreated(
          e,
          sqlQuery.queryContext(),
          serverConfig.getErrorResponseTransformStrategy()
      );
    }

    final String currThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", stmt.sqlQueryId()));

      QueryResultPusher pusher = resultPusherFactory.factorize(req, stmt, sqlQuery);
      return pusher.push();
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelQuery(
      @PathParam("id") String sqlQueryId,
      @Context final HttpServletRequest req
  )
  {
    log.debug("Received cancel request for query [%s]", sqlQueryId);

    List<Cancelable> lifecycles = sqlLifecycleManager.getAll(sqlQueryId);
    if (lifecycles.isEmpty()) {
      return Response.status(Status.NOT_FOUND).build();
    }

    final AuthorizationResult authResult = authorizeCancellation(req, lifecycles);

    if (authResult.allowAccessWithNoRestriction()) {
      // should remove only the lifecycles in the snapshot.
      sqlLifecycleManager.removeAll(sqlQueryId, lifecycles);
      lifecycles.forEach(Cancelable::cancel);
      return Response.status(Status.ACCEPTED).build();
    } else {
      return Response.status(Status.FORBIDDEN).build();
    }
  }

  /**
   * The SqlResource only generates metrics and doesn't keep track of aggregate counts of successful/failed/interrupted
   * queries, so this implementation is effectively just a noop.
   */
  private static class SqlResourceQueryMetricCounter implements QueryResource.QueryMetricCounter
  {
    @Override
    public void incrementSuccess()
    {
    }

    @Override
    public void incrementFailed()
    {
    }

    @Override
    public void incrementInterrupted()
    {
    }

    @Override
    public void incrementTimedOut()
    {
    }
  }

  /**
   * Authorize a query cancellation operation.
   * <p>
   * Considers only datasource and table resources; not context key resources when checking permissions. This means
   * that a user's permission to cancel a query depends on the datasource, not the context variables used in the query.
   */
  public AuthorizationResult authorizeCancellation(final HttpServletRequest req, final List<Cancelable> cancelables)
  {
    Set<ResourceAction> resources = cancelables
        .stream()
        .flatMap(lifecycle -> lifecycle.resources().stream())
        .collect(Collectors.toSet());
    return AuthorizationUtils.authorizeAllResourceActions(
        req,
        resources,
        authorizerMapper
    );
  }

  /**
   * Create a {@link SqlQueryPlus}, which involves parsing the query from {@link SqlQuery#getQuery()} and
   * extracing any SET parameters into the query context.
   */
  public static SqlQueryPlus makeSqlQueryPlus(
      final SqlQuery sqlQuery,
      final HttpServletRequest req,
      final Map<String, Object> defaultQueryConfig
  )
  {
    return SqlQueryPlus.builder()
                       .sql(sqlQuery.getQuery())
                       .systemDefaultContext(defaultQueryConfig)
                       .queryContext(sqlQuery.getContext())
                       .parameters(sqlQuery.getParameterList())
                       .auth(AuthorizationUtils.authenticationResultFromRequest(req))
                       .build();
  }

  /**
   * Generates a response for a {@link DruidException} that occurs prior to the {@link HttpStatement} being created.
   */
  public static Response handleExceptionBeforeStatementCreated(
      final Exception e,
      final QueryContext queryContext,
      final ErrorResponseTransformStrategy strategy
  )
  {
    if (e instanceof DruidException) {
      final String sqlQueryId = queryContext.getString(QueryContexts.CTX_SQL_QUERY_ID);
      String errorId = sqlQueryId == null ? UUID.randomUUID().toString() : sqlQueryId;
      Optional<Exception> transformed = strategy.maybeTransform((DruidException) e, Optional.of(errorId));
      if (transformed.isPresent()) {
        // Log the exception here itself, since the error has been transformed.
        log.error(e, StringUtils.format("External Error ID: [%s]", errorId));
      }
      return QueryResultPusher.handleDruidExceptionBeforeResponseStarted(
          (DruidException) transformed.orElse(e),
          MediaType.APPLICATION_JSON_TYPE,
          sqlQueryId != null
          ? ImmutableMap.<String, String>builder()
                        .put(QueryResource.QUERY_ID_RESPONSE_HEADER, sqlQueryId)
                        .put(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId)
                        .build()
          : Collections.emptyMap()
      );
    } else {
      return QueryResultPusher.handleDruidExceptionBeforeResponseStarted(
          DruidException.forPersona(DruidException.Persona.OPERATOR)
                        .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                        .build(e, "Cannot handle query"),
          MediaType.APPLICATION_JSON_TYPE,
          Collections.emptyMap()
      );
    }
  }
}
