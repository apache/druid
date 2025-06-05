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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.sun.jersey.api.core.HttpContext;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.QueryResultPusher;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.calcite.run.SqlEngine;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
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
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Path(SqlResource.PATH)
public class SqlResource
{
  public static final String PATH = "/druid/v2/sql/";
  public static final String SQL_QUERY_ID_RESPONSE_HEADER = "X-Druid-SQL-Query-Id";
  public static final String SQL_HEADER_RESPONSE_HEADER = "X-Druid-SQL-Header-Included";
  public static final String SQL_HEADER_VALUE = "yes";

  private static final Logger log = new Logger(SqlResource.class);
  private static final SqlResourceQueryMetricCounter QUERY_METRIC_COUNTER = new SqlResourceQueryMetricCounter();

  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final ServerConfig serverConfig;
  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;
  private final SqlLifecycleManager sqlLifecycleManager;
  private final SqlEngineRegistry sqlEngineRegistry;

  @VisibleForTesting
  @Inject
  public SqlResource(
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper,
      final ServerConfig serverConfig,
      final SqlLifecycleManager sqlLifecycleManager,
      final SqlEngineRegistry sqlEngineRegistry,
      ResponseContextConfig responseContextConfig,
      @Self DruidNode selfNode
  )
  {
    this.sqlEngineRegistry = Preconditions.checkNotNull(sqlEngineRegistry, "sqlEngineRegistry");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.serverConfig = Preconditions.checkNotNull(serverConfig, "serverConfig");
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
    this.sqlLifecycleManager = Preconditions.checkNotNull(sqlLifecycleManager, "sqlLifecycleManager");
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
      queries.addAll(sqlEngine.getRunningQueries(selfOnly != null, authenticationResult, stateReadAccess));
    }

    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    return Response.ok().entity(new GetQueriesResponse(queries)).build();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Nullable
  public Response doPost(
      @Context final HttpServletRequest req,
      @Context final HttpContext httpContext
  )
  {
    return doPost(SqlQuery.from(httpContext), req);
  }

  /**
   * This method is defined as public so that tests can access it
   */
  public Response doPost(
      final SqlQuery sqlQuery,
      final HttpServletRequest req
  )
  {
    final String engineName = sqlQuery.queryContext().getEngine();
    final SqlEngine engine = sqlEngineRegistry.getEngine(engineName);
    final HttpStatement stmt = engine.getSqlStatementFactory().httpStatement(sqlQuery, req);
    final String sqlQueryId = stmt.sqlQueryId();
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));
      log.info("Test Received SQL query with id [%s]", sqlQueryId);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      QueryResultPusher pusher = makePusher(req, stmt, sqlQuery);
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

  private SqlResourceQueryResultPusher makePusher(HttpServletRequest req, HttpStatement stmt, SqlQuery sqlQuery)
  {
    final String sqlQueryId = stmt.sqlQueryId();
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId);

    if (sqlQuery.includeHeader()) {
      headers.put(SQL_HEADER_RESPONSE_HEADER, SQL_HEADER_VALUE);
    }

    return new SqlResourceQueryResultPusher(req, sqlQueryId, stmt, sqlQuery, headers);
  }

  private class SqlResourceQueryResultPusher extends QueryResultPusher
  {
    private final String sqlQueryId;
    private final HttpStatement stmt;
    private final SqlQuery sqlQuery;

    public SqlResourceQueryResultPusher(
        HttpServletRequest req,
        String sqlQueryId,
        HttpStatement stmt,
        SqlQuery sqlQuery,
        Map<String, String> headers
    )
    {
      super(
          req,
          jsonMapper,
          responseContextConfig,
          selfNode,
          SqlResource.QUERY_METRIC_COUNTER,
          sqlQueryId,
          MediaType.APPLICATION_JSON_TYPE,
          headers
      );
      this.sqlQueryId = sqlQueryId;
      this.stmt = stmt;
      this.sqlQuery = sqlQuery;
    }

    @Override
    public ResultsWriter start()
    {
      return new ResultsWriter()
      {
        private QueryResponse<Object[]> queryResponse;
        private ResultSet thePlan;

        @Override
        @Nullable
        public Response.ResponseBuilder start()
        {
          thePlan = stmt.plan();
          queryResponse = thePlan.run();
          return null;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public QueryResponse<Object> getQueryResponse()
        {
          return (QueryResponse) queryResponse;
        }

        @Override
        public Writer makeWriter(OutputStream out) throws IOException
        {
          ResultFormat.Writer writer = sqlQuery.getResultFormat().createFormatter(out, jsonMapper);
          final SqlRowTransformer rowTransformer = thePlan.createRowTransformer();

          return new Writer()
          {

            @Override
            public void writeResponseStart() throws IOException
            {
              writer.writeResponseStart();

              if (sqlQuery.includeHeader()) {
                writer.writeHeader(
                    rowTransformer.getRowType(),
                    sqlQuery.includeTypesHeader(),
                    sqlQuery.includeSqlTypesHeader()
                );
              }
            }

            @Override
            public void writeRow(Object obj) throws IOException
            {
              Object[] row = (Object[]) obj;

              writer.writeRowStart();
              for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
                final Object value = rowTransformer.transform(row, i);
                writer.writeRowField(rowTransformer.getFieldList().get(i), value);
              }
              writer.writeRowEnd();
            }

            @Override
            public void writeResponseEnd() throws IOException
            {
              writer.writeResponseEnd();
            }

            @Override
            public void close() throws IOException
            {
              writer.close();
            }
          };
        }

        @Override
        public void recordSuccess(long numBytes)
        {
          stmt.reporter().succeeded(numBytes);
        }

        @Override
        public void recordFailure(Exception e)
        {
          if (QueryLifecycle.shouldLogStackTrace(e, sqlQuery.queryContext())) {
            log.warn(e, "Exception while processing sqlQueryId[%s]", sqlQueryId);
          } else {
            log.noStackTrace().warn(e, "Exception while processing sqlQueryId[%s]", sqlQueryId);
          }
          stmt.reporter().failed(e);
        }

        @Override
        public void close()
        {
          stmt.close();
        }
      };
    }

    @Override
    public void writeException(Exception ex, OutputStream out) throws IOException
    {
      if (ex instanceof SanitizableException) {
        ex = serverConfig.getErrorResponseTransformStrategy().transformIfNeeded((SanitizableException) ex);
      }
      out.write(jsonMapper.writeValueAsBytes(ex));
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
}
