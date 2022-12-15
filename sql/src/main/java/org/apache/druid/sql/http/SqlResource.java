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
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.ResultPusher;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Path("/druid/v2/sql/")
public class SqlResource
{
  public static final String SQL_QUERY_ID_RESPONSE_HEADER = "X-Druid-SQL-Query-Id";
  public static final String SQL_HEADER_RESPONSE_HEADER = "X-Druid-SQL-Header-Included";
  public static final String SQL_HEADER_VALUE = "yes";
  private static final Logger log = new Logger(SqlResource.class);
  public static final SqlResourceQueryMetricCounter QUERY_METRIC_COUNTER = new SqlResourceQueryMetricCounter();

  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final SqlStatementFactory sqlStatementFactory;
  private final SqlLifecycleManager sqlLifecycleManager;
  private final ServerConfig serverConfig;
  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;

  @Inject
  SqlResource(
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory,
      final SqlLifecycleManager sqlLifecycleManager,
      final ServerConfig serverConfig,
      ResponseContextConfig responseContextConfig,
      @Self DruidNode selfNode
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.sqlStatementFactory = Preconditions.checkNotNull(sqlStatementFactory, "sqlStatementFactory");
    this.sqlLifecycleManager = Preconditions.checkNotNull(sqlLifecycleManager, "sqlLifecycleManager");
    this.serverConfig = Preconditions.checkNotNull(serverConfig, "serverConfig");
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;

  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Nullable
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  )
  {
    final HttpStatement stmt = sqlStatementFactory.httpStatement(sqlQuery, req);
    final String sqlQueryId = stmt.sqlQueryId();
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));

      // We use an async context not because we are actually going to run this async, but because we want to delay
      // the decision of what the response code should be until we have gotten the first few data points to return.
      // Returning a Response object from this point forward requires that object to know the status code, which we
      // don't actually know until we are in the accumulator, but if we try to return a Response object from the
      // accumulator, we cannot properly stream results back, because the accumulator won't release control of the
      // Response until it has consumed the underlying Sequence.
      final AsyncContext asyncContext = req.startAsync();

      try {
        ResultPusher pusher = new SqlResourceResultPusher(asyncContext, sqlQueryId, stmt, sqlQuery);
        pusher.push();
        return null;
      }
      finally {
        asyncContext.complete();
      }
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

    // Considers only datasource and table resources; not context
    // key resources when checking permissions. This means that a user's
    // permission to cancel a query depends on the datasource, not the
    // context variables used in the query.
    Set<ResourceAction> resources = lifecycles
        .stream()
        .flatMap(lifecycle -> lifecycle.resources().stream())
        .collect(Collectors.toSet());
    Access access = AuthorizationUtils.authorizeAllResourceActions(
        req,
        resources,
        authorizerMapper
    );

    if (access.isAllowed()) {
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

  private class SqlResourceResultPusher extends ResultPusher
  {
    private final String sqlQueryId;
    private final HttpStatement stmt;
    private final SqlQuery sqlQuery;

    public SqlResourceResultPusher(
        AsyncContext asyncContext,
        String sqlQueryId,
        HttpStatement stmt,
        SqlQuery sqlQuery
    )
    {
      super(
          (HttpServletResponse) asyncContext.getResponse(),
          SqlResource.this.jsonMapper,
          SqlResource.this.responseContextConfig,
          SqlResource.this.selfNode,
          SqlResource.QUERY_METRIC_COUNTER,
          sqlQueryId,
          MediaType.APPLICATION_JSON_TYPE
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
        private ResultSet thePlan;

        @Override
        @Nullable
        @SuppressWarnings({"unchecked", "rawtypes"})
        public QueryResponse<Object> start(HttpServletResponse response)
        {
          response.setHeader(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId);

          final QueryResponse<Object[]> retVal;
          try {
            thePlan = stmt.plan();
            retVal = thePlan.run();
          }
          catch (RelOptPlanner.CannotPlanException e) {
            recordFailure(e);
            final SqlPlanningException wrappedException = new SqlPlanningException(
                SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
                e.getMessage()
            );

            writeErrorResponse(HttpServletResponse.SC_BAD_REQUEST, response, wrappedException);
            return null;
          }
          // There is a claim that Calcite sometimes throws a java.lang.AssertionError, but we do not have a test that can
          // reproduce it checked into the code (the best we have is something that uses mocks to throw an Error, which is
          // dubious at best).  We keep this just in case, but it might be best to remove it and see where the
          // AssertionErrors are coming from and do something to ensure that they don't actually make it out of Calcite
          catch (AssertionError e) {
            log.warn(e, "AssertionError killed query: %s", sqlQuery);

            // We wrap the exception here so that we get the sanitization.  java.lang.AssertionError apparently
            // doesn't implement org.apache.druid.common.exception.SanitizableException.
            final QueryInterruptedException wrappedEx = new QueryInterruptedException(e);
            recordFailure(wrappedEx);
            writeErrorResponse(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, response, wrappedEx);
            return null;
          }

          return (QueryResponse) retVal;
        }

        private void writeErrorResponse(
            int statusCode,
            HttpServletResponse response,
            Exception wrappedEx
        )
        {
          response.setStatus(statusCode);
          response.setContentType("application/json");
          try (OutputStream out = response.getOutputStream()) {
            writeExceptionInternal(wrappedEx, out);
          }
          catch (IOException ioException) {
            log.warn(ioException, "Suppressing ioException when streaming error response for query[%s].", sqlQueryId);
          }
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
      writeExceptionInternal(ex, out);
    }

    private void writeExceptionInternal(Exception ex, OutputStream out) throws IOException
    {
      if (ex instanceof SanitizableException) {
        ex = serverConfig.getErrorResponseTransformStrategy().transformIfNeeded((SanitizableException) ex);
      }
      out.write(jsonMapper.writeValueAsBytes(ex));
    }
  }
}
