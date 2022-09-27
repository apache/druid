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

package org.apache.druid.msq.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.util.Collections;

/**
 * Endpoint for SQL execution using MSQ tasks.
 *
 * Unlike the SQL endpoint in {@link SqlResource}, this endpoint returns task IDs instead of inline results. Queries
 * are executed asynchronously using MSQ tasks via the indexing service (Overlord + MM or Indexer). This endpoint
 * does not provide a way for users to get the status or results of a query. That must be done using Overlord APIs
 * for status and reports.
 *
 * One exception: EXPLAIN query results are returned inline by this endpoint, in the same way as {@link SqlResource}
 * would return them.
 *
 * This endpoint does not support system tables or INFORMATION_SCHEMA. Queries on those tables result in errors.
 */
@Path("/druid/v2/sql/task/")
public class SqlTaskResource
{
  private static final Logger log = new Logger(SqlTaskResource.class);

  private final SqlStatementFactory sqlStatementFactory;
  private final ServerConfig serverConfig;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;

  @Inject
  public SqlTaskResource(
      final @MSQ SqlStatementFactory sqlStatementFactory,
      final ServerConfig serverConfig,
      final AuthorizerMapper authorizerMapper,
      final ObjectMapper jsonMapper
  )
  {
    this.sqlStatementFactory = sqlStatementFactory;
    this.serverConfig = serverConfig;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
  }

  /**
   * API that allows callers to check if this resource is installed without actually issuing a query. If installed,
   * this call returns 200 OK. If not installed, callers get 404 Not Found.
   */
  @GET
  @Path("/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetEnabled(@Context final HttpServletRequest request)
  {
    // All authenticated users are authorized for this API: check an empty resource list.
    final Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        request,
        Collections.emptyList(),
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    return Response.ok(ImmutableMap.of("enabled", true)).build();
  }

  /**
   * Post a query task.
   *
   * Execution uses {@link MSQTaskSqlEngine} to ship the query off to the Overlord as an indexing task using
   * {@link org.apache.druid.msq.indexing.MSQControllerTask}. The task ID is returned immediately to the caller,
   * and execution proceeds asynchronously.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  )
  {
    // Queries run as MSQ tasks look like regular queries, but return the task ID as their only output.
    final HttpStatement stmt = sqlStatementFactory.httpStatement(sqlQuery, req);
    final String sqlQueryId = stmt.sqlQueryId();
    try {
      final DirectStatement.ResultSet plan = stmt.plan();
      final QueryResponse<Object[]> response = plan.run();
      final Sequence<Object[]> sequence = response.getResults();
      final SqlRowTransformer rowTransformer = plan.createRowTransformer();
      final boolean isTaskStruct = MSQTaskSqlEngine.TASK_STRUCT_FIELD_NAMES.equals(rowTransformer.getFieldList());

      if (isTaskStruct) {
        return buildTaskResponse(sequence);
      } else {
        // Used for EXPLAIN
        return buildStandardResponse(sequence, sqlQuery, sqlQueryId, rowTransformer);
      }
    }
    // Kitchen-sinking the errors since they are all unchecked.
    // Just copied from SqlResource.
    catch (QueryCapacityExceededException cap) {
      stmt.reporter().failed(cap);
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap, sqlQueryId);
    }
    catch (QueryUnsupportedException unsupported) {
      stmt.reporter().failed(unsupported);
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported, sqlQueryId);
    }
    catch (QueryTimeoutException timeout) {
      stmt.reporter().failed(timeout);
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout, sqlQueryId);
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      stmt.reporter().failed(e);
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e, sqlQueryId);
    }
    catch (ForbiddenException e) {
      // No request logs for forbidden queries; same as SqlResource
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy()
                                             .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      stmt.reporter().failed(e);
      SqlPlanningException spe = new SqlPlanningException(
          SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage()
      );
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    // Calcite throws a java.lang.AssertionError which is type Error not Exception. Using Throwable catches both.
    catch (Throwable e) {
      stmt.reporter().failed(e);
      log.noStackTrace().warn(e, "Failed to handle query: %s", sqlQueryId);

      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e),
          sqlQueryId
      );
    }
    finally {
      stmt.close();
    }
  }

  /**
   * Generates a task response using {@link SqlTaskStatus}.
   */
  private Response buildTaskResponse(Sequence<Object[]> sequence) throws IOException
  {
    Yielder<Object[]> yielder = Yielders.each(sequence);
    String taskId = null;

    try {
      while (!yielder.isDone()) {
        final Object[] row = yielder.get();
        if (taskId == null && row.length > 0) {
          taskId = (String) row[0];
        }
        yielder = yielder.next(null);
      }
    }
    catch (Throwable e) {
      try {
        yielder.close();
      }
      catch (Throwable e2) {
        e.addSuppressed(e2);
      }

      throw e;
    }

    yielder.close();

    if (taskId == null) {
      // Note: no ID to include in error: that is the problem we're reporting.
      return genericError(
          Response.Status.INTERNAL_SERVER_ERROR,
          "Internal error",
          "Failed to issue query task",
          null
      );
    }

    return Response
        .status(Response.Status.ACCEPTED)
        .entity(new SqlTaskStatus(taskId, TaskState.RUNNING, null))
        .build();
  }

  private Response buildStandardResponse(
      Sequence<Object[]> sequence,
      SqlQuery sqlQuery,
      String sqlQueryId,
      SqlRowTransformer rowTransformer
  ) throws IOException
  {
    final Yielder<Object[]> yielder0 = Yielders.each(sequence);

    try {
      final Response.ResponseBuilder responseBuilder = Response
          .ok(
              (StreamingOutput) outputStream -> {
                CountingOutputStream os = new CountingOutputStream(outputStream);
                Yielder<Object[]> yielder = yielder0;

                try (final ResultFormat.Writer writer = sqlQuery.getResultFormat()
                                                                .createFormatter(os, jsonMapper)) {
                  writer.writeResponseStart();

                  if (sqlQuery.includeHeader()) {
                    writer.writeHeader(
                        rowTransformer.getRowType(),
                        sqlQuery.includeTypesHeader(),
                        sqlQuery.includeSqlTypesHeader()
                    );
                  }

                  while (!yielder.isDone()) {
                    final Object[] row = yielder.get();
                    writer.writeRowStart();
                    for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
                      final Object value = rowTransformer.transform(row, i);
                      writer.writeRowField(rowTransformer.getFieldList().get(i), value);
                    }
                    writer.writeRowEnd();
                    yielder = yielder.next(null);
                  }

                  writer.writeResponseEnd();
                }
                catch (Exception e) {
                  log.error(e, "Unable to send SQL response [%s]", sqlQueryId);
                  throw new RuntimeException(e);
                }
                finally {
                  yielder.close();
                }
              }
          );

      if (sqlQuery.includeHeader()) {
        responseBuilder.header(SqlResource.SQL_HEADER_RESPONSE_HEADER, SqlResource.SQL_HEADER_VALUE);
      }

      return responseBuilder.build();
    }
    catch (Throwable e) {
      // make sure to close yielder if anything happened before starting to serialize the response.
      yielder0.close();
      throw e;
    }
  }

  private Response buildNonOkResponse(int status, SanitizableException e, String sqlQueryId)
  {
    QueryException cleaned = (QueryException) serverConfig
        .getErrorResponseTransformStrategy()
        .transformIfNeeded(e);
    return Response
        .status(status)
        .entity(new SqlTaskStatus(sqlQueryId, TaskState.FAILED, cleaned))
        .build();
  }

  private Response genericError(Response.Status status, String code, String msg, String id)
  {
    return Response
        .status(status)
        .entity(new SqlTaskStatus(id, TaskState.FAILED, new QueryException("FAILED", msg, null, null)))
        .build();
  }
}
