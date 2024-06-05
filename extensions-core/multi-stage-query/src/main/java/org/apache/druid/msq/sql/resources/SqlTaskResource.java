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

package org.apache.druid.msq.sql.resources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.QueryExceptionCompat;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.sql.SqlTaskStatus;
import org.apache.druid.query.QueryException;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
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
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;

/**
 * Endpoint for SQL execution using MSQ tasks.
 * <p>
 * Unlike the SQL endpoint in {@link SqlResource}, this endpoint returns task IDs instead of inline results. Queries
 * are executed asynchronously using MSQ tasks via the indexing service (Overlord + MM or Indexer). This endpoint
 * does not provide a way for users to get the status or results of a query. That must be done using Overlord APIs
 * for status and reports.
 * <p>
 * One exception: EXPLAIN query results are returned inline by this endpoint, in the same way as {@link SqlResource}
 * would return them.
 * <p>
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
      final @MultiStageQuery SqlStatementFactory sqlStatementFactory,
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
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    return Response.ok(ImmutableMap.of("enabled", true)).build();
  }

  /**
   * Post a query task.
   * <p>
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
    catch (DruidException e) {
      stmt.reporter().failed(e);
      return buildNonOkResponse(sqlQueryId, e);
    }
    catch (QueryException queryException) {
      stmt.reporter().failed(queryException);
      final DruidException underlyingException = DruidException.fromFailure(new QueryExceptionCompat(queryException));
      return buildNonOkResponse(sqlQueryId, underlyingException);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(
          "forbidden",
          DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.FORBIDDEN)
                        .build(Access.DEFAULT_ERROR_MESSAGE)
      );
    }
    // Calcite throws java.lang.AssertionError at various points in planning/validation.
    catch (AssertionError | Exception e) {
      stmt.reporter().failed(e);
      log.noStackTrace().warn(e, "Failed to handle query: %s", sqlQueryId);

      return buildNonOkResponse(
          sqlQueryId,
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build("%s", e.getMessage())
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
      // Note: no ID to include in error: that is the problem we're reporting.  It would be really nice to know
      // why we don't have an ID or more information about why things failed.  Hopefully that gets returned to the
      // user through a DruidExcpetion that makes it out of this code and this code never actually gets executed.
      // Using a defensive exception just to report something with the opes that any time this actually happens, the
      // fix is to make error reporting somewhere that actually understands more about why it failed.
      return buildNonOkResponse(
          null,
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.DEFENSIVE)
                        .build("Failed to issue query task")
      );
    }

    return Response
        .status(Response.Status.ACCEPTED)
        .entity(new SqlTaskStatus(taskId, TaskState.RUNNING, null))
        .build();
  }

  @SuppressWarnings("UnstableApiUsage")
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

  private Response buildNonOkResponse(String sqlQueryId, DruidException exception)
  {
    return Response
        .status(exception.getStatusCode())
        .entity(new SqlTaskStatus(sqlQueryId, TaskState.FAILED, new ErrorResponse(exception)))
        .build();
  }
}
