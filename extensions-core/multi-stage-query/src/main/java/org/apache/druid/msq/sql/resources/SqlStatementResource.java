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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.QueryExceptionCompat;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
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
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Path("/druid/v2/sql/statements/")
public class SqlStatementResource
{

  private static final Logger log = new Logger(SqlStatementResource.class);
  private final SqlStatementFactory msqSqlStatementFactory;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;
  private final OverlordClient overlordClient;


  @Inject
  public SqlStatementResource(
      final @MSQ SqlStatementFactory msqSqlStatementFactory,
      final AuthorizerMapper authorizerMapper,
      final ObjectMapper jsonMapper,
      final OverlordClient overlordClient
  )
  {
    this.msqSqlStatementFactory = msqSqlStatementFactory;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
    this.overlordClient = overlordClient;
  }


  @GET
  @Path("/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public Response isEnabled(@Context final HttpServletRequest request)
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

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(final SqlQuery sqlQuery, @Context final HttpServletRequest req)
  {
    final HttpStatement stmt = msqSqlStatementFactory.httpStatement(sqlQuery, req);
    final String sqlQueryId = stmt.sqlQueryId();
    final String currThreadName = Thread.currentThread().getName();
    try {
      ExecutionMode executionMode = QueryContexts.getAsEnum(
          QueryContexts.CTX_EXECUTION_MODE,
          sqlQuery.getContext().get(QueryContexts.CTX_EXECUTION_MODE),
          ExecutionMode.class
      );
      if (ExecutionMode.ASYNC != executionMode) {
        return buildNonOkResponse(
            DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              StringUtils.format(
                                  "The statement sql api only supports sync mode[%s]. Please set context parameter [%s=%s] in the context payload",
                                  ExecutionMode.ASYNC,
                                  QueryContexts.CTX_EXECUTION_MODE,
                                  ExecutionMode.ASYNC
                              )
                          )
        );
      }


      Thread.currentThread().setName(StringUtils.format("statement_sql[%s]", sqlQueryId));

      final DirectStatement.ResultSet plan = stmt.plan();
      // in case the engine is async, the query is not run yet. We just return the taskID in case of non explain queries.
      final QueryResponse<Object[]> response = plan.run();
      final Sequence<Object[]> sequence = response.getResults();
      final SqlRowTransformer rowTransformer = plan.createRowTransformer();

      final boolean isTaskStruct = MSQTaskSqlEngine.TASK_STRUCT_FIELD_NAMES.equals(rowTransformer.getFieldList());

      if (isTaskStruct) {
        return buildTaskResponse(sequence, stmt.query().authResult().getIdentity());
      } else {
        // Used for EXPLAIN
        return buildStandardResponse(sequence, sqlQuery, sqlQueryId, rowTransformer);
      }
    }
    catch (DruidException e) {
      stmt.reporter().failed(e);
      return buildNonOkResponse(e);
    }
    catch (QueryException queryException) {
      stmt.reporter().failed(queryException);
      final DruidException underlyingException = DruidException.fromFailure(new QueryExceptionCompat(queryException));
      return buildNonOkResponse(underlyingException);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(
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
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.UNCATEGORIZED)
                        .build(e.getMessage())
      );
    }
    finally {
      stmt.close();
      Thread.currentThread().setName(currThreadName);
    }
  }


  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetStatus(
      @PathParam("id") final String queryId, @Context final HttpServletRequest req
  )
  {
    try {
      Access authResult = AuthorizationUtils.authorizeAllResourceActions(
          req,
          Collections.emptyList(),
          authorizerMapper
      );
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      Optional<SqlStatementResult> sqlStatementResult = getStatementStatus(
          queryId,
          authenticationResult.getIdentity(),
          true
      );

      if (sqlStatementResult.isPresent()) {
        return Response.ok().entity(sqlStatementResult.get()).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.FORBIDDEN)
                        .build(Access.DEFAULT_ERROR_MESSAGE)
      );
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Failed to handle query: %s", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query: [%s]", queryId));
    }
  }

  @GET
  @Path("/{id}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetResults(
      @PathParam("id") final String queryId,
      @QueryParam("page") Long page,
      @Context final HttpServletRequest req
  )
  {
    try {
      Access authResult = AuthorizationUtils.authorizeAllResourceActions(
          req,
          Collections.emptyList(),
          authorizerMapper
      );
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      if (page != null && page < 0) {
        return buildNonOkResponse(
            DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Page cannot be negative. Please pass a positive number."
                          )
        );
      }

      TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId));
      if (taskResponse == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }

      TaskStatusPlus statusPlus = taskResponse.getStatus();
      if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }

      MSQControllerTask msqControllerTask = getMSQControllerTaskOrThrow(queryId, authenticationResult.getIdentity());
      SqlStatementState sqlStatementState = SqlStatementResourceHelper.getSqlStatementState(statusPlus);

      if (sqlStatementState == SqlStatementState.RUNNING || sqlStatementState == SqlStatementState.ACCEPTED) {
        return buildNonOkResponse(
            DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] is currently in [%s] state. Please wait for it to complete.",
                              queryId,
                              sqlStatementState
                          )
        );
      } else if (sqlStatementState == SqlStatementState.FAILED) {
        return buildNonOkResponse(
            DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] failed. Hit status api for more details.",
                              queryId
                          )
        );
      } else {
        Optional<List<ColumnNameAndTypes>> signature = SqlStatementResourceHelper.getSignature(msqControllerTask);
        if (!signature.isPresent()) {
          return Response.ok().build();
        }

        if (page != null && page > 0) {
          // Results from task report are only present as one page.
          return buildNonOkResponse(
              DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build("Page number is out of range of the results.")
          );
        }

        Optional<List<Object>> results = SqlStatementResourceHelper.getResults(
            SqlStatementResourceHelper.getPayload(
                contactOverlord(overlordClient.taskReportAsMap(queryId))
            )
        );

        return Response.ok((StreamingOutput) outputStream -> {
          CountingOutputStream os = new CountingOutputStream(outputStream);

          try (final ResultFormat.Writer writer = ResultFormat.OBJECT.createFormatter(os, jsonMapper)) {
            List<ColumnNameAndTypes> rowSignature = signature.get();
            writer.writeResponseStart();

            for (long k = 0; k < results.get().size(); k++) {
              writer.writeRowStart();
              for (int i = 0; i < rowSignature.size(); i++) {
                writer.writeRowField(
                    rowSignature.get(i).getColName(),
                    ((List) results.get().get(Math.toIntExact(k))).get(i)
                );
              }
              writer.writeRowEnd();
            }

            writer.writeResponseEnd();
          }
          catch (Exception e) {
            log.error(e, "Unable to stream results back for query[%s]", queryId);
            throw new ISE(e, "Unable to stream results back for query[%s]", queryId);
          }
        }).build();

      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.FORBIDDEN)
                        .build(Access.DEFAULT_ERROR_MESSAGE)
      );
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Failed to handle query: %s", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query: [%s]", queryId));
    }
  }

  /**
   * Queries can be canceled while in any {@link SqlStatementState}. Canceling a query that has already completed will be a no-op.
   *
   * @param queryId queryId
   * @param req     httpServletRequest
   * @return HTTP 404 if the query ID does not exist,expired or originated by different user. HTTP 202 if the deletion
   * request has been accepted.
   */
  @DELETE
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response deleteQuery(@PathParam("id") final String queryId, @Context final HttpServletRequest req)
  {

    try {
      Access authResult = AuthorizationUtils.authorizeAllResourceActions(
          req,
          Collections.emptyList(),
          authorizerMapper
      );
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      Optional<SqlStatementResult> sqlStatementResult = getStatementStatus(
          queryId,
          authenticationResult.getIdentity(),
          false
      );
      if (sqlStatementResult.isPresent()) {
        switch (sqlStatementResult.get().getState()) {
          case ACCEPTED:
          case RUNNING:
            overlordClient.cancelTask(queryId);
            return Response.status(Response.Status.ACCEPTED).build();
          case SUCCESS:
          case FAILED:
            // we would also want to clean up the results in the future.
            return Response.ok().build();
          default:
            throw new ISE("Illegal State[%s] encountered", sqlStatementResult.get().getState());
        }

      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.FORBIDDEN)
                        .build(Access.DEFAULT_ERROR_MESSAGE)
      );
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Failed to handle query: %s", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query: [%s]", queryId));
    }
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
      final Response.ResponseBuilder responseBuilder = Response.ok((StreamingOutput) outputStream -> {
        CountingOutputStream os = new CountingOutputStream(outputStream);
        Yielder<Object[]> yielder = yielder0;

        try (final ResultFormat.Writer writer = sqlQuery.getResultFormat().createFormatter(os, jsonMapper)) {
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
      });

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

  private Response buildTaskResponse(Sequence<Object[]> sequence, String user)
  {
    List<Object[]> rows = sequence.toList();
    int numRows = rows.size();
    if (numRows != 1) {
      throw new RE("Expected a single row but got [%d] rows. Please check broker logs for more information.", numRows);
    }
    Object[] firstRow = rows.get(0);
    if (firstRow == null || firstRow.length != 1) {
      throw new RE(
          "Expected a single column but got [%s] columns. Please check broker logs for more information.",
          firstRow == null ? 0 : firstRow.length
      );
    }
    String taskId = String.valueOf(firstRow[0]);

    Optional<SqlStatementResult> statementResult = getStatementStatus(taskId, user, true);

    if (statementResult.isPresent()) {
      return Response.status(Response.Status.OK).entity(statementResult.get()).build();
    } else {
      return buildNonOkResponse(
          DruidException.forPersona(DruidException.Persona.DEVELOPER)
                        .ofCategory(DruidException.Category.DEFENSIVE).build(
                            "Unable to find associated task for query id [%s]. Contact cluster admin to check overlord logs for [%s]",
                            taskId,
                            taskId
                        )
      );
    }
  }

  private Response buildNonOkResponse(DruidException exception)
  {
    return Response
        .status(exception.getStatusCode())
        .entity(new ErrorResponse(exception))
        .build();
  }

  private Optional<ResultSetInformation> getSampleResults(
      String asyncResultId,
      boolean isSelectQuery,
      String dataSource,
      SqlStatementState sqlStatementState
  )
  {
    if (sqlStatementState == SqlStatementState.SUCCESS) {
      Map<String, Object> payload = SqlStatementResourceHelper.getPayload(contactOverlord(overlordClient.taskReportAsMap(
          asyncResultId)));
      Optional<Pair<Long, Long>> rowsAndSize = SqlStatementResourceHelper.getRowsAndSizeFromPayload(
          payload,
          isSelectQuery
      );
      return Optional.of(new ResultSetInformation(
          rowsAndSize.orElse(new Pair<>(null, null)).lhs,
          rowsAndSize.orElse(new Pair<>(null, null)).rhs,
          null,
          dataSource,
          // only populate sample results in case a select query is successful
          isSelectQuery ? SqlStatementResourceHelper.getResults(payload).orElse(null) : null,
          ImmutableList.of(
              new PageInformation(
                  rowsAndSize.orElse(new Pair<>(null, null)).lhs,
                  rowsAndSize.orElse(new Pair<>(null, null)).rhs,
                  0
              )
          )
      ));
    } else {
      return Optional.empty();
    }
  }


  private Optional<SqlStatementResult> getStatementStatus(String queryId, String currentUser, boolean withResults)
      throws DruidException
  {
    TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId));
    if (taskResponse == null) {
      return Optional.empty();
    }

    TaskStatusPlus statusPlus = taskResponse.getStatus();
    if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
      return Optional.empty();
    }

    // since we need the controller payload for auth checks.
    MSQControllerTask msqControllerTask = getMSQControllerTaskOrThrow(queryId, currentUser);
    SqlStatementState sqlStatementState = SqlStatementResourceHelper.getSqlStatementState(statusPlus);

    if (SqlStatementState.FAILED == sqlStatementState) {
      return SqlStatementResourceHelper.getExceptionPayload(
          queryId,
          taskResponse,
          statusPlus,
          sqlStatementState,
          contactOverlord(overlordClient.taskReportAsMap(
              queryId))
      );
    } else {
      Optional<List<ColumnNameAndTypes>> signature = SqlStatementResourceHelper.getSignature(msqControllerTask);
      return Optional.of(new SqlStatementResult(
          queryId,
          sqlStatementState,
          taskResponse.getStatus().getCreatedTime(),
          signature.orElse(null),
          taskResponse.getStatus().getDuration(),
          withResults ? getSampleResults(
              queryId,
              signature.isPresent(),
              msqControllerTask.getDataSource(),
              sqlStatementState
          ).orElse(null) : null,
          null
      ));
    }
  }


  private MSQControllerTask getMSQControllerTaskOrThrow(String queryId, String currentUser)
  {
    TaskPayloadResponse taskPayloadResponse = contactOverlord(overlordClient.taskPayload(queryId));
    SqlStatementResourceHelper.isMSQPayload(taskPayloadResponse, queryId);

    MSQControllerTask msqControllerTask = (MSQControllerTask) taskPayloadResponse.getPayload();
    String queryUser = String.valueOf(msqControllerTask.getQuerySpec()
                                                       .getQuery()
                                                       .getContext()
                                                       .get(MSQTaskQueryMaker.USER_KEY));
    if (currentUser == null || !currentUser.equals(queryUser)) {
      throw new ForbiddenException(StringUtils.format(
          "The current user[%s] cannot view query id[%s] since the query is owned by user[%s]",
          currentUser,
          queryId,
          queryUser
      ));
    }
    return msqControllerTask;
  }

  private <T> T contactOverlord(final ListenableFuture<T> future)
  {
    try {
      return FutureUtils.getUnchecked(future, true);
    }
    catch (RuntimeException e) {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build("Unable to contact overlord " + e.getMessage());
    }
  }
}
