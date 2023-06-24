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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
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
import org.apache.druid.msq.indexing.TaskReportMSQDestination;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;
import org.apache.http.HttpStatus;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;


@Path("/druid/v2/sql/statements/")
public class SqlStatementResource
{

  private static final Logger log = new Logger(SqlStatementResource.class);
  private final SqlStatementFactory msqSqlStatementFactory;
  private final ServerConfig serverConfig;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;
  private final OverlordClient overlordClient;


  @Inject
  public SqlStatementResource(
      final @MSQ SqlStatementFactory msqSqlStatementFactory,
      final ServerConfig serverConfig,
      final AuthorizerMapper authorizerMapper,
      final ObjectMapper jsonMapper,
      final OverlordClient overlordClient
  )
  {
    this.msqSqlStatementFactory = msqSqlStatementFactory;
    this.serverConfig = serverConfig;
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
            HttpStatus.SC_UNPROCESSABLE_ENTITY,
            new QueryException(
                QueryException.UNSUPPORTED_OPERATION_ERROR_CODE,
                StringUtils.format(
                    "The statement sql api only supports sync mode[%s]. Please set context parameter [%s=%s] in the payload",
                    ExecutionMode.ASYNC,
                    QueryContexts.CTX_EXECUTION_MODE,
                    ExecutionMode.ASYNC
                ),
                null,
                null,
                null
            ),
            stmt.sqlQueryId()
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
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e),
          sqlQueryId
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
      AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), authorizerMapper);
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
    catch (ForbiddenException e) {
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(e);
    }
    catch (QueryException e) {
      return buildNonOkResponse(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e, queryId);
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Failed to handle query: %s", queryId);
      throw e;
    }
  }

  @GET
  @Path("/{id}/results")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetResults(
      @PathParam("id") final String queryId,
      @QueryParam("offset") Long offset,
      @QueryParam("numRows") Long numberOfRows,
      @Context final HttpServletRequest req
  )
  {
    if (offset != null && offset < 0) {
      return buildNonOkResponse(
          Response.Status.PRECONDITION_FAILED.getStatusCode(),
          new QueryException(
              null,
              "offset cannot be negative. Please pass a positive number.",
              null,
              null,
              null
          ),
          queryId
      );
    }
    if (numberOfRows != null && numberOfRows < 0) {
      return buildNonOkResponse(
          Response.Status.PRECONDITION_FAILED.getStatusCode(),
          new QueryException(
              null,
              "numRows cannot be negative. Please pass a positive number.",
              null,
              null,
              null
          ),
          queryId
      );
    }

    final long start = offset == null ? 0 : offset;
    final long last = getLastIndex(numberOfRows, start);

    try {
      AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), authorizerMapper);
      final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

      TaskStatusResponse taskResponse = overlordWork(overlordClient.taskStatus(queryId));
      if (taskResponse == null) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }

      TaskStatusPlus statusPlus = taskResponse.getStatus();
      if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      SqlStatementState sqlStatementState = getSqlStatementState(statusPlus);

      if (sqlStatementState == SqlStatementState.RUNNING || sqlStatementState == SqlStatementState.ACCEPTED) {
        return buildNonOkResponse(Response.Status.NOT_FOUND.getStatusCode(), new QueryException(
            null,
            StringUtils.format(
                "Query is [%s]. Please wait for it to complete.",
                sqlStatementState
            ),
            null,
            null,
            null
        ), queryId);
      } else if (sqlStatementState == SqlStatementState.FAILED) {
        return buildNonOkResponse(
            Response.Status.NOT_FOUND.getStatusCode(),
            new QueryException(null, statusPlus.getErrorMsg(), null, null, null),
            queryId
        );
      } else {
        MSQControllerTask msqControllerTask = getMSQControllerTaskOrThrow(queryId, authenticationResult.getIdentity());
        Optional<List<ColumnNameAndTypes>> signature = getSignature(msqControllerTask);
        if (!signature.isPresent()) {
          return Response.ok().build();
        }
        Optional<List<Object>> results = getResults(getPayload(overlordWork(overlordClient.taskReportAsMap(queryId))));

        return Response.ok((StreamingOutput) outputStream -> {
          CountingOutputStream os = new CountingOutputStream(outputStream);

          try (final ResultFormat.Writer writer = ResultFormat.OBJECT.createFormatter(os, jsonMapper)) {
            List<ColumnNameAndTypes> rowSignature = signature.get();
            writer.writeResponseStart();

            for (long k = start; k < Math.min(last, results.get().size()); k++) {
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
    catch (ForbiddenException e) {
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(e);
    }
    catch (QueryException e) {
      return buildNonOkResponse(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e, queryId);
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Failed to handle query: %s", queryId);
      throw e;
    }
  }

  /**
   * Queries can be canceled while in anystate. Canceling a query that has already completed will remove its results.
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
      AuthorizationUtils.authorizeAllResourceActions(req, Collections.emptyList(), authorizerMapper);
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
            // we would also want to clean up the
            return Response.ok().build();
          default:
            throw new ISE("Illegal State[%s] encountered", sqlStatementResult.get().getState());
        }

      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }


    }
    catch (ForbiddenException e) {
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(e);
    }
    catch (QueryException e) {
      return buildNonOkResponse(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), e, queryId);
    }
    catch (Exception e) {
      log.noStackTrace().warn(e, "Failed to handle query: %s", queryId);
      throw e;
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
    String taskId = (String) rows.get(0)[0];
    try {
      Optional<SqlStatementResult> statementResult = getStatementStatus(taskId, user, true);

      if (statementResult.isPresent()) {
        return Response.status(Response.Status.OK).entity(statementResult.get()).build();
      } else {
        return buildNonOkResponse(
            Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
            new QueryException(
                null,
                StringUtils.format(
                    "Unable to find associated task for query id %s",
                    taskId
                ),
                null,
                null,
                null
            ),
            taskId
        );
      }
    }
    catch (QueryException e) {
      return buildNonOkResponse(Response.Status.OK.getStatusCode(), e, taskId);
    }
  }

  private Response buildNonOkResponse(int status, SanitizableException e, String sqlQueryId)
  {
    // Though transformIfNeeded returns an exception, its purpose is to return
    // a QueryException
    Exception cleaned = serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(e);
    return Response.status(status).entity(cleaned).build();
  }


  private static Optional<List<ColumnNameAndTypes>> getSignature(
      MSQControllerTask msqControllerTask
  )
  {
    // only populate signature for select q's
    if (msqControllerTask.getQuerySpec().getDestination().getClass() == TaskReportMSQDestination.class) {
      ColumnMappings columnMappings = msqControllerTask.getQuerySpec().getColumnMappings();
      List<SqlTypeName> sqlTypeNames = msqControllerTask.getSqlTypeNames();
      if (sqlTypeNames == null || sqlTypeNames.size() != columnMappings.size()) {
        return Optional.empty();
      }
      List<ColumnType> nativeTypeNames = msqControllerTask.getNativeTypeNames();
      if (nativeTypeNames == null || nativeTypeNames.size() != columnMappings.size()) {
        return Optional.empty();
      }
      List<ColumnNameAndTypes> signature = new ArrayList<>(columnMappings.size());
      int index = 0;
      for (String colName : columnMappings.getOutputColumnNames()) {
        signature.add(new ColumnNameAndTypes(
            colName,
            sqlTypeNames.get(index).getName(),
            nativeTypeNames.get(index).asTypeString()
        ));
        index++;
      }
      return Optional.of(signature);
    }
    return Optional.empty();
  }


  private void checkTaskPayloadOrThrow(TaskPayloadResponse taskPayloadResponse, String queryId) throws QueryException
  {
    if (taskPayloadResponse == null || taskPayloadResponse.getPayload() == null) {
      throw new QueryException(
          QueryException.UNKNOWN_EXCEPTION_ERROR_CODE,
          StringUtils.format(
              "Could not get payload details for query[%s] from the overlord",
              queryId
          ),
          null,
          null,
          null
      );
    }

    if (MSQControllerTask.class != taskPayloadResponse.getPayload().getClass()) {
      throw new QueryException(
          QueryException.UNKNOWN_EXCEPTION_ERROR_CODE,
          StringUtils.format(
              "Fetched an unexpected payload for query[%s] from the overlord.",
              queryId
          ),
          null,
          null,
          null
      );
    }
  }

  private Optional<ResultSetInformation> getSampleResults(
      String asyncResultId,
      boolean isSelectQuery,
      String dataSource,
      SqlStatementState sqlStatementState
  )
  {
    if (sqlStatementState == SqlStatementState.SUCCESS) {
      Map<String, Object> payload = getPayload(overlordWork(overlordClient.taskReportAsMap(asyncResultId)));
      Optional<Pair<Long, Long>> rowsAndSize = getRowsAndSizeFromPayload(payload, isSelectQuery);
      return Optional.of(new ResultSetInformation(
          null,
          // since the rows can be sampled, get the number of rows from counters
          rowsAndSize.orElse(new Pair<>(null, null)).lhs,
          rowsAndSize.orElse(new Pair<>(null, null)).rhs,
          dataSource,
          // only populate sample results in case a select query is successful
          isSelectQuery ? getResults(payload).orElse(null) : null
      ));
    } else {
      return Optional.empty();
    }
  }

  private Optional<Pair<Long, Long>> getRowsAndSizeFromPayload(Map<String, Object> payload, boolean isSelectQuery)
  {
    List stages = getList(payload, "stages");
    if (stages == null || stages.isEmpty()) {
      return Optional.empty();
    } else {
      int maxStage = stages.size() - 1; // Last stage output is the total number of rows returned to the end user.
      Map<String, Object> counterMap = getMap(getMap(payload, "counters"), String.valueOf(maxStage));
      long rows = -1L;
      long sizeInBytes = -1L;
      if (counterMap == null) {
        return Optional.empty();
      }
      for (Map.Entry<String, Object> worker : counterMap.entrySet()) {
        Object workerChannels = worker.getValue();
        if (workerChannels == null || !(workerChannels instanceof Map)) {
          return Optional.empty();
        }
        if (isSelectQuery) {
          Object output = ((Map<?, ?>) workerChannels).get("output");
          if (output != null && output instanceof Map) {
            List<Integer> rowsPerChannel = (List<Integer>) ((Map<String, Object>) output).get("rows");
            List<Integer> bytesPerChannel = (List<Integer>) ((Map<String, Object>) output).get("bytes");
            for (Integer row : rowsPerChannel) {
              rows = rows + row;
            }
            for (Integer bytes : bytesPerChannel) {
              sizeInBytes = sizeInBytes + bytes;
            }
          }
        } else {
          Object output = ((Map<?, ?>) workerChannels).get("segmentGenerationProgress");
          if (output != null && output instanceof Map) {
            rows += (Integer) ((Map<String, Object>) output).get("rowsPushed");
          }
        }
      }

      return Optional.of(new Pair<>(rows == -1L ? null : rows + 1, sizeInBytes == -1L ? null : sizeInBytes + 1));
    }
  }


  private Optional<SqlStatementResult> getStatementStatus(String queryId, String currentUser, boolean withResults)
      throws QueryException, ForbiddenException
  {
    TaskStatusResponse taskResponse = overlordWork(overlordClient.taskStatus(queryId));
    if (taskResponse == null) {
      return Optional.empty();
    }

    TaskStatusPlus statusPlus = taskResponse.getStatus();
    if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
      return Optional.empty();
    }

    // since we need the controller payload for auth checks.
    MSQControllerTask msqControllerTask = getMSQControllerTaskOrThrow(queryId, currentUser);
    SqlStatementState sqlStatementState = getSqlStatementState(statusPlus);

    if (SqlStatementState.FAILED == sqlStatementState) {
      Map<String, Object> exceptionDetails = getQueryExceptionDetails(getPayload(overlordWork(overlordClient.taskReportAsMap(
          queryId))));
      Map<String, Object> exception = getMap(exceptionDetails, "error");
      if (exceptionDetails == null || exception == null) {
        return Optional.of(new SqlStatementResult(
            queryId,
            sqlStatementState,
            taskResponse.getStatus().getCreatedTime(),
            null,
            taskResponse.getStatus().getDuration(),
            null,
            new QueryException(null, statusPlus.getErrorMsg(), null, null, null)
        ));
      }

      final String errorMessage = String.valueOf(exception.getOrDefault("errorMessage", statusPlus.getErrorMsg()));
      exception.remove("errorMessage");
      String host = (String) exceptionDetails.getOrDefault("host", null);
      Map<String, String> stringException = new HashMap<>();
      for (Map.Entry<String, Object> exceptionKeys : exception.entrySet()) {
        stringException.put(exceptionKeys.getKey(), String.valueOf(exceptionKeys.getValue()));
      }
      return Optional.of(new SqlStatementResult(
          queryId,
          sqlStatementState,
          taskResponse.getStatus().getCreatedTime(),
          null,
          taskResponse.getStatus().getDuration(),
          null,
          new QueryException(null, errorMessage, null, host, stringException)
      ));
    } else {
      Optional<List<ColumnNameAndTypes>> signature = getSignature(msqControllerTask);

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

  private Map<String, Object> getQueryExceptionDetails(Map<String, Object> payload)
  {
    return getMap(getMap(payload, "status"), "errorReport");
  }

  private MSQControllerTask getMSQControllerTaskOrThrow(String queryId, String currentUser) throws ForbiddenException
  {
    TaskPayloadResponse taskPayloadResponse = overlordWork(overlordClient.taskPayload(queryId));
    checkTaskPayloadOrThrow(taskPayloadResponse, queryId);

    MSQControllerTask msqControllerTask = (MSQControllerTask) taskPayloadResponse.getPayload();
    if (currentUser == null || !currentUser.equals(msqControllerTask.getQuerySpec()
                                                                    .getQuery()
                                                                    .getContext()
                                                                    .get(MSQTaskQueryMaker.USER_KEY))) {
      throw new ForbiddenException();
    }
    return msqControllerTask;
  }

  private <T> T overlordWork(final ListenableFuture<T> future)
  {
    try {
      return FutureUtils.getUnchecked(future, true);
    }
    catch (RuntimeException e) {
      throw new QueryException(null, "Unable to contact overlord " + e.getMessage(), null, null, null);
    }
  }

  private static SqlStatementState getSqlStatementState(TaskStatusPlus taskStatusPlus)
  {
    TaskState state = taskStatusPlus.getStatusCode();
    if (state == null) {
      return SqlStatementState.ACCEPTED;
    }

    switch (state) {
      case FAILED:
        return SqlStatementState.FAILED;
      case RUNNING:
        if (TaskLocation.unknown().equals(taskStatusPlus.getLocation())) {
          return SqlStatementState.ACCEPTED;
        } else {
          return SqlStatementState.RUNNING;
        }
      case SUCCESS:
        return SqlStatementState.SUCCESS;
      default:
        throw new ISE("Unrecognized state[%s] found.", state);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> getMap(Map<String, Object> map, String key)
  {
    if (map == null) {
      return null;
    }
    return (Map<String, Object>) map.get(key);
  }

  @SuppressWarnings("rawtypes")
  private List getList(Map<String, Object> map, String key)
  {
    if (map == null) {
      return null;
    }
    return (List) map.get(key);
  }

  /**
   * Get results from report
   */
  @SuppressWarnings("unchecked")
  private Optional<List<Object>> getResults(Map<String, Object> payload)
  {
    Map<String, Object> resultsHolder = getMap(payload, "results");

    if (resultsHolder == null) {
      return Optional.empty();
    }

    List<Object> data = (List<Object>) resultsHolder.get("results");
    List<Object> rows = new ArrayList<>();
    if (data != null) {
      rows.addAll(data);
    }
    return Optional.of(rows);
  }

  private Map<String, Object> getPayload(Map<String, Object> results)
  {
    Map<String, Object> msqReport = getMap(results, "multiStageQuery");
    Map<String, Object> payload = getMap(msqReport, "payload");
    return payload;
  }

  private static long getLastIndex(Long numberOfRows, long start)
  {
    final long last;
    if (numberOfRows == null) {
      last = Long.MAX_VALUE;
    } else {
      long finalIndex;
      try {
        finalIndex = Math.addExact(start, numberOfRows);
      }
      catch (ArithmeticException e) {
        finalIndex = Long.MAX_VALUE;
      }
      last = finalIndex;
    }
    return last;
  }

}
