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
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.Forbidden;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.error.QueryExceptionCompat;
import org.apache.druid.frame.channel.FrameChannelSequence;
import org.apache.druid.guice.annotations.MSQ;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.destination.DurableStorageMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.shuffle.input.DurableStorageInputChannelFactory;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.sql.SqlStatementState;
import org.apache.druid.msq.sql.entity.ColumnNameAndTypes;
import org.apache.druid.msq.sql.entity.PageInformation;
import org.apache.druid.msq.sql.entity.ResultSetInformation;
import org.apache.druid.msq.sql.entity.SqlStatementResult;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.msq.util.SqlStatementResourceHelper;
import org.apache.druid.query.ExecutionMode;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.rpc.HttpResponseException;
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
import org.apache.druid.storage.NilStorageConnector;
import org.apache.druid.storage.StorageConnector;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;


@Path("/druid/v2/sql/statements/")
public class SqlStatementResource
{

  private static final Logger log = new Logger(SqlStatementResource.class);
  private final SqlStatementFactory msqSqlStatementFactory;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper jsonMapper;
  private final OverlordClient overlordClient;
  private final StorageConnector storageConnector;


  @Inject
  public SqlStatementResource(
      final @MSQ SqlStatementFactory msqSqlStatementFactory,
      final AuthorizerMapper authorizerMapper,
      final ObjectMapper jsonMapper,
      final OverlordClient overlordClient,
      final @MultiStageQuery StorageConnector storageConnector
  )
  {
    this.msqSqlStatementFactory = msqSqlStatementFactory;
    this.authorizerMapper = authorizerMapper;
    this.jsonMapper = jsonMapper;
    this.overlordClient = overlordClient;
    this.storageConnector = storageConnector;
  }

  /**
   * API for clients like web-console to check if this resource is enabled.
   */

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
    boolean isDebug = false;
    try {
      QueryContext queryContext = QueryContext.of(sqlQuery.getContext());
      isDebug = queryContext.isDebug();
      contextChecks(queryContext);

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
      return buildNonOkResponse(Forbidden.exception());
    }
    // Calcite throws java.lang.AssertionError at various points in planning/validation.
    catch (AssertionError | Exception e) {
      stmt.reporter().failed(e);
      if (isDebug) {
        log.warn(e, "Failed to handle query [%s]", sqlQueryId);
      } else {
        log.noStackTrace().warn(e, "Failed to handle query [%s]", sqlQueryId);
      }
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
        throw queryNotFoundException(queryId);
      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query [%s]", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query [%s]", queryId));
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
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.INVALID_INPUT)
                            .build(
                                "Page cannot be negative. Please pass a positive number."
                            );
      }

      TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId), queryId);
      if (taskResponse == null) {
        throw queryNotFoundException(queryId);
      }

      TaskStatusPlus statusPlus = taskResponse.getStatus();
      if (statusPlus == null || !MSQControllerTask.TYPE.equals(statusPlus.getType())) {
        throw queryNotFoundException(queryId);
      }

      MSQControllerTask msqControllerTask = getMSQControllerTaskOrThrow(queryId, authenticationResult.getIdentity());
      throwIfQueryIsNotSuccessful(queryId, statusPlus);

      Optional<List<ColumnNameAndTypes>> signature = SqlStatementResourceHelper.getSignature(msqControllerTask);
      if (!signature.isPresent() || MSQControllerTask.isIngestion(msqControllerTask.getQuerySpec())) {
        // Since it's not a select query, nothing to return.
        return Response.ok().build();
      }

      // returning results
      final Closer closer = Closer.create();
      final Optional<Yielder<Object[]>> results;
      results = getResultYielder(queryId, page, msqControllerTask, closer);
      if (!results.isPresent()) {
        // no results, return empty
        return Response.ok().build();
      }

      return Response.ok((StreamingOutput) outputStream -> resultPusher(
          queryId,
          signature,
          closer,
          results,
          new CountingOutputStream(outputStream)
      )).build();
    }


    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query [%s]", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query [%s]", queryId));
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
        throw queryNotFoundException(queryId);
      }
    }
    catch (DruidException e) {
      return buildNonOkResponse(e);
    }
    catch (ForbiddenException e) {
      log.debug("Got forbidden request for reason [%s]", e.getErrorMessage());
      return buildNonOkResponse(Forbidden.exception());
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query [%s]", queryId);
      return buildNonOkResponse(DruidException.forPersona(DruidException.Persona.DEVELOPER)
                                              .ofCategory(DruidException.Category.UNCATEGORIZED)
                                              .build(e, "Failed to handle query [%s]", queryId));
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

  @SuppressWarnings("ReassignedVariable")
  private Optional<ResultSetInformation> getSampleResults(
      String queryId,
      String dataSource,
      SqlStatementState sqlStatementState,
      MSQDestination msqDestination
  )
  {
    if (sqlStatementState == SqlStatementState.SUCCESS) {
      Map<String, Object> payload =
          SqlStatementResourceHelper.getPayload(contactOverlord(
              overlordClient.taskReportAsMap(queryId),
              queryId
          ));
      MSQTaskReportPayload msqTaskReportPayload = jsonMapper.convertValue(payload, MSQTaskReportPayload.class);
      Optional<List<PageInformation>> pageList = SqlStatementResourceHelper.populatePageList(
          msqTaskReportPayload,
          msqDestination
      );

      // getting the total number of rows, size from page information.
      Long rows = null;
      Long size = null;
      if (pageList.isPresent()) {
        rows = 0L;
        size = 0L;
        for (PageInformation pageInformation : pageList.get()) {
          rows += pageInformation.getNumRows() != null ? pageInformation.getNumRows() : 0L;
          size += pageInformation.getSizeInBytes() != null ? pageInformation.getSizeInBytes() : 0L;
        }
      }

      boolean isSelectQuery = msqDestination instanceof TaskReportMSQDestination
                              || msqDestination instanceof DurableStorageMSQDestination;

      List<Object[]> results = null;
      if (isSelectQuery) {
        results = new ArrayList<>();
        Yielder<Object[]> yielder = null;
        if (msqTaskReportPayload.getResults() != null) {
          yielder = msqTaskReportPayload.getResults().getResultYielder();
        }
        try {
          while (yielder != null && !yielder.isDone()) {
            results.add(yielder.get());
            yielder = yielder.next(null);
          }
        }
        finally {
          if (yielder != null) {
            try {
              yielder.close();
            }
            catch (IOException e) {
              log.warn(e, StringUtils.format("Unable to close yielder for query[%s]", queryId));
            }
          }
        }

      }

      return Optional.of(
          new ResultSetInformation(
              rows,
              size,
              null,
              dataSource,
              results,
              isSelectQuery ? pageList.orElse(null) : null
          )
      );
    } else {
      return Optional.empty();
    }
  }


  private Optional<SqlStatementResult> getStatementStatus(String queryId, String currentUser, boolean withResults)
      throws DruidException
  {
    TaskStatusResponse taskResponse = contactOverlord(overlordClient.taskStatus(queryId), queryId);
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
          contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)
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
              msqControllerTask.getDataSource(),
              sqlStatementState,
              msqControllerTask.getQuerySpec().getDestination()
          ).orElse(null) : null,
          null
      ));
    }
  }


  private MSQControllerTask getMSQControllerTaskOrThrow(String queryId, String currentUser)
  {
    TaskPayloadResponse taskPayloadResponse = contactOverlord(overlordClient.taskPayload(queryId), queryId);
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

  private Optional<Yielder<Object[]>> getResultYielder(
      String queryId,
      Long page,
      MSQControllerTask msqControllerTask,
      Closer closer
  )
  {
    final Optional<Yielder<Object[]>> results;

    if (msqControllerTask.getQuerySpec().getDestination() instanceof TaskReportMSQDestination) {
      // Results from task report are only present as one page.
      if (page != null && page > 0) {
        throw InvalidInput.exception(
            "Page number [%d] is out of the range of results", page
        );
      }

      MSQTaskReportPayload msqTaskReportPayload = jsonMapper.convertValue(SqlStatementResourceHelper.getPayload(
          contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)), MSQTaskReportPayload.class);

      if (msqTaskReportPayload.getResults().getResultYielder() == null) {
        results = Optional.empty();
      } else {
        results = Optional.of(msqTaskReportPayload.getResults().getResultYielder());
      }

    } else if (msqControllerTask.getQuerySpec().getDestination() instanceof DurableStorageMSQDestination) {

      MSQTaskReportPayload msqTaskReportPayload = jsonMapper.convertValue(SqlStatementResourceHelper.getPayload(
          contactOverlord(overlordClient.taskReportAsMap(queryId), queryId)), MSQTaskReportPayload.class);

      List<PageInformation> pages =
          SqlStatementResourceHelper.populatePageList(
              msqTaskReportPayload,
              msqControllerTask.getQuerySpec().getDestination()
          ).orElse(null);

      if (pages == null || pages.isEmpty()) {
        return Optional.empty();
      }

      final StageDefinition finalStage = Objects.requireNonNull(SqlStatementResourceHelper.getFinalStage(
          msqTaskReportPayload)).getStageDefinition();

      // get all results
      final Long selectedPageId;
      if (page != null) {
        selectedPageId = getPageInformationForPageId(pages, page).getId();
      } else {
        selectedPageId = null;
      }
      checkForDurableStorageConnectorImpl();
      final DurableStorageInputChannelFactory standardImplementation = DurableStorageInputChannelFactory.createStandardImplementation(
          msqControllerTask.getId(),
          storageConnector,
          closer,
          true
      );
      results = Optional.of(Yielders.each(
          Sequences.concat(pages.stream()
                                .filter(pageInformation -> selectedPageId == null
                                                           || selectedPageId.equals(pageInformation.getId()))
                                .map(pageInformation -> {
                                  try {
                                    return new FrameChannelSequence(standardImplementation.openChannel(
                                        finalStage.getId(),
                                        (int) pageInformation.getId(),
                                        (int) pageInformation.getId()
// we would always have partition number == worker number
                                    ));
                                  }
                                  catch (Exception e) {
                                    throw new RuntimeException(e);
                                  }
                                })
                                .collect(Collectors.toList()))
                   .flatMap(frame -> SqlStatementResourceHelper.getResultSequence(
                                msqControllerTask,
                                finalStage,
                                frame,
                                jsonMapper
                            )
                   )
                   .withBaggage(closer)));

    } else {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build(
                              "MSQ select destination[%s] not supported. Please reach out to druid slack community for more help.",
                              msqControllerTask.getQuerySpec().getDestination().toString()
                          );
    }
    return results;
  }

  private PageInformation getPageInformationForPageId(List<PageInformation> pages, Long pageId)
  {
    for (PageInformation pageInfo : pages) {
      if (pageInfo.getId() == pageId) {
        return pageInfo;
      }
    }
    throw InvalidInput.exception("Invalid page id [%d] passed.", pageId);
  }

  private void resultPusher(
      String queryId,
      Optional<List<ColumnNameAndTypes>> signature,
      Closer closer,
      Optional<Yielder<Object[]>> results,
      CountingOutputStream os
  ) throws IOException
  {
    try {
      try (final ResultFormat.Writer writer = ResultFormat.OBJECTLINES.createFormatter(os, jsonMapper)) {
        Yielder<Object[]> yielder = results.get();
        List<ColumnNameAndTypes> rowSignature = signature.get();
        writer.writeResponseStart();

        while (!yielder.isDone()) {
          writer.writeRowStart();
          Object[] row = yielder.get();
          for (int i = 0; i < Math.min(rowSignature.size(), row.length); i++) {
            writer.writeRowField(
                rowSignature.get(i).getColName(),
                row[i]
            );
          }
          writer.writeRowEnd();
          yielder = yielder.next(null);
        }
        writer.writeResponseEnd();
        yielder.close();
      }
      catch (Exception e) {
        log.error(e, "Unable to stream results back for query[%s]", queryId);
        throw new ISE(e, "Unable to stream results back for query[%s]", queryId);
      }
    }
    catch (Exception e) {
      log.error(e, "Unable to stream results back for query[%s]", queryId);
      throw new ISE(e, "Unable to stream results back for query[%s]", queryId);
    }
    finally {
      closer.close();
    }
  }

  private static void throwIfQueryIsNotSuccessful(String queryId, TaskStatusPlus statusPlus)
  {
    SqlStatementState sqlStatementState = SqlStatementResourceHelper.getSqlStatementState(statusPlus);
    if (sqlStatementState == SqlStatementState.RUNNING || sqlStatementState == SqlStatementState.ACCEPTED) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] is currently in [%s] state. Please wait for it to complete.",
                              queryId,
                              sqlStatementState
                          );
    } else if (sqlStatementState == SqlStatementState.FAILED) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "Query[%s] failed. Check the status api for more details.",
                              queryId
                          );
    } else {
      // do nothing
    }
  }

  private void contextChecks(QueryContext queryContext)
  {
    ExecutionMode executionMode = queryContext.getEnum(QueryContexts.CTX_EXECUTION_MODE, ExecutionMode.class, null);

    if (executionMode == null) {
      throw InvalidInput.exception(
          "Execution mode is not provided to the sql statement api. "
          + "Please set [%s] to [%s] in the query context",
          QueryContexts.CTX_EXECUTION_MODE,
          ExecutionMode.ASYNC
      );
    }

    if (!ExecutionMode.ASYNC.equals(executionMode)) {
      throw InvalidInput.exception(
          "The sql statement api currently does not support the provided execution mode [%s]. "
          + "Please set [%s] to [%s] in the query context",
          executionMode,
          QueryContexts.CTX_EXECUTION_MODE,
          ExecutionMode.ASYNC
      );
    }

    MSQSelectDestination selectDestination = MultiStageQueryContext.getSelectDestination(queryContext);
    if (MSQSelectDestination.DURABLESTORAGE.equals(selectDestination)) {
      checkForDurableStorageConnectorImpl();
    }
  }

  private void checkForDurableStorageConnectorImpl()
  {
    if (storageConnector instanceof NilStorageConnector) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              StringUtils.format(
                                  "The sql statement api cannot read from the select destination [%s] provided "
                                  + "in the query context [%s] since it is not configured on the %s. It is recommended to configure durable storage "
                                  + "as it allows the user to fetch large result sets. Please contact your cluster admin to "
                                  + "configure durable storage.",
                                  MSQSelectDestination.DURABLESTORAGE.getName(),
                                  MultiStageQueryContext.CTX_SELECT_DESTINATION,
                                  NodeRole.BROKER.getJsonName()
                              )
                          );
    }
  }

  private <T> T contactOverlord(final ListenableFuture<T> future, String queryId)
  {
    try {
      return FutureUtils.getUnchecked(future, true);
    }
    catch (RuntimeException e) {
      if (e.getCause() instanceof HttpResponseException) {
        HttpResponseException httpResponseException = (HttpResponseException) e.getCause();
        if (httpResponseException.getResponse() != null && httpResponseException.getResponse().getResponse().getStatus()
                                                                                .equals(HttpResponseStatus.NOT_FOUND)) {
          log.info(httpResponseException, "Query details not found for queryId [%s]", queryId);
          // since we get a 404, we mark the request as a NotFound. This code path is generally triggered when user passes a `queryId` which is not found in the overlord.
          throw queryNotFoundException(queryId);
        }
      }
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.UNCATEGORIZED)
                          .build("Unable to contact overlord " + e.getMessage());
    }
  }

  private static DruidException queryNotFoundException(String queryId)
  {
    return NotFound.exception("Query [%s] was not found. The query details are no longer present or might not be of the type [%s]. Verify that the id is correct.", queryId, MSQControllerTask.TYPE);
  }

}
