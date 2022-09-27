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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryResource;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlExecutionReporter;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlLifecycleManager.Cancelable;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
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
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final HttpStatement stmt = sqlStatementFactory.httpStatement(sqlQuery, req);
    final String sqlQueryId = stmt.sqlQueryId();
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));
      ResultSet resultSet = stmt.plan();
      final QueryResponse<Object[]> response = resultSet.run();
      final SqlRowTransformer rowTransformer = resultSet.createRowTransformer();
      final Yielder<Object[]> finalYielder = Yielders.each(response.getResults());

      final Response.ResponseBuilder responseBuilder = Response
          .ok(
              new StreamingOutput()
              {
                @Override
                public void write(OutputStream output) throws IOException, WebApplicationException
                {
                  Exception e = null;
                  CountingOutputStream os = new CountingOutputStream(output);
                  Yielder<Object[]> yielder = finalYielder;

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
                  catch (Exception ex) {
                    e = ex;
                    log.error(ex, "Unable to send SQL response [%s]", sqlQueryId);
                    throw new RuntimeException(ex);
                  }
                  finally {
                    yielder.close();
                    endLifecycle(stmt, e, os.getCount());
                  }
                }
              }
          )
          .header(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId);

      if (sqlQuery.includeHeader()) {
        responseBuilder.header(SQL_HEADER_RESPONSE_HEADER, SQL_HEADER_VALUE);
      }

      QueryResource.attachResponseContextToHttpResponse(
          sqlQueryId,
          response.getResponseContext(),
          responseBuilder,
          jsonMapper,
          responseContextConfig,
          selfNode
      );

      return responseBuilder.build();
    }
    catch (QueryCapacityExceededException cap) {
      endLifecycle(stmt, cap, -1);
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap, sqlQueryId);
    }
    catch (QueryUnsupportedException unsupported) {
      endLifecycle(stmt, unsupported, -1);
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported, sqlQueryId);
    }
    catch (QueryTimeoutException timeout) {
      endLifecycle(stmt, timeout, -1);
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout, sqlQueryId);
    }
    catch (BadQueryException e) {
      endLifecycle(stmt, e, -1);
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e, sqlQueryId);
    }
    catch (ForbiddenException e) {
      endLifecycleWithoutEmittingMetrics(stmt);
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy()
                                             .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      endLifecycle(stmt, e, -1);
      SqlPlanningException spe = new SqlPlanningException(SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage());
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    // Calcite throws a java.lang.AssertionError which is type error not exception.
    // Using throwable will catch all.
    catch (Throwable e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);
      endLifecycle(stmt, e, -1);

      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e),
          sqlQueryId
      );
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  private void endLifecycleWithoutEmittingMetrics(
      HttpStatement stmt
  )
  {
    sqlLifecycleManager.remove(stmt.sqlQueryId(), stmt);
    stmt.closeQuietly();
  }

  private void endLifecycle(
      HttpStatement stmt,
      @Nullable final Throwable e,
      final long bytesWritten
  )
  {
    SqlExecutionReporter reporter = stmt.reporter();
    if (e == null) {
      reporter.succeeded(bytesWritten);
    } else {
      reporter.failed(e);
    }
    sqlLifecycleManager.remove(stmt.sqlQueryId(), stmt);
    stmt.close();
  }

  private Response buildNonOkResponse(int status, SanitizableException e, String sqlQueryId)
      throws JsonProcessingException
  {
    return Response.status(status)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(
                       jsonMapper.writeValueAsBytes(
                           serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(e)
                       )
                   )
                   .header(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId)
                   .build();
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
}
