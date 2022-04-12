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
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlRowTransformer;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
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
import javax.ws.rs.core.StreamingOutput;

import java.io.IOException;
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
  private final SqlLifecycleFactory sqlLifecycleFactory;
  private final SqlLifecycleManager sqlLifecycleManager;
  private final ServerConfig serverConfig;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper,
      SqlLifecycleFactory sqlLifecycleFactory,
      SqlLifecycleManager sqlLifecycleManager,
      ServerConfig serverConfig
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.sqlLifecycleFactory = Preconditions.checkNotNull(sqlLifecycleFactory, "sqlLifecycleFactory");
    this.sqlLifecycleManager = Preconditions.checkNotNull(sqlLifecycleManager, "sqlLifecycleManager");
    this.serverConfig = serverConfig;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
    final String remoteAddr = req.getRemoteAddr();
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));

      lifecycle.setParameters(sqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      // must add after lifecycle is authorized
      sqlLifecycleManager.add(sqlQueryId, lifecycle);

      lifecycle.plan();

      final SqlRowTransformer rowTransformer = lifecycle.createRowTransformer();
      final Sequence<Object[]> sequence = lifecycle.execute();
      final Yielder<Object[]> yielder0 = Yielders.each(sequence);

      try {
        final Response.ResponseBuilder responseBuilder = Response
            .ok(
                (StreamingOutput) outputStream -> {
                  Exception e = null;
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
                  catch (Exception ex) {
                    e = ex;
                    log.error(ex, "Unable to send SQL response [%s]", sqlQueryId);
                    throw new RuntimeException(ex);
                  }
                  finally {
                    yielder.close();
                    endLifecycle(sqlQueryId, lifecycle, e, remoteAddr, os.getCount());
                  }
                }
            )
            .header(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId);

        if (sqlQuery.includeHeader()) {
          responseBuilder.header(SQL_HEADER_RESPONSE_HEADER, SQL_HEADER_VALUE);
        }

        return responseBuilder.build();
      }
      catch (Throwable e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder0.close();
        throw new RuntimeException(e);
      }
    }
    catch (QueryCapacityExceededException cap) {
      endLifecycle(sqlQueryId, lifecycle, cap, remoteAddr, -1);
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap, sqlQueryId);
    }
    catch (QueryUnsupportedException unsupported) {
      endLifecycle(sqlQueryId, lifecycle, unsupported, remoteAddr, -1);
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported, sqlQueryId);
    }
    catch (QueryTimeoutException timeout) {
      endLifecycle(sqlQueryId, lifecycle, timeout, remoteAddr, -1);
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout, sqlQueryId);
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      endLifecycle(sqlQueryId, lifecycle, e, remoteAddr, -1);
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e, sqlQueryId);
    }
    catch (ForbiddenException e) {
      endLifecycleWithoutEmittingMetrics(sqlQueryId, lifecycle);
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy()
                                             .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      endLifecycle(sqlQueryId, lifecycle, e, remoteAddr, -1);
      SqlPlanningException spe = new SqlPlanningException(SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage());
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    // calcite throws a java.lang.AssertionError which is type error not exception. using throwable will catch all
    catch (Throwable e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);
      endLifecycle(sqlQueryId, lifecycle, e, remoteAddr, -1);

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
      String sqlQueryId,
      SqlLifecycle lifecycle
  )
  {
    sqlLifecycleManager.remove(sqlQueryId, lifecycle);
  }

  private void endLifecycle(
      String sqlQueryId,
      SqlLifecycle lifecycle,
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    lifecycle.finalizeStateAndEmitLogsAndMetrics(e, remoteAddress, bytesWritten);
    sqlLifecycleManager.remove(sqlQueryId, lifecycle);
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

    List<SqlLifecycle> lifecycles = sqlLifecycleManager.getAll(sqlQueryId);
    if (lifecycles.isEmpty()) {
      return Response.status(Status.NOT_FOUND).build();
    }
    Set<ResourceAction> resources = lifecycles
        .stream()
        .flatMap(lifecycle -> lifecycle.getRequiredResourceActions().stream())
        .collect(Collectors.toSet());
    Access access = AuthorizationUtils.authorizeAllResourceActions(
        req,
        resources,
        authorizerMapper
    );

    if (access.isAllowed()) {
      // should remove only the lifecycles in the snapshot.
      sqlLifecycleManager.removeAll(sqlQueryId, lifecycles);
      lifecycles.forEach(SqlLifecycle::cancel);
      return Response.status(Status.ACCEPTED).build();
    } else {
      return Response.status(Status.FORBIDDEN).build();
    }
  }
}
