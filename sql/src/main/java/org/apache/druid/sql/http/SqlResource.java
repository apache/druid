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
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResponse;
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
    final String sqlQueryId = lifecycle.initialize(sqlQuery);
    final String currThreadName = Thread.currentThread().getName();

    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", sqlQueryId));

      // Initialize the query stats using the original received query.
      SqlLifecycle.LifecycleStats stats = lifecycle.stats();
      stats.onStart(req.getRemoteAddr(), sqlQueryId, sqlQuery);

      lifecycle.setParameters(sqlQuery.getParameterList());
      lifecycle.validateAndAuthorize(req);
      // must add after lifecycle is authorized
      sqlLifecycleManager.add(sqlQueryId, lifecycle);

      lifecycle.plan();

      // The only failure possible in this last line is if the query
      // run within QueryOutput fails. If so, the yielder is never
      // created. Else, if the query starts running, the QueryOutput
      // is returned to Jetty, which will call write() which will ensure
      // that the yielder is closed.
      return Response
          .ok(new QueryOutput(lifecycle))
          .header(SQL_QUERY_ID_RESPONSE_HEADER, sqlQueryId)
          .type(sqlQuery.getResultFormat().contentType())
          .build();
    }
    catch (QueryCapacityExceededException cap) {
      endLifecycle(sqlQueryId, lifecycle, cap);
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap, sqlQueryId);
    }
    catch (QueryUnsupportedException unsupported) {
      endLifecycle(sqlQueryId, lifecycle, unsupported);
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported, sqlQueryId);
    }
    catch (QueryTimeoutException timeout) {
      endLifecycle(sqlQueryId, lifecycle, timeout);
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout, sqlQueryId);
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      endLifecycle(sqlQueryId, lifecycle, e);
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e, sqlQueryId);
    }
    catch (ForbiddenException e) {
      endLifecycleWithoutEmittingMetrics(sqlQueryId, lifecycle);
      throw (ForbiddenException) serverConfig.getErrorResponseTransformStrategy()
                                             .transformIfNeeded(e); // let ForbiddenExceptionMapper handle this
    }
    catch (RelOptPlanner.CannotPlanException e) {
      endLifecycle(sqlQueryId, lifecycle, e);
      SqlPlanningException spe = new SqlPlanningException(SqlPlanningException.PlanningError.UNSUPPORTED_SQL_ERROR,
          e.getMessage());
      return buildNonOkResponse(BadQueryException.STATUS_CODE, spe, sqlQueryId);
    }
    catch (QueryInterruptedException e) {
      endLifecycle(sqlQueryId, lifecycle, e);
      log.info("Query cancelled: %s", sqlQuery);
      return buildNonOkResponse(QueryInterruptedException.STATUS_CODE, e, sqlQueryId);
    }
    // Calcite throws a java.lang.AssertionError which is type error not exception. using throwable will catch all
    catch (Throwable e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);
      endLifecycle(sqlQueryId, lifecycle, e);

      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          new QueryInterruptedException(e),
          sqlQueryId
      );
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  /**
   * Streaming output to run the query and deliver results to the HTTP client.
   * This class continues to run after the POST request returns: it is
   * responsible for cleaning up for any errors that occur during reading
   * output, and for finishing up upon completion.
   * <p>
   * The {@code yielder} is of critical importance. Is is created in the
   * constructor. If the query fails before returning control to Jetty,
   * then the handler code *must* call {#link #close()} to close the
   * yielder. During the {#link write()} call, this class ensures that
   * the yielder is closed on both the success and failure paths.
   */
  private class QueryOutput implements StreamingOutput
  {
    final SqlLifecycle lifecycle;

    final SqlRowTransformer rowTransformer;
    final QueryResponse<Object[]> queryResponse;
    final String sqlQueryId;
    Yielder<Object[]> yielder;

    private QueryOutput(
        final SqlLifecycle lifecycle
    )
    {
      this.lifecycle = lifecycle;
      this.sqlQueryId = lifecycle.sqlQueryId();
      this.rowTransformer = lifecycle.createRowTransformer();
      this.queryResponse = lifecycle.execute();
      this.yielder = Yielders.each(queryResponse.getResults());
    }

    @Override
    public void write(OutputStream outputStream) throws WebApplicationException
    {
      Exception e = null;
      final CountingOutputStream os = new CountingOutputStream(outputStream);
      long resultRowCount = 0;
      final SqlQuery sqlQuery = lifecycle.sqlQuery();

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
          resultRowCount++;
          yielder = yielder.next(null);
        }

        // Avoid closing again in the finally block if "yielder.close" fails.
        close();

        SqlLifecycle.LifecycleStats stats = lifecycle.stats();
        final ResponseContext responseContext =
            queryResponse.getResponseContext().orElse(ResponseContext.createEmpty());
        stats.setResponseContext(responseContext);
        stats.onResultsSent(resultRowCount, os.getCount());
        if (sqlQuery.getResultFormat().hasTrailer()) {
          writer.writeTrailer(stats.trailer());
        }

        writer.writeResponseEnd();
      }

      // Handle exceptions that occur while producing results.
      catch (QueryCapacityExceededException cap) {
        e = cap;
        log.error(cap, "SQL query failed [%s]", sqlQueryId);
      }
      catch (QueryTimeoutException timeout) {
        e = timeout;
        log.error(timeout, "SQL query failed [%s]", sqlQueryId);
      }
      catch (Exception ex) {
        e = ex;
        log.error(ex, "Unable to send SQL response [%s]", sqlQueryId);
        throw new RuntimeException(ex);
      }
      finally {
        try {
          close();
        }
        catch (Exception ex2) {
          if (e == null) {
            e = ex2;
          } else {
            e.addSuppressed(ex2);
          }
        }
        endLifecycle(sqlQueryId, lifecycle, e);
      }
    }

    public void close() throws IOException
    {
      if (yielder == null) {
        return;
      }
      try {
        yielder.close();
      }
      finally {
        yielder = null;
      }
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
      @Nullable final Throwable e
  )
  {
    lifecycle.finalizeStateAndEmitLogsAndMetrics(e);
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
