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
import com.google.common.collect.Iterables;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
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
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final SqlLifecycleFactory sqlLifecycleFactory;
  private final SqlLifecycleManager sqlLifecycleManager;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper,
      SqlLifecycleFactory sqlLifecycleFactory,
      SqlLifecycleManager sqlLifecycleManager
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.authorizerMapper = Preconditions.checkNotNull(authorizerMapper, "authorizerMapper");
    this.sqlLifecycleFactory = Preconditions.checkNotNull(sqlLifecycleFactory, "sqlLifecycleFactory");
    this.sqlLifecycleManager = Preconditions.checkNotNull(sqlLifecycleManager, "sqlLifecycleManager");
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
      final Optional<DateTimeZone> maybeTimeZone = lifecycle.getTimeZone(); // can be empty when the query is canceled
      if (!maybeTimeZone.isPresent()) {
        sqlLifecycleManager.remove(sqlQueryId, lifecycle);
        return buildCanceledResponse(sqlQueryId);
      }
      final DateTimeZone timeZone = maybeTimeZone.get();
      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      // Also store list of all column names, for X-Druid-Sql-Columns header.
      Optional<RelDataType> maybeRowType = lifecycle.rowType();
      final boolean[] timeColumns, dateColumns;
      final String[] columnNames;
      if (!maybeRowType.isPresent()) {
        sqlLifecycleManager.remove(sqlQueryId, lifecycle);
        return buildCanceledResponse(sqlQueryId);
      }
      final List<RelDataTypeField> fieldList = maybeRowType.get().getFieldList();
      timeColumns = new boolean[fieldList.size()];
      dateColumns = new boolean[fieldList.size()];
      columnNames = new String[fieldList.size()];

      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        columnNames[i] = fieldList.get(i).getName();
      }

      final Optional<Sequence<Object[]>> maybeSequence = lifecycle.execute();
      if (!maybeSequence.isPresent()) {
        sqlLifecycleManager.remove(sqlQueryId, lifecycle);
        return buildCanceledResponse(sqlQueryId);
      }
      final Yielder<Object[]> yielder0 = Yielders.each(maybeSequence.get());

      try {
        return Response
            .ok(
                (StreamingOutput) outputStream -> {
                  Exception e = null;
                  CountingOutputStream os = new CountingOutputStream(outputStream);
                  Yielder<Object[]> yielder = yielder0;

                  try (final ResultFormat.Writer writer = sqlQuery.getResultFormat()
                                                                  .createFormatter(os, jsonMapper)) {
                    writer.writeResponseStart();

                    if (sqlQuery.includeHeader()) {
                      writer.writeHeader(Arrays.asList(columnNames));
                    }

                    while (!yielder.isDone()) {
                      final Object[] row = yielder.get();
                      writer.writeRowStart();
                      for (int i = 0; i < fieldList.size(); i++) {
                        final Object value;

                        if (row[i] == null) {
                          value = null;
                        } else if (timeColumns[i]) {
                          value = ISODateTimeFormat.dateTime().print(
                              Calcites.calciteTimestampToJoda((long) row[i], timeZone)
                          );
                        } else if (dateColumns[i]) {
                          value = ISODateTimeFormat.dateTime().print(
                              Calcites.calciteDateToJoda((int) row[i], timeZone)
                          );
                        } else {
                          value = row[i];
                        }

                        writer.writeRowField(fieldList.get(i).getName(), value);
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
            .header("X-Druid-SQL-Query-Id", sqlQueryId)
            .build();
      }
      catch (Throwable e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder0.close();
        throw new RuntimeException(e);
      }
    }
    catch (QueryCapacityExceededException cap) {
      endLifecycle(sqlQueryId, lifecycle, cap, remoteAddr, -1);
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, cap);
    }
    catch (QueryUnsupportedException unsupported) {
      endLifecycle(sqlQueryId, lifecycle, unsupported, remoteAddr, -1);
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, unsupported);
    }
    catch (QueryTimeoutException timeout) {
      endLifecycle(sqlQueryId, lifecycle, timeout, remoteAddr, -1);
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, timeout);
    }
    catch (SqlPlanningException | ResourceLimitExceededException e) {
      endLifecycle(sqlQueryId, lifecycle, e, remoteAddr, -1);
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e);
    }
    catch (ForbiddenException e) {
      sqlLifecycleManager.remove(sqlQueryId, lifecycle);
      throw e; // let ForbiddenExceptionMapper handle this
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);
      endLifecycle(sqlQueryId, lifecycle, e, remoteAddr, -1);

      final Exception exceptionToReport;

      if (e instanceof RelOptPlanner.CannotPlanException) {
        exceptionToReport = new ISE("Cannot build plan for query: %s", sqlQuery.getQuery());
      } else {
        exceptionToReport = e;
      }

      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(exceptionToReport)
      );
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  private void endLifecycle(
      String sqlQueryId,
      SqlLifecycle lifecycle,
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten
  )
  {
    lifecycle.emitLogsAndMetrics(e, remoteAddress, bytesWritten);
    sqlLifecycleManager.remove(sqlQueryId, lifecycle);
  }

  private Response buildCanceledResponse(String sqlQueryId) throws JsonProcessingException
  {
    return buildNonOkResponse(
        Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        new QueryInterruptedException(
            QueryInterruptedException.QUERY_CANCELLED,
            StringUtils.format("Query is canceled [%s]", sqlQueryId),
            null,
            null
        )
    );
  }

  private Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
  {
    return Response.status(status)
                   .type(MediaType.APPLICATION_JSON_TYPE)
                   .entity(jsonMapper.writeValueAsBytes(e))
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
    Set<Resource> resources = lifecycles
        .stream()
        .flatMap(lifecycle -> lifecycle.getAuthorizedResources().stream())
        .collect(Collectors.toSet());
    Access access = AuthorizationUtils.authorizeAllResourceActions(
        req,
        Iterables.transform(resources, AuthorizationUtils.RESOURCE_READ_RA_GENERATOR),
        authorizerMapper
    );

    if (access.isAllowed()) {
      sqlLifecycleManager.removeAll(sqlQueryId, lifecycles);
      lifecycles.forEach(SqlLifecycle::cancel);
      return Response.status(Status.ACCEPTED).build();
    } else {
      // Return 404 for authorization failures as well
      return Response.status(Status.NOT_FOUND).build();
    }
  }
}
