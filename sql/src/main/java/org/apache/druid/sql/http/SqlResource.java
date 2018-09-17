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
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final PlannerFactory plannerFactory;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      PlannerFactory plannerFactory
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.plannerFactory = Preconditions.checkNotNull(plannerFactory, "connection");
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final PlannerResult plannerResult;
    final DateTimeZone timeZone;

    try (final DruidPlanner planner = plannerFactory.createPlanner(sqlQuery.getContext())) {
      plannerResult = planner.plan(sqlQuery.getQuery(), req);
      timeZone = planner.getPlannerContext().getTimeZone();

      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      // Also store list of all column names, for X-Druid-Sql-Columns header.
      final List<RelDataTypeField> fieldList = plannerResult.rowType().getFieldList();
      final boolean[] timeColumns = new boolean[fieldList.size()];
      final boolean[] dateColumns = new boolean[fieldList.size()];
      final String[] columnNames = new String[fieldList.size()];
      final String[] columnTypes = new String[fieldList.size()];

      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        columnNames[i] = fieldList.get(i).getName();
        columnTypes[i] = sqlTypeName.getName();
      }

      final Yielder<Object[]> yielder0 = Yielders.each(plannerResult.run());

      try {
        return Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(final OutputStream outputStream) throws IOException, WebApplicationException
                  {
                    Yielder<Object[]> yielder = yielder0;

                    try (final ResultFormat.Writer writer = sqlQuery.getResultFormat()
                                                                    .createFormatter(outputStream, jsonMapper)) {
                      writer.writeResponseStart();

                      while (!yielder.isDone()) {
                        final Object[] row = yielder.get();
                        writer.writeRowStart();
                        for (int i = 0; i < fieldList.size(); i++) {
                          final Object value;

                          if (timeColumns[i]) {
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
                    finally {
                      yielder.close();
                    }
                  }
                }
            )
            .header("X-Druid-Column-Names", jsonMapper.writeValueAsString(columnNames))
            .header("X-Druid-Column-Types", jsonMapper.writeValueAsString(columnTypes))
            .build();
      }
      catch (Throwable e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder0.close();
        throw Throwables.propagate(e);
      }
    }
    catch (ForbiddenException e) {
      throw e; // let ForbiddenExceptionMapper handle this
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);

      final Exception exceptionToReport;

      if (e instanceof RelOptPlanner.CannotPlanException) {
        exceptionToReport = new ISE("Cannot build plan for query: %s", sqlQuery.getQuery());
      } else {
        exceptionToReport = e;
      }

      return Response.serverError()
                     .type(MediaType.APPLICATION_JSON_TYPE)
                     .entity(jsonMapper.writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(exceptionToReport)))
                     .build();
    }
  }
}
