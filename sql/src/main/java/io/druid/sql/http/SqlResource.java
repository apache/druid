/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.http;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.Yielders;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryInterruptedException;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Planner;
import org.joda.time.DateTime;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
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
  public Response doPost(final SqlQuery sqlQuery) throws SQLException, IOException
  {
    // This is not integrated with the experimental authorization framework.
    // (Non-trivial since we don't know the dataSources up-front)

    final PlannerResult plannerResult;

    try (final Planner planner = plannerFactory.createPlanner()) {
      plannerResult = Calcites.plan(planner, sqlQuery.getQuery());
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

    // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
    final List<RelDataTypeField> fieldList = plannerResult.rowType().getFieldList();
    final boolean[] timeColumns = new boolean[fieldList.size()];
    for (int i = 0; i < fieldList.size(); i++) {
      final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
      timeColumns[i] = SqlTypeName.DATETIME_TYPES.contains(sqlTypeName);
    }

    final Yielder<Object[]> yielder0 = Yielders.each(plannerResult.run());

    return Response.ok(
        new StreamingOutput()
        {
          @Override
          public void write(final OutputStream outputStream) throws IOException, WebApplicationException
          {
            Yielder<Object[]> yielder = yielder0;

            try (final JsonGenerator jsonGenerator = jsonMapper.getFactory().createGenerator(outputStream)) {
              jsonGenerator.writeStartArray();

              while (!yielder.isDone()) {
                final Object[] row = yielder.get();
                jsonGenerator.writeStartObject();
                for (int i = 0; i < fieldList.size(); i++) {
                  final Object value;

                  if (timeColumns[i]) {
                    value = new DateTime((long) row[i]);
                  } else {
                    value = row[i];
                  }

                  jsonGenerator.writeObjectField(fieldList.get(i).getName(), value);
                }
                jsonGenerator.writeEndObject();
                yielder = yielder.next(null);
              }

              jsonGenerator.writeEndArray();
              jsonGenerator.flush();

              // End with CRLF
              outputStream.write('\r');
              outputStream.write('\n');
            }
            finally {
              yielder.close();
            }
          }
        }
    ).build();
  }
}
