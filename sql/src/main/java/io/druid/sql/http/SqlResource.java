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
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryInterruptedException;
import org.apache.calcite.jdbc.CalciteConnection;
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
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final Connection connection;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      CalciteConnection connection
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.connection = Preconditions.checkNotNull(connection, "connection");
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(final SqlQuery sqlQuery) throws SQLException, IOException
  {
    // This is not integrated with the experimental authorization framework.
    // (Non-trivial since we don't know the dataSources up-front)

    try {
      final ResultSet resultSet = connection.createStatement().executeQuery(sqlQuery.getQuery());
      final ResultSetMetaData metaData = resultSet.getMetaData();

      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      final boolean[] timeColumns = new boolean[metaData.getColumnCount()];
      for (int i = 0; i < metaData.getColumnCount(); i++) {
        final int columnType = metaData.getColumnType(i + 1);
        if (columnType == Types.TIMESTAMP || columnType == Types.TIME || columnType == Types.DATE) {
          timeColumns[i] = true;
        } else {
          timeColumns[i] = false;
        }
      }

      return Response.ok(
          new StreamingOutput()
          {
            @Override
            public void write(final OutputStream outputStream) throws IOException, WebApplicationException
            {
              try (final JsonGenerator jsonGenerator = jsonMapper.getFactory().createGenerator(outputStream)) {
                jsonGenerator.writeStartArray();
                while (resultSet.next()) {
                  jsonGenerator.writeStartObject();
                  for (int i = 0; i < metaData.getColumnCount(); i++) {
                    final Object value;

                    if (timeColumns[i]) {
                      value = new DateTime(resultSet.getLong(i + 1));
                    } else {
                      value = resultSet.getObject(i + 1);
                    }

                    jsonGenerator.writeObjectField(metaData.getColumnLabel(i + 1), value);
                  }
                  jsonGenerator.writeEndObject();
                }
                jsonGenerator.writeEndArray();
                jsonGenerator.flush();

                // End with CRLF
                outputStream.write('\r');
                outputStream.write('\n');
              }
              catch (SQLException e) {
                throw Throwables.propagate(e);
              }
              finally {
                try {
                  resultSet.close();
                }
                catch (SQLException e) {
                  log.warn(e, "Failed to close ResultSet, ignoring.");
                }
              }
            }
          }
      ).build();
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s", sqlQuery);

      // Unwrap preparing exceptions into potentially more useful exceptions.
      final Throwable maybeUnwrapped;
      if (e instanceof SQLException && e.getMessage().contains("Error while preparing statement")) {
        maybeUnwrapped = e.getCause();
      } else {
        maybeUnwrapped = e;
      }

      return Response.serverError()
                     .type(MediaType.APPLICATION_JSON_TYPE)
                     .entity(jsonMapper.writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(maybeUnwrapped)))
                     .build();
    }
  }
}
