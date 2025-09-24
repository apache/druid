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
import org.apache.druid.common.exception.SanitizableException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.QueryResultPusher;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlRowTransformer;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

class SqlResourceQueryResultPusher extends QueryResultPusher
{

  private static final Logger log = new Logger(SqlResourceQueryResultPusher.class);

  private final ObjectMapper jsonMapper;
  private final String sqlQueryId;
  private final HttpStatement stmt;
  private final SqlQuery sqlQuery;
  private final ServerConfig serverConfig;

  public SqlResourceQueryResultPusher(
      final ObjectMapper jsonMapper,
      final ResponseContextConfig responseContextConfig,
      final DruidNode selfNode,
      final ServerConfig serverConfig,
      final HttpServletRequest req,
      final HttpStatement stmt,
      final SqlQuery sqlQuery,
      final Map<String, String> headers,
      final QueryCountStatsProvider counter
  )
  {
    super(
        req,
        jsonMapper,
        responseContextConfig,
        selfNode,
        counter,
        stmt.sqlQueryId(),
        MediaType.APPLICATION_JSON_TYPE,
        headers,
        sqlQuery.getContext()
    );
    this.serverConfig = serverConfig;
    this.jsonMapper = jsonMapper;
    this.sqlQueryId = stmt.sqlQueryId();
    this.stmt = stmt;
    this.sqlQuery = sqlQuery;
  }

  @Override
  public ResultsWriter start()
  {
    return new ResultsWriter()
    {
      private QueryResponse<Object[]> queryResponse;
      private DirectStatement.ResultSet thePlan;

      @Override
      @Nullable
      public Response.ResponseBuilder start()
      {
        thePlan = stmt.plan();
        queryResponse = thePlan.run();
        return null;
      }

      @Override
      @SuppressWarnings({"unchecked", "rawtypes"})
      public QueryResponse<Object> getQueryResponse()
      {
        return (QueryResponse) queryResponse;
      }

      @Override
      public Writer makeWriter(OutputStream out) throws IOException
      {
        ResultFormat.Writer writer = sqlQuery.getResultFormat().createFormatter(out, jsonMapper);
        final SqlRowTransformer rowTransformer = thePlan.createRowTransformer();

        return new Writer()
        {

          @Override
          public void writeResponseStart() throws IOException
          {
            writer.writeResponseStart();

            if (sqlQuery.includeHeader()) {
              writer.writeHeader(
                  rowTransformer.getRowType(),
                  sqlQuery.includeTypesHeader(),
                  sqlQuery.includeSqlTypesHeader()
              );
            }
          }

          @Override
          public void writeRow(Object obj) throws IOException
          {
            Object[] row = (Object[]) obj;

            writer.writeRowStart();
            for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
              final Object value = rowTransformer.transform(row, i);
              writer.writeRowField(rowTransformer.getFieldList().get(i), value);
            }
            writer.writeRowEnd();
          }

          @Override
          public void writeResponseEnd() throws IOException
          {
            writer.writeResponseEnd();
          }

          @Override
          public void close() throws IOException
          {
            writer.close();
          }
        };
      }

      @Override
      public void recordSuccess(long numBytes)
      {
        stmt.reporter().succeeded(numBytes);
      }

      @Override
      public void recordFailure(Exception e)
      {
        if (QueryLifecycle.shouldLogStackTrace(e, sqlQuery.queryContext())) {
          log.warn(e, "Exception while processing sqlQueryId[%s]", sqlQueryId);
        } else {
          log.noStackTrace().warn(e, "Exception while processing sqlQueryId[%s]", sqlQueryId);
        }
        stmt.reporter().failed(e);
      }

      @Override
      public void close()
      {
        stmt.close();
      }
    };
  }

  @Override
  public void writeException(Exception ex, OutputStream out) throws IOException
  {
    if (ex instanceof SanitizableException) {
      ex = serverConfig.getErrorResponseTransformStrategy().transformIfNeeded((SanitizableException) ex);
    }
    out.write(jsonMapper.writeValueAsBytes(ex));
  }
}
