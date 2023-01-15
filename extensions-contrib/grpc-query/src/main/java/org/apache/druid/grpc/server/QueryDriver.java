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

package org.apache.druid.grpc.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.apache.calcite.avatica.SqlType;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlParameter;
import org.druid.grpc.proto.QueryOuterClass.QueryParameter;
import org.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.druid.grpc.proto.QueryOuterClass.QueryStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryDriver
{
  private final ObjectMapper jsonMapper;
  private final SqlStatementFactory sqlStatementFactory;

  @Inject
  public QueryDriver(
      final @Json ObjectMapper jsonMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.sqlStatementFactory = Preconditions.checkNotNull(sqlStatementFactory, "sqlStatementFactory");
  }

  public QueryResponse submitQuery(QueryRequest request)
  {
    final SqlQueryPlus queryPlus = translateQuery(request);
    final DirectStatement stmt = sqlStatementFactory.directStatement(queryPlus);
    final String currThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", stmt.sqlQueryId()));
      final ResultSet thePlan = stmt.plan();
      final ByteString results = encodeResults(request.getResultFormat(), thePlan);
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.OK)
          .setData(results)
          .build();
    }
    catch (ForbiddenException e) {
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.UNAUTHORIZED)
          .build();
    }
    catch (SqlPlanningException e) {
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.INVALID_SQL)
          .setErrorMessage(e.getMessage())
          .build();
    }
    catch (IOException | RuntimeException e) {
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.RUNTIME_ERROR)
          .setErrorMessage(e.getMessage())
          .build();
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  private ByteString encodeResults(QueryResultFormat queryResultFormat, ResultSet thePlan) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    org.apache.druid.server.QueryResponse<Object[]> queryResponse = thePlan.run();
    ResultFormat.Writer writer;
    switch (queryResultFormat) {
      case CSV:
        writer = ResultFormat.CSV.createFormatter(out, jsonMapper);
        break;
      case JSON_ARRAY:
        writer = ResultFormat.ARRAY.createFormatter(out, jsonMapper);
        break;
      case JSON_ARRAY_LINES:
        writer = ResultFormat.ARRAYLINES.createFormatter(out, jsonMapper);
        break;
      case JSON_OBJECT:
        throw new UnsupportedOperationException(); // TODO
      case JSON_OBJECT_LINES:
        throw new UnsupportedOperationException(); // TODO
      case PROTOBUF_INLINE:
        throw new UnsupportedOperationException(); // TODO
      case PROTOBUF_RESPONSE:
        throw new UnsupportedOperationException(); // TODO
      default:
        throw new IAE("Unsupported query result format");
    }
    final SqlRowTransformer rowTransformer = thePlan.createRowTransformer();
    return null;
  }

  private SqlQueryPlus translateQuery(QueryRequest request)
  {
    AuthenticationResult authResult = new AuthenticationResult(
        "testSuperuser",
        AuthConfig.ALLOW_ALL_NAME,
        null, null
    );
    return SqlQueryPlus.builder()
        .sql(request.getQuery())
        .context(translateContext(request))
        .sqlParameters(translateParameters(request))
        .auth(authResult)
        .build();
  }

  private Map<String, Object> translateContext(QueryRequest request)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    if (request.getContextCount() > 0) {
      for (Map.Entry<String, String> entry : request.getContextMap().entrySet()) {
        builder.put(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private List<SqlParameter> translateParameters(QueryRequest request)
  {
    if (request.getParametersCount() == 0) {
      return null;
    }
    List<SqlParameter> params = new ArrayList<>();
    for (QueryParameter value : request.getParametersList()) {
      params.add(translateParameter(value));
    }
    return params;
   }

  private SqlParameter translateParameter(QueryParameter value)
  {
    switch (value.getValueCase()) {
    case ARRAYVALUE:
      // Not yet supported: waiting for an open PR
      return null;
    case DOUBLEVALUE:
      return new SqlParameter(SqlType.DOUBLE, value.getDoubleValue());
    case LONGVALUE:
      return new SqlParameter(SqlType.BIGINT, value.getLongValue());
    case STRINGVALUE:
      return new SqlParameter(SqlType.VARCHAR, value.getStringValue());
    case NULLVALUE:
    case VALUE_NOT_SET:
    default:
      return null;
    }
  }
}
