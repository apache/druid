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

package org.apache.druid.msq.dart.controller.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.Engine;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.http.QueryManager;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Resource for Dart queries. API-compatible with {@link SqlResource}, so clients can be pointed from
 * {@code /druid/v2/sql/} to {@code /druid/v2/sql/dart/} without code changes.
 */
@Deprecated
@Path(DartSqlResource.PATH + '/')
public class DartSqlResource extends SqlResource
{
  public static final String PATH = "/druid/v2/sql/dart";

  private final DefaultQueryConfig dartQueryConfig;

  @Inject
  public DartSqlResource(
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper,
      @Dart final SqlStatementFactory sqlStatementFactory,
      final SqlLifecycleManager sqlLifecycleManager,
      final ServerConfig serverConfig,
      final ResponseContextConfig responseContextConfig,
      @Self final DruidNode selfNode,
      final Map<Engine, QueryManager> queryManagers,
      @Dart final DefaultQueryConfig dartQueryConfig
  )
  {
    super(
        jsonMapper,
        authorizerMapper,
        sqlStatementFactory,
        sqlLifecycleManager,
        serverConfig,
        queryManagers,
        responseContextConfig,
        selfNode
    );
    this.dartQueryConfig = dartQueryConfig;
  }

  /**
   * API to issue a query.
   */
  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Override
  public Response doPost(
      final SqlQuery sqlQuery,
      @Context final HttpServletRequest req
  )
  {
    final Map<String, Object> context = new HashMap<>(sqlQuery.getContext());

    // Default context keys from dartQueryConfig.
    for (Map.Entry<String, Object> entry : dartQueryConfig.getContext().entrySet()) {
      context.putIfAbsent(entry.getKey(), entry.getValue());
    }

    /**
     * Dart queryId must be globally unique, so we cannot use the user-provided {@link QueryContexts#CTX_SQL_QUERY_ID}
     * or {@link BaseQuery#QUERY_ID}. Instead we generate a UUID in {@link DartSqlResource#doPost}, overriding whatever
     * the user may have provided. This becomes the {@link Controller#queryId()}.
     *
     * The user-provided {@link QueryContexts#CTX_SQL_QUERY_ID} is still registered with the {@link SqlLifecycleManager}
     * for purposes of query cancellation.
     *
     * The user-provided {@link BaseQuery#QUERY_ID} is ignored.
     */
    final String dartQueryId = UUID.randomUUID().toString();
    context.put(QueryContexts.CTX_DART_QUERY_ID, dartQueryId);

    return super.doPost(sqlQuery.withOverridenContext(context), req);
  }
}
