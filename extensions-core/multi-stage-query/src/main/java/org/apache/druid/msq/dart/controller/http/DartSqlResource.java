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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.query.Engine;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.http.QueryManager;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 * Resource for Dart queries. API-compatible with {@link SqlResource}, so clients can be pointed from
 * {@code /druid/v2/sql/} to {@code /druid/v2/sql/dart/} without code changes.
 */
@Deprecated
@Path(DartSqlResource.PATH + '/')
public class DartSqlResource extends SqlResource
{
  public static final String PATH = "/druid/v2/sql/dart";

  @Inject
  public DartSqlResource(
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper,
      final ServerConfig serverConfig,
      final ResponseContextConfig responseContextConfig,
      @Self final DruidNode selfNode,
      final Map<Engine, QueryManager> queryManagers
  )
  {
    super(
        jsonMapper,
        authorizerMapper,
        serverConfig,
        queryManagers,
        responseContextConfig,
        selfNode
    );
  }

  /**
   * API that allows callers to check if this resource is installed without actually issuing a query. If installed,
   * this call returns 200 OK. If not installed, callers get 404 Not Found.
   */
  @GET
  @Path("/enabled")
  @Produces(MediaType.APPLICATION_JSON)
  public Response doGetEnabled(@Context final HttpServletRequest request)
  {
    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(request);
    return Response.ok(ImmutableMap.of("enabled", true)).build();
  }

  @Override
  public Response doGetRunningQueries(
      String selfOnly,
      @Nullable String engineString,
      HttpServletRequest request
  )
  {
    return super.doGetRunningQueries(selfOnly, engineString, request);
  }

  @Nullable
  @Override
  public Response doPost(SqlQuery sqlQuery, String engineString, HttpServletRequest req)
  {
    return super.doPost(sqlQuery, engineString, req);
  }

  @Override
  public Response cancelQuery(String sqlQueryId, String engineString, HttpServletRequest req)
  {
    return super.cancelQuery(sqlQueryId, engineString, req);
  }
}
