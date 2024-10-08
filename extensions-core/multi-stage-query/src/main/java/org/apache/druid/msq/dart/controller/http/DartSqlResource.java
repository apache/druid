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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.ResponseContextConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Resource for Dart queries. API-compatible with {@link SqlResource}, so clients can be pointed from
 * {@code /druid/v2/sql/} to {@code /druid/v2/sql/dart/} without code changes.
 */
@Path(DartSqlResource.PATH + '/')
public class DartSqlResource extends SqlResource
{
  public static final String PATH = "/druid/v2/sql/dart";

  private static final Logger log = new Logger(DartSqlResource.class);

  private final DartControllerRegistry controllerRegistry;
  private final SqlLifecycleManager sqlLifecycleManager;
  private final DartSqlClients sqlClients;
  private final AuthorizerMapper authorizerMapper;
  private final DefaultQueryConfig dartQueryConfig;

  @Inject
  public DartSqlResource(
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper,
      @Dart final SqlStatementFactory sqlStatementFactory,
      final DartControllerRegistry controllerRegistry,
      final SqlLifecycleManager sqlLifecycleManager,
      final DartSqlClients sqlClients,
      final ServerConfig serverConfig,
      final ResponseContextConfig responseContextConfig,
      @Self final DruidNode selfNode,
      @Dart final DefaultQueryConfig dartQueryConfig
  )
  {
    super(
        jsonMapper,
        authorizerMapper,
        sqlStatementFactory,
        sqlLifecycleManager,
        serverConfig,
        responseContextConfig,
        selfNode
    );
    this.controllerRegistry = controllerRegistry;
    this.sqlLifecycleManager = sqlLifecycleManager;
    this.sqlClients = sqlClients;
    this.authorizerMapper = authorizerMapper;
    this.dartQueryConfig = dartQueryConfig;
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

  /**
   * API to list all running queries.
   *
   * @param selfOnly if true, return queries running on this server. If false, return queries running on all servers.
   * @param req      http request
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public GetQueriesResponse doGetRunningQueries(
      @QueryParam("selfOnly") final String selfOnly,
      @Context final HttpServletRequest req
  )
  {
    final AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);
    final Access stateReadAccess = AuthorizationUtils.authorizeAllResourceActions(
        authenticationResult,
        Collections.singletonList(new ResourceAction(Resource.STATE_RESOURCE, Action.READ)),
        authorizerMapper
    );

    final List<DartQueryInfo> queries =
        controllerRegistry.getAllHolders()
                          .stream()
                          .map(DartQueryInfo::fromControllerHolder)
                          .collect(Collectors.toList());

    // Add queries from all other servers, if "selfOnly" is not set.
    if (selfOnly == null) {
      final List<GetQueriesResponse> otherQueries = FutureUtils.getUnchecked(
          Futures.successfulAsList(
              Iterables.transform(sqlClients.getAllClients(), client -> client.getRunningQueries(true))),
          true
      );

      for (final GetQueriesResponse response : otherQueries) {
        if (response != null) {
          queries.addAll(response.getQueries());
        }
      }
    }

    // Sort queries by start time, breaking ties by query ID, so the list comes back in a consistent and nice order.
    queries.sort(Comparator.comparing(DartQueryInfo::getStartTime).thenComparing(DartQueryInfo::getDartQueryId));

    final GetQueriesResponse response;
    if (stateReadAccess.isAllowed()) {
      // User can READ STATE, so they can see all running queries, as well as authentication details.
      response = new GetQueriesResponse(queries);
    } else {
      // User cannot READ STATE, so they can see only their own queries, without authentication details.
      response = new GetQueriesResponse(
          queries.stream()
                 .filter(
                     query ->
                         authenticationResult.getAuthenticatedBy() != null
                         && authenticationResult.getIdentity() != null
                         && Objects.equals(authenticationResult.getAuthenticatedBy(), query.getAuthenticator())
                         && Objects.equals(authenticationResult.getIdentity(), query.getIdentity()))
                 .map(DartQueryInfo::withoutAuthenticationResult)
                 .collect(Collectors.toList())
      );
    }

    AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
    return response;
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

    // Dart queryId must be globally unique; cannot use user-provided sqlQueryId or queryId.
    final String dartQueryId = UUID.randomUUID().toString();
    context.put(DartSqlEngine.CTX_DART_QUERY_ID, dartQueryId);

    return super.doPost(sqlQuery.withOverridenContext(context), req);
  }

  /**
   * API to cancel a query.
   */
  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @Override
  public Response cancelQuery(
      @PathParam("id") String sqlQueryId,
      @Context final HttpServletRequest req
  )
  {
    log.debug("Received cancel request for query[%s]", sqlQueryId);

    List<SqlLifecycleManager.Cancelable> cancelables = sqlLifecycleManager.getAll(sqlQueryId);
    if (cancelables.isEmpty()) {
      // Return ACCEPTED even if the query wasn't found. When the Router broadcasts cancellation requests to all
      // Brokers, this ensures the user sees a successful request.
      AuthorizationUtils.setRequestAuthorizationAttributeIfNeeded(req);
      return Response.status(Response.Status.ACCEPTED).build();
    }

    final Access access = authorizeCancellation(req, cancelables);

    if (access.isAllowed()) {
      sqlLifecycleManager.removeAll(sqlQueryId, cancelables);

      // Don't call cancel() on the cancelables. That just cancels native queries, which is useless here. Instead,
      // get the controller and stop it.
      for (SqlLifecycleManager.Cancelable cancelable : cancelables) {
        final HttpStatement stmt = (HttpStatement) cancelable;
        final Object dartQueryId = stmt.context().get(DartSqlEngine.CTX_DART_QUERY_ID);
        if (dartQueryId instanceof String) {
          final ControllerHolder holder = controllerRegistry.get((String) dartQueryId);
          if (holder != null) {
            holder.cancel();
          }
        } else {
          log.warn(
              "%s[%s] for query[%s] is not a string, cannot cancel.",
              DartSqlEngine.CTX_DART_QUERY_ID,
              dartQueryId,
              sqlQueryId
          );
        }
      }

      // Return ACCEPTED even if the query wasn't found. When the Router broadcasts cancellation requests to all
      // Brokers, this ensures the user sees a successful request.
      return Response.status(Response.Status.ACCEPTED).build();
    } else {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
  }
}
