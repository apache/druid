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

package org.apache.druid.msq.dart.controller;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.controller.sql.DartSqlClients;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.sql.HttpStatement;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.QueryInfo;
import org.apache.druid.sql.http.QueryManager;
import org.apache.druid.sql.http.SqlQuery;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DartQueryManager implements QueryManager
{
  private static final Logger log = new Logger(DartQueryManager.class);
  private final DartControllerRegistry controllerRegistry;
  private final SqlLifecycleManager sqlLifecycleManager;
  private final DartSqlClients sqlClients;
  private final DefaultQueryConfig dartQueryConfig;
  private final SqlStatementFactory sqlStatementFactory;

  @Inject
  public DartQueryManager(
      DartControllerRegistry controllerRegistry,
      DartSqlClients sqlClients,
      SqlLifecycleManager sqlLifecycleManager,
      @Dart DefaultQueryConfig dartQueryConfig,
      @Dart SqlStatementFactory sqlStatementFactory
  )
  {
    this.dartQueryConfig = dartQueryConfig;
    this.sqlStatementFactory = Preconditions.checkNotNull(sqlStatementFactory, "sqlStatementFactory");
    this.sqlLifecycleManager = Preconditions.checkNotNull(sqlLifecycleManager, "sqlLifecycleManager");
    log.error("CREATED");
    this.controllerRegistry = controllerRegistry;
    this.sqlClients = sqlClients;
  }

  @Override
  public Response cancelQuery(
      String sqlQueryId,
      Function<List<SqlLifecycleManager.Cancelable>, AuthorizationResult> authFunction
  )
  {
    List<SqlLifecycleManager.Cancelable> cancelables = sqlLifecycleManager.getAll(sqlQueryId);
    final AuthorizationResult authResult = authFunction.apply(cancelables);

    if (cancelables.isEmpty()) {
      // Return ACCEPTED even if the query wasn't found. When the Router broadcasts cancellation requests to all
      // Brokers, this ensures the user sees a successful request.
      return Response.status(Response.Status.ACCEPTED).build();
    }

    if (authResult.allowAccessWithNoRestriction()) {
      sqlLifecycleManager.removeAll(sqlQueryId, cancelables);

      // Don't call cancel() on the cancelables. That just cancels native queries, which is useless here. Instead,
      // get the controller and stop it.
      for (SqlLifecycleManager.Cancelable cancelable : cancelables) {
        final HttpStatement stmt = (HttpStatement) cancelable;
        final Object dartQueryId = stmt.context().get(QueryContexts.CTX_DART_QUERY_ID);
        if (dartQueryId instanceof String) {
          final ControllerHolder holder = controllerRegistry.get((String) dartQueryId);
          if (holder != null) {
            holder.cancel();
          }
        } else {
          log.warn(
              "%s[%s] for query[%s] is not a string, cannot cancel.",
              QueryContexts.CTX_DART_QUERY_ID,
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

  @Override
  public List<QueryInfo> getRunningQueries(boolean selfOnly)
  {
    final List<DartQueryInfo> queries = controllerRegistry.getAllHolders()
                                                          .stream()
                                                          .map(DartQueryInfo::fromControllerHolder)
                                                          .collect(Collectors.toList());

    // Add queries from all other servers, if "selfOnly" is false.
    if (!selfOnly) {
      final List<GetQueriesResponse> otherQueries = FutureUtils.getUnchecked(
          Futures.successfulAsList(
              Iterables.transform(sqlClients.getAllClients(), client -> client.getRunningQueries(true))),
          true
      );

      for (final GetQueriesResponse response : otherQueries) {
        if (response != null) {
          response.getQueries().stream()
                  .filter(queryInfo -> (queryInfo instanceof DartQueryInfo))
                  .map(queryInfo -> (DartQueryInfo) queryInfo)
                  .forEach(queries::add);
        }
      }
    }

    // Sort queries by start time, breaking ties by query ID, so the list comes back in a consistent and nice order.
    queries.sort(Comparator.comparing(DartQueryInfo::getStartTime).thenComparing(DartQueryInfo::getDartQueryId));
    return List.copyOf(queries);
  }

  @Override
  public HttpStatement doPost(SqlQuery sqlQuery, HttpServletRequest req)
  {
    final Map<String, Object> context = new HashMap<>(sqlQuery.getContext());

    // Default context keys from dartQueryConfig.
    for (Map.Entry<String, Object> entry : dartQueryConfig.getContext().entrySet()) {
      context.putIfAbsent(entry.getKey(), entry.getValue());
    }

    /**
     * Dart queryId must be globally unique, so we cannot use the user-provided {@link QueryContexts#CTX_SQL_QUERY_ID}
     * or {@link BaseQuery#QUERY_ID}. Instead, we generate a UUID in this function, overriding whatever
     * the user may have provided. This becomes the {@link Controller#queryId()}.
     *
     * The user-provided {@link QueryContexts#CTX_SQL_QUERY_ID} is still registered with the {@link SqlLifecycleManager}
     * for purposes of query cancellation.
     *
     * The user-provided {@link BaseQuery#QUERY_ID} is ignored.
     */
    final String dartQueryId = UUID.randomUUID().toString();
    context.put(QueryContexts.CTX_DART_QUERY_ID, dartQueryId);

    return sqlStatementFactory.httpStatement(sqlQuery.withOverridenContext(context), req);
  }
}
