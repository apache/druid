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

package org.apache.druid.msq.dart.controller.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.ControllerHolder;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.QueryKitSpecFactory;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.sql.DartQueryKitSpecFactory;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.destination.IngestDestination;
import org.apache.druid.sql.http.GetQueriesResponse;
import org.apache.druid.sql.http.QueryInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@LazySingleton
public class DartSqlEngine implements SqlEngine
{
  public static final String NAME = "msq-dart";
  private static final Logger log = new Logger(DartSqlEngine.class);

  private final DartControllerContextFactory controllerContextFactory;
  private final DartControllerRegistry controllerRegistry;
  private final DartControllerConfig controllerConfig;
  private final ExecutorService controllerExecutor;
  private final ServerConfig serverConfig;
  private final QueryKitSpecFactory queryKitSpecFactory;
  private final DefaultQueryConfig dartQueryConfig;
  private final SqlToolbox toolbox;
  private final DartSqlClients sqlClients;

  @Inject
  public DartSqlEngine(
      DartControllerContextFactory controllerContextFactory,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      DartQueryKitSpecFactory queryKitSpecFactory,
      ServerConfig serverConfig,
      @Dart DefaultQueryConfig dartQueryConfig,
      SqlToolbox toolbox,
      DartSqlClients sqlClients
  )
  {
    this(
        controllerContextFactory,
        controllerRegistry,
        controllerConfig,
        Execs.multiThreaded(controllerConfig.getConcurrentQueries(), "dart-controller-%s"),
        queryKitSpecFactory,
        serverConfig,
        dartQueryConfig,
        toolbox,
        sqlClients
    );
  }

  public DartSqlEngine(
      DartControllerContextFactory controllerContextFactory,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      ExecutorService controllerExecutor,
      QueryKitSpecFactory queryKitSpecFactory,
      ServerConfig serverConfig,
      DefaultQueryConfig dartQueryConfig,
      SqlToolbox toolbox,
      DartSqlClients sqlClients
  )
  {
    this.controllerContextFactory = controllerContextFactory;
    this.controllerRegistry = controllerRegistry;
    this.controllerConfig = controllerConfig;
    this.controllerExecutor = controllerExecutor;
    this.queryKitSpecFactory = queryKitSpecFactory;
    this.serverConfig = serverConfig;
    this.dartQueryConfig = dartQueryConfig;
    this.toolbox = toolbox;
    this.sqlClients = sqlClients;
  }

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public boolean featureAvailable(EngineFeature feature)
  {
    switch (feature) {
      case CAN_SELECT:
      case SCAN_ORDER_BY_NON_TIME:
      case SCAN_NEEDS_SIGNATURE:
      case WINDOW_FUNCTIONS:
      case WINDOW_LEAF_OPERATOR:
      case UNNEST:
        return true;

      case CAN_INSERT:
      case CAN_REPLACE:
      case READ_EXTERNAL_DATA:
      case ALLOW_BINDABLE_PLAN:
      case ALLOW_BROADCAST_RIGHTY_JOIN:
      case ALLOW_TOP_LEVEL_UNION_ALL:
      case TIMESERIES_QUERY:
      case TOPN_QUERY:
      case TIME_BOUNDARY_QUERY:
      case GROUPING_SETS:
      case GROUPBY_IMPLICITLY_SORTS:
        return false;

      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public void validateContext(Map<String, Object> queryContext)
  {
    SqlEngines.validateNoSpecialContextKeys(queryContext, MSQTaskSqlEngine.SYSTEM_CONTEXT_PARAMETERS);
  }

  @Override
  public RelDataType resultTypeForSelect(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext
  )
  {
    if (QueryContext.of(queryContext).getFullReport()) {
      return typeFactory.createStructType(
          ImmutableList.of(
              Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)
          ),
          ImmutableList.of(QueryContexts.CTX_FULL_REPORT)
      );
    } else {
      return validatedRowType;
    }
  }

  @Override
  public RelDataType resultTypeForInsert(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext
  )
  {
    // Defensive, because we expect this method will not be called without the CAN_INSERT and CAN_REPLACE features.
    throw DruidException.defensive("Cannot execute DML commands with engine[%s]", name());
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    DartQueryMaker dartQueryMaker = new DartQueryMaker(
        relRoot.fields,
        controllerContextFactory,
        plannerContext,
        controllerRegistry,
        controllerConfig,
        controllerExecutor,
        queryKitSpecFactory,
        serverConfig
    );
    if (plannerContext.queryContext().isPrePlanned()) {
      return new PrePlannedDartQueryMaker(plannerContext, dartQueryMaker);
    }
    return dartQueryMaker;
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(
      IngestDestination destination,
      RelRoot relRoot,
      PlannerContext plannerContext
  )
  {
    // Defensive, because we expect this method will not be called without the CAN_INSERT and CAN_REPLACE features.
    throw DruidException.defensive("Cannot execute DML commands with engine[%s]", name());
  }

  @Override
  public void initContextMap(Map<String, Object> contextMap)
  {
    // Default context keys from dartQueryConfig.
    for (Map.Entry<String, Object> entry : dartQueryConfig.getContext().entrySet()) {
      contextMap.putIfAbsent(entry.getKey(), entry.getValue());
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
    contextMap.put(QueryContexts.CTX_DART_QUERY_ID, dartQueryId);
  }

  @Override
  public SqlStatementFactory getSqlStatementFactory()
  {
    return new SqlStatementFactory(toolbox.withEngine(this));
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
  public void cancelQuery(PlannerContext plannerContext, QueryScheduler queryScheduler)
  {
    final Object dartQueryId = plannerContext.queryContext().get(QueryContexts.CTX_DART_QUERY_ID);
    if (dartQueryId instanceof String) {
      final ControllerHolder holder = controllerRegistry.get((String) dartQueryId);
      if (holder != null) {
        holder.cancel(CancellationReason.USER_REQUEST);
      }
    } else {
      log.warn(
          "%s[%s] for query[%s] is not a string, cannot cancel.",
          QueryContexts.CTX_DART_QUERY_ID,
          dartQueryId,
          plannerContext.getSqlQueryId()
      );
    }
  }
}
