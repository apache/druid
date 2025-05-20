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
import com.google.inject.Inject;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.QueryKitSpecFactory;
import org.apache.druid.msq.sql.DartQueryKitSpecFactory;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;
import java.util.concurrent.ExecutorService;

@LazySingleton
public class DartSqlEngine implements SqlEngine
{
  public static final String NAME = "msq-dart";

  private final DartControllerContextFactory controllerContextFactory;
  private final DartControllerRegistry controllerRegistry;
  private final DartControllerConfig controllerConfig;
  private final ExecutorService controllerExecutor;
  private final ServerConfig serverConfig;
  private final QueryKitSpecFactory queryKitSpecFactory;

  @Inject
  public DartSqlEngine(
      DartControllerContextFactory controllerContextFactory,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      DartQueryKitSpecFactory queryKitSpecFactory,
      ServerConfig serverConfig
  )
  {
    this(
        controllerContextFactory,
        controllerRegistry,
        controllerConfig,
        Execs.multiThreaded(controllerConfig.getConcurrentQueries(), "dart-controller-%s"),
        queryKitSpecFactory,
        serverConfig
    );
  }

  public DartSqlEngine(
      DartControllerContextFactory controllerContextFactory,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      ExecutorService controllerExecutor,
      QueryKitSpecFactory queryKitSpecFactory,
      ServerConfig serverConfig
  )
  {
    this.controllerContextFactory = controllerContextFactory;
    this.controllerRegistry = controllerRegistry;
    this.controllerConfig = controllerConfig;
    this.controllerExecutor = controllerExecutor;
    this.queryKitSpecFactory = queryKitSpecFactory;
    this.serverConfig = serverConfig;
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
}
