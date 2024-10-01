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
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.dart.controller.DartControllerRegistry;
import org.apache.druid.msq.dart.controller.http.DartSqlResource;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;
import java.util.concurrent.ExecutorService;

public class DartSqlEngine implements SqlEngine
{
  private static final String NAME = "msq-dart";

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
  public static final String CTX_DART_QUERY_ID = "dartQueryId";
  public static final String CTX_FULL_REPORT = "fullReport";
  public static final boolean CTX_FULL_REPORT_DEFAULT = false;

  private final DartControllerContextFactory controllerContextFactory;
  private final DartControllerRegistry controllerRegistry;
  private final DartControllerConfig controllerConfig;
  private final ExecutorService controllerExecutor;

  public DartSqlEngine(
      DartControllerContextFactory controllerContextFactory,
      DartControllerRegistry controllerRegistry,
      DartControllerConfig controllerConfig,
      ExecutorService controllerExecutor
  )
  {
    this.controllerContextFactory = controllerContextFactory;
    this.controllerRegistry = controllerRegistry;
    this.controllerConfig = controllerConfig;
    this.controllerExecutor = controllerExecutor;
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
    if (QueryContext.of(queryContext).getBoolean(CTX_FULL_REPORT, CTX_FULL_REPORT_DEFAULT)) {
      return typeFactory.createStructType(
          ImmutableList.of(
              Calcites.createSqlType(typeFactory, SqlTypeName.VARCHAR)
          ),
          ImmutableList.of(CTX_FULL_REPORT)
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
    return new DartQueryMaker(
        relRoot.fields,
        controllerContextFactory,
        plannerContext,
        controllerRegistry,
        controllerConfig,
        controllerExecutor
    );
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
