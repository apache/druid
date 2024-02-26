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

package org.apache.druid.sql.calcite.view;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;

/**
 * Engine used for getting the row type of views. Does not do any actual planning or execution of the view.
 */
public class ViewSqlEngine implements SqlEngine
{
  public static final ViewSqlEngine INSTANCE = new ViewSqlEngine();
  private static final String NAME = "view";

  private ViewSqlEngine()
  {
    // Singleton.
  }

  @Override
  public String name()
  {
    return NAME;
  }

  @Override
  public boolean featureAvailable(EngineFeature feature, PlannerContext plannerContext)
  {
    switch (feature) {
      // Use most permissive set of SELECT features, since our goal is to get the row type of the view.
      // Later on, the query involving the view will be executed with an actual engine with a different set of
      // features, and planning may fail then. But we don't want it to fail now.
      case CAN_SELECT:
      case ALLOW_BINDABLE_PLAN:
      case READ_EXTERNAL_DATA:
      case WRITE_EXTERNAL_DATA:
      case SCAN_ORDER_BY_NON_TIME:
      case GROUPING_SETS:
      case WINDOW_FUNCTIONS:
      case UNNEST:
      case ALLOW_TOP_LEVEL_UNION_ALL:
        return true;
      // Views can't sit on top of INSERT or REPLACE.
      case CAN_INSERT:
      case CAN_REPLACE:
        return false;

      // Simplify planning by sticking to basic query types.
      case TOPN_QUERY:
      case TIMESERIES_QUERY:
      case TIME_BOUNDARY_QUERY:
      case SCAN_NEEDS_SIGNATURE:
        return false;

      default:
        throw SqlEngines.generateUnrecognizedFeatureException(ViewSqlEngine.class.getSimpleName(), feature);
    }
  }

  @Override
  public void validateContext(Map<String, Object> queryContext)
  {
    // No query context validation for view row typing.
  }

  @Override
  public RelDataType resultTypeForSelect(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    return validatedRowType;
  }

  @Override
  public RelDataType resultTypeForInsert(RelDataTypeFactory typeFactory, RelDataType validatedRowType)
  {
    // Can't have views of INSERT or REPLACE statements.
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    // View engine does not execute queries.
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(IngestDestination destination, RelRoot relRoot, PlannerContext plannerContext)
  {
    // Can't have views of INSERT or REPLACE statements.
    throw new UnsupportedOperationException();
  }
}
