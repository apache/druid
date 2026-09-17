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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngines;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;

/**
 * Engine used to plan the body of a projection definition without running it. The planned {@link DruidQuery} is
 * captured so the projection specification can be lifted out of it.
 * <p>
 * A projection is defined by SQL but stored as a native specification, and it is only useful if it matches the
 * queries the planner generates at query time. Planning the body through the normal pipeline is what makes the two
 * agree: the same aggregator factories, virtual column expressions and filters come out either way.
 * <p>
 * Not a singleton, unlike {@link org.apache.druid.sql.calcite.view.ViewSqlEngine}: each instance captures one query.
 */
public class ProjectionSqlEngine implements SqlEngine
{
  private static final String NAME = "projection";

  private DruidQuery captured;

  /**
   * The query planned for the projection body, available after the statement has been planned and run.
   */
  public DruidQuery captured()
  {
    if (captured == null) {
      throw DruidException.defensive("Projection body was not planned into a native query");
    }
    return captured;
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
      case GROUPING_SETS:
        return true;

      // A projection stores grouping columns and aggregators, so the body must plan to a group-by rather than to a
      // specialized query shape that would hide them.
      case TIMESERIES_QUERY:
      case TOPN_QUERY:
      case TIME_BOUNDARY_QUERY:
      case GROUPBY_IMPLICITLY_SORTS:
      case ALLOW_BINDABLE_PLAN:
        return false;

      // The body has no FROM clause, so it can only read the table it belongs to.
      case READ_EXTERNAL_DATA:
      case WRITE_EXTERNAL_DATA:
      case CAN_INSERT:
      case CAN_REPLACE:
      case CAN_DDL:
      case SCAN_ORDER_BY_NON_TIME:
      case WINDOW_FUNCTIONS:
      case WINDOW_LEAF_OPERATOR:
      case UNNEST:
      case ALLOW_BROADCAST_RIGHTY_JOIN:
      case ALLOW_TOP_LEVEL_UNION_ALL:
        return false;

      default:
        throw SqlEngines.generateUnrecognizedFeatureException(ProjectionSqlEngine.class.getSimpleName(), feature);
    }
  }

  @Override
  public void validateContext(Map<String, Object> queryContext)
  {
    // The context is supplied by the translator, not by the user.
  }

  @Override
  public RelDataType resultTypeForSelect(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext
  )
  {
    return validatedRowType;
  }

  @Override
  public RelDataType resultTypeForInsert(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext)
  {
    return druidQuery -> {
      captured = druidQuery;
      return QueryResponse.withEmptyContext(Sequences.empty());
    };
  }

  @Override
  public QueryMaker buildQueryMakerForInsert(
      IngestDestination destination,
      RelRoot relRoot,
      PlannerContext plannerContext
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SqlStatementFactory getSqlStatementFactory()
  {
    throw new UnsupportedOperationException();
  }
}
