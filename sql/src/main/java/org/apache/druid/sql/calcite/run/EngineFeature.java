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

package org.apache.druid.sql.calcite.run;

import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;

/**
 * Arguments to {@link SqlEngine#featureAvailable(EngineFeature, PlannerContext)}.
 */
public enum EngineFeature
{
  /**
   * Can execute SELECT statements.
   */
  CAN_SELECT,

  /**
   * Can execute INSERT statements.
   */
  CAN_INSERT,

  /**
   * Can execute REPLACE statements.
   */
  CAN_REPLACE,

  /**
   * Queries of type {@link org.apache.druid.query.timeseries.TimeseriesQuery} are usable.
   */
  TIMESERIES_QUERY,

  /**
   * Queries of type {@link org.apache.druid.query.topn.TopNQuery} are usable.
   */
  TOPN_QUERY,

  /**
   * Queries of type {@link org.apache.druid.query.timeboundary.TimeBoundaryQuery} are usable.
   */
  TIME_BOUNDARY_QUERY,

  /**
   * Queries can use {@link ExternalDataSource}.
   */
  READ_EXTERNAL_DATA,

  /**
   * Scan queries can use {@link org.apache.druid.query.scan.ScanQuery#getOrderBys()} that are based on something
   * other than the "__time" column.
   */
  SCAN_ORDER_BY_NON_TIME,

  /**
   * Scan queries must have {@link org.apache.druid.sql.calcite.rel.DruidQuery#CTX_SCAN_SIGNATURE} set in their
   * query contexts.
   *
   * {@link Deprecated} Instead of the context value {@link org.apache.druid.query.scan.ScanQuery#getRowSignature()} can be used.
   */
  @Deprecated
  SCAN_NEEDS_SIGNATURE,

  /**
   * Planner is permitted to use a {@link org.apache.calcite.runtime.Bindable} plan on local resources, instead
   * of {@link QueryMaker}, for SELECT query implementation. Used for system tables and the like.
   */
  ALLOW_BINDABLE_PLAN,

  /**
   * Queries can use GROUPING SETS.
   */
  GROUPING_SETS,

  /**
   * Queries can use window functions.
   */
  WINDOW_FUNCTIONS,

  /**
   * Queries can use {@link org.apache.calcite.sql.fun.SqlStdOperatorTable#UNNEST}.
   */
  UNNEST,

  /**
   * Planner is permitted to use {@link org.apache.druid.sql.calcite.planner.JoinAlgorithm#BROADCAST} with RIGHT
   * and FULL join. Not guaranteed to produce correct results in either the native or MSQ engines, but we allow
   * it in native for two reasons: legacy (the docs caution against it, but it's always been allowed), and the fact
   * that it actually *does* generate correct results in native when the join is processed on the Broker. It is much
   * less likely that MSQ will plan in such a way that generates correct results.
   */
  ALLOW_BROADCAST_RIGHTY_JOIN,

  /**
   * Planner is permitted to use {@link org.apache.druid.sql.calcite.rel.DruidUnionRel} to plan the top level UNION ALL.
   * This is to dissuade planner from accepting and running the UNION ALL queries that are not supported by engines
   * (primarily MSQ).
   *
   * Due to the nature of the exeuction of the top level UNION ALLs (we run the individual queries and concat the
   * results), it only makes sense to enable this on engines where the queries return the results synchronously
   *
   * Planning queries with top level UNION_ALL leads to undesirable behaviour with asynchronous engines like MSQ.
   * To enumerate this behaviour for MSQ, the broker attempts to run the individual queries as MSQ queries in succession,
   * submits the first query correctly, fails on the rest of the queries (due to conflicting taskIds),
   * and cannot concat the results together (as * the result for broker is the query id). Therefore, we don't get the
   * correct result back, while the MSQ engine is executing the partial query
   */
  ALLOW_TOP_LEVEL_UNION_ALL,
  /**
   * Queries can write to an external datasource using {@link org.apache.druid.sql.destination.ExportDestination}
   */
  WRITE_EXTERNAL_DATA;
}
