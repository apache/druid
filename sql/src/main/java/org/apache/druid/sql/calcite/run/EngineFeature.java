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
 * Arguments to {@link EngineFeatureInspector#feature(EngineFeature, PlannerContext)}.
 */
public enum EngineFeature
{
  /**
   * Can execute INSERT and REPLACE statements.
   */
  CAN_INSERT,

  /**
   * Queries of type {@link org.apache.druid.query.timeseries.TimeseriesQuery} are usable.
   */
  CAN_RUN_TIMESERIES,

  /**
   * Queries of type {@link org.apache.druid.query.topn.TopNQuery} are usable.
   */
  CAN_RUN_TOPN,

  /**
   * Queries of type {@link org.apache.druid.query.timeboundary.TimeBoundaryQuery} are usable.
   */
  CAN_RUN_TIME_BOUNDARY,

  /**
   * Queries can use {@link ExternalDataSource}.
   */
  CAN_READ_EXTERNAL_DATA,

  /**
   * Scan queries can use {@link org.apache.druid.query.scan.ScanQuery#getOrderBys()} that are based on something
   * other than the "__time" column.
   */
  SCAN_CAN_ORDER_BY_NON_TIME,

  /**
   * Scan queries must have {@link org.apache.druid.sql.calcite.rel.DruidQuery#CTX_SCAN_SIGNATURE} set in their
   * query contexts.
   */
  SCAN_NEEDS_SIGNATURE,

  /**
   * Planner is permitted to use a {@link org.apache.calcite.runtime.Bindable} plan on local resources, instead
   * of {@link QueryMaker}, for SELECT query implementation. Used for system tables and the like.
   */
  ALLOW_BINDABLE_PLAN
}
