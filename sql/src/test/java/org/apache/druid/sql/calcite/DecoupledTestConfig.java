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

package org.apache.druid.sql.calcite;

import org.apache.calcite.rel.rules.CoreRules;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.UnnestDataSource;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Specifies test case level matching customizations for decoupled mode.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface DecoupledTestConfig
{
  /**
   * Enables the framework to ignore native query differences.
   *
   * The value of this field should describe the root cause of the difference.
   */
  QuidemTestCaseReason quidemReason() default QuidemTestCaseReason.NONE;

  /**
   * Run the tests normally; however disable the native plan checks.
   */
  IgnoreQueriesReason ignoreExpectedQueriesReason() default IgnoreQueriesReason.NONE;

  enum IgnoreQueriesReason
  {
    NONE,
    /**
     * An extra ScanQuery to service a Project and/or Filter was added.
     */
    UNNEST_EXTRA_SCANQUERY;

    public boolean isPresent()
    {
      return this != NONE;
    }
  }

  enum QuidemTestCaseReason
  {
    NONE,
    /**
     * Aggregate column order changes.
     *
     * dim1/dim2 exchange
     */
    AGG_COL_EXCHANGE,
    /**
     * This happens when {@link CoreRules#AGGREGATE_REMOVE} gets supressed by {@link CoreRules#AGGREGATE_CASE_TO_FILTER}
     */
    AGGREGATE_REMOVE_NOT_FIRED,
    /**
     * Improved plan
     *
     * Seen that some are induced by {@link CoreRules#AGGREGATE_ANY_PULL_UP_CONSTANTS}
     * And in some cases decoupled has moved virtualcolumn to postagg
     */
    IMPROVED_PLAN,
    /**
     * Worse plan; may loose vectorization; but no extra queries
     */
    SLIGHTLY_WORSE_PLAN,
    /**
     * Equvivalent plan.
     *
     * Renamed variable
     */
    EQUIV_PLAN,
    /**
     * {@link QueryContexts#SQL_JOIN_LEFT_SCAN_DIRECT} not supported.
     */
    JOIN_LEFT_DIRECT_ACCESS,
    /**
     * Different filter layout.
     *
     * Filter is pushed below join to the left.
     */
    JOIN_FILTER_LOCATIONS,
    /**
     * New scans / etc.
     */
    DEFINETLY_WORSE_PLAN,
    /**
     * Some extra unused columns are being projected.
     *
     * Example: ScanQuery over a join projects columns=[dim2, j0.m1, m1, m2] instead of just columns=[dim2, m2]
     */
    EQUIV_PLAN_EXTRA_COLUMNS,
    /**
     * Materialization of a CAST was pushed down to a join branch
     *
     * instead of  joining on condition (CAST("j0.k", 'DOUBLE') == "_j0.m1")
     * a vc was computed for CAST("j0.k", 'DOUBLE')
     */
    EQUIV_PLAN_CAST_MATERIALIZED_EARLIER,
    /**
     * Filter pushed down.
     *
     * Instead:
     *  Filter -> Join -> Table
     *  Join -> Filter -> Table
     */
    SLIGHTLY_WORSE_FILTER_PUSHED_TO_JOIN_OPERAND,
    /**
     * Instead:
     * Join (l=r && lCol='a') -> Gby
     * Join (l=r) -> Gby[lCol='a]
     */
    FILTER_PUSHED_DOWN_FROM_JOIN_CAN_BE_MORE,
    /**
     * Strange things; needs more investigation
     */
    IRRELEVANT_SCANQUERY,
    /**
     * Extra scan query under {@link UnnestDataSource}.
     */
    UNNEST_EXTRA_SCAN,
    /**
     * Extra virtualcolumn appeared; seemingly unused
     */
    UNUSED_VIRTUALCOLUMN,
    /**
     * Unnest uses a VC to access a constant like array(1,2,3).
     */
    UNNEST_VC_USES_PROJECTED_CONSTANT,
    /**
     * This should need some investigation.
     *
     * Its not invalid; just strange.
     */
    SCAN_QUERY_ON_FILTERED_DS_DOING_FILTERING,
    /**
     * New plan UNNEST-s a different resultset.
     */
    UNNEST_DIFFERENT_RESULTSET,
    /**
     * Uses a UNION ALL query.
     */
    UNION_ALL_QUERY,
    /**
     * This is due to substring('',1') is null.
     */
    UNNEST_SUBSTRING_EMPTY;

    public boolean isPresent()
    {
      return this != NONE;
    }
  }

  boolean separateDefaultModeTest() default false;
}
