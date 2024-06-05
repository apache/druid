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
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
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
     * A new {@link FinalizingFieldAccessPostAggregator} appeared in the plan.
     */
    FINALIZING_FIELD_ACCESS;

    public boolean isPresent()
    {
      return this != NONE;
    }
  }

  boolean separateDefaultModeTest() default false;
}
