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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
<<<<<<< HEAD
import org.junit.Ignore;
=======
import org.junit.Rule;
>>>>>>> apache/master

public class DecoupledPlanningCalciteQueryTest extends CalciteQueryTest
{

  @Rule(order = 0)
  public NotYetSupportedProcessor decoupledIgnoreProcessor = new NotYetSupportedProcessor();

  private static final ImmutableMap<String, Object> CONTEXT_OVERRIDES = ImmutableMap.of(
      PlannerConfig.CTX_NATIVE_QUERY_SQL_PLANNING_MODE, PlannerConfig.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED,
      QueryContexts.ENABLE_DEBUG, true);

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(
        new CalciteTestConfig(CONTEXT_OVERRIDES)
        {
          @Override
          public SqlTestFramework.PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
          {
            plannerConfig = plannerConfig.withOverrides(CONTEXT_OVERRIDES);
            return queryFramework().plannerFixture(DecoupledPlanningCalciteQueryTest.this, plannerConfig, authConfig);
          }
        })
        .cannotVectorize(cannotVectorize)
        .skipVectorize(skipVectorize);
  }
<<<<<<< HEAD


  @Override
  @Ignore
  public void testGroupByWithSelectAndOrderByProjections()
  {

  }

  @Override
  @Ignore
  public void testTopNWithSelectAndOrderByProjections()
  {

  }

  @Override
  @Ignore
  public void testUnionAllQueries()
  {

  }

  @Override
  @Ignore
  public void testUnionAllQueriesWithLimit()
  {

  }

  @Override
  @Ignore
  public void testUnionAllDifferentTablesWithMapping()
  {

  }

  @Override
  @Ignore
  public void testJoinUnionAllDifferentTablesWithMapping()
  {

  }

  @Override
  @Ignore
  public void testUnionAllTablesColumnTypeMismatchFloatLong()
  {

  }

  @Override
  @Ignore
  public void testUnionAllTablesColumnTypeMismatchStringLong()
  {

  }

  @Override
  @Ignore
  public void testUnionAllTablesWhenMappingIsRequired()
  {

  }

  @Override
  @Ignore
  public void testUnionIsUnplannable()
  {

  }

  @Override
  @Ignore
  public void testUnionAllTablesWhenCastAndMappingIsRequired()
  {

  }

  @Override
  @Ignore
  public void testUnionAllSameTableTwice()
  {

  }

  @Override
  @Ignore
  public void testUnionAllSameTableTwiceWithSameMapping()
  {

  }

  @Override
  @Ignore
  public void testUnionAllSameTableTwiceWithDifferentMapping()
  {

  }

  @Override
  @Ignore
  public void testUnionAllSameTableThreeTimes()
  {

  }

  @Override
  @Ignore
  public void testUnionAllSameTableThreeTimesWithSameMapping()
  {

  }

  @Override
  @Ignore
  public void testGroupByWithSortOnPostAggregationDefault()
  {

  }

  @Override
  @Ignore
  public void testGroupByWithSortOnPostAggregationNoTopNConfig()
  {

  }

  @Override
  @Ignore
  public void testGroupByWithSortOnPostAggregationNoTopNContext()
  {

  }

  @Override
  @Ignore
  public void testUnplannableQueries()
  {

  }

  @Override
  @Ignore
  public void testUnplannableTwoExactCountDistincts()
  {

  }

  @Override
  @Ignore
  public void testUnplannableExactCountDistinctOnSketch()
  {

  }

  @Override
  @Ignore
  public void testExactCountDistinctUsingSubqueryOnUnionAllTables()
  {

  }

  @Override
  @Ignore
  public void testExactCountDistinctUsingSubqueryWithWherePushDown()
  {

  }

  @Override
  @Ignore
  public void testGroupByTimeFloorAndDimOnGroupByTimeFloorAndDim()
  {

  }

  @Override
  @Ignore
  public void testRequireTimeConditionPositive()
  {

  }

  @Override
  public void testRequireTimeConditionSemiJoinNegative()
  {

  }

  @Override
  @Ignore
  public void testOrderByAlongWithInternalScanQuery()
  {

  }

  @Override
  @Ignore
  public void testOrderByAlongWithInternalScanQueryNoDistinct()
  {

  }

  @Override
  @Ignore
  public void testFilterOnCurrentTimestampWithIntervalArithmetic()
  {

  }

  @Override
  @Ignore
  public void testFilterOnCurrentTimestampOnView()
  {

  }

  @Override
  @Ignore
  public void testPlanWithInFilterLessThanInSubQueryThreshold()
  {

  }
=======
>>>>>>> apache/master
}
