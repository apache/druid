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
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.junit.Ignore;
import org.junit.Test;

public class DecoupledPlanningCalciteQueryTest extends CalciteQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig())
        .queryContext(ImmutableMap.of(
            PlannerConfig.CTX_NATIVE_QUERY_SQL_PLANNING_MODE, PlannerConfig.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED,
            QueryContexts.ENABLE_DEBUG, true
        ))
        .cannotVectorize(cannotVectorize)
        .skipVectorize(skipVectorize);
  }

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
  public void testSelfJoin()
  {

  }

  @Override
  @Ignore
  public void testTwoExactCountDistincts()
  {

  }

  @Override
  @Ignore
  public void testViewAndJoin()
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
  public void testUseTimeFloorInsteadOfGranularityOnJoinResult()
  {

  }

  @Override
  @Ignore
  public void testMinMaxAvgDailyCountWithLimit()
  {

  }

  @Override
  @Ignore
  public void testExactCountDistinctOfSemiJoinResult()
  {

  }

  @Override
  @Ignore
  public void testMaxSubqueryRows()
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
  public void testUsingSubqueryAsFilterOnTwoColumns()
  {

  }

  @Override
  @Ignore
  public void testUsingSubqueryAsFilterWithInnerSort()
  {

  }

  @Override
  @Ignore
  public void testUsingSubqueryWithLimit()
  {

  }

  @Override
  @Ignore
  public void testPostAggWithTimeseries()
  {

  }

  @Override
  @Ignore
  public void testPostAggWithTopN()
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
  public void testEmptyGroupWithOffsetDoesntInfiniteLoop()
  {

  }

  @Override
  @Ignore
  public void testJoinWithTimeDimension()
  {

  }

  @Override
  @Ignore
  public void testSubqueryTypeMismatchWithLiterals()
  {

  }

  @Override
  @Ignore
  public void testTimeseriesQueryWithEmptyInlineDatasourceAndGranularity()
  {

  }

  @Override
  @Ignore
  public void testGroupBySortPushDown()
  {

  }

  @Override
  @Ignore
  public void testGroupingWithNullInFilter()
  {

  }

  @Override
  @Ignore
  @Test
  public void testStringAggExpressionNonConstantSeparator()
  {

  }

  @Override
  @Ignore
  public void testOrderByAlongWithInternalScanQuery()
  {

  }

  @Override
  @Ignore
  public void testSortProjectAfterNestedGroupBy()
  {

  }

  @Override
  @Ignore
  public void testOrderByAlongWithInternalScanQueryNoDistinct()
  {

  }

  @Override
  @Ignore
  public void testNestedGroupBy()
  {

  }

  @Override
  @Ignore
  public void testQueryWithSelectProjectAndIdentityProjectDoesNotRename()
  {

  }
}
