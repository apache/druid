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

package org.apache.druid.msq.test;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.CalciteJoinQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.planner.JoinAlgorithm;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@link CalciteJoinQueryTest} but with MSQ engine.
 */
public class CalciteSelectJoinQueryMSQTest
{
  /**
   * Run all tests with {@link JoinAlgorithm#BROADCAST}.
   */
  @Nested
  public class BroadcastTest extends Base
  {
    @Override
    protected QueryTestBuilder testBuilder()
    {
      return super.testBuilder()
                  .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
    }

    @Override
    protected JoinAlgorithm joinAlgorithm()
    {
      return JoinAlgorithm.BROADCAST;
    }
  }

  /**
   * Run all tests with {@link JoinAlgorithm#SORT_MERGE}.
   */
  @Nested
  public class SortMergeTest extends Base
  {
    @Override
    public boolean isSortBasedJoin()
    {
      return true;
    }

    @Override
    protected QueryTestBuilder testBuilder()
    {
      // Don't verify native queries for sort-merge join, since the structure is different.
      // (Lots of extra subqueries.)
      return super.testBuilder()
                  .verifyNativeQueries(xs -> false);
    }

    @Override
    protected JoinAlgorithm joinAlgorithm()
    {
      return JoinAlgorithm.SORT_MERGE;
    }

    @Override
    @MethodSource("provideQueryContexts")
    @ParameterizedTest(name = "{0}")
    public void testRegressionFilteredAggregatorsSubqueryJoins(Map<String, Object> queryContext)
    {
      assumeTrue(NullHandling.sqlCompatible(), "Returns incorrect result with replaceWithDefault");
      super.testRegressionFilteredAggregatorsSubqueryJoins(queryContext);
    }

    @Override
    @MethodSource("provideQueryContexts")
    @ParameterizedTest(name = "{0}")
    public void testFilterAndGroupByLookupUsingJoinOperatorWithNotFilter(Map<String, Object> queryContext)
    {
      assumeTrue(NullHandling.sqlCompatible(), "Returns incorrect result with replaceWithDefault");
      super.testFilterAndGroupByLookupUsingJoinOperatorWithNotFilter(queryContext);
    }
  }

  @SqlTestFrameworkConfig.ComponentSupplier(StandardMSQComponentSupplier.class)
  public abstract static class Base extends CalciteJoinQueryTest
  {
    protected abstract JoinAlgorithm joinAlgorithm();

    @Override
    protected QueryTestBuilder testBuilder()
    {
      Map<String, Object> defaultCtx = ImmutableMap.<String, Object>builder()
          .putAll(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
          .put(PlannerContext.CTX_SQL_JOIN_ALGORITHM, joinAlgorithm().toString())
          .build();
      return new QueryTestBuilder(new CalciteTestConfig(defaultCtx, true))
          .addCustomRunner(
              new ExtractResultsFactory(
                  () -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
          .skipVectorize(true);
    }
  }
}
