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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.sql.calcite.CalciteTimeConditionQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.junit.jupiter.api.Test;

import java.util.Map;

/**
 * Runs {@link CalciteTimeConditionQueryTest} with the MSQ engine.
 */
@SqlTestFrameworkConfig.ComponentSupplier(StandardMSQComponentSupplier.class)
public class CalciteTimeConditionQueryMSQTest extends CalciteTimeConditionQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(
            new ExtractResultsFactory(
                () -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()
            )
        )
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Test
  public void testSortMergeJoin()
  {
    final Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                                    .putAll(makeContext("2000/2001"))
                                                    .put(
                                                        PlannerContext.CTX_SQL_JOIN_ALGORITHM,
                                                        JoinAlgorithm.SORT_MERGE.toString()
                                                    )
                                                    .build();

    // Both sides of a sort-merge join are subqueries, so the time condition applies within each subquery.
    testBuilder()
        .sql("SELECT f1.dim1, f2.dim2 FROM foo f1 INNER JOIN foo f2 ON f1.dim1 = f2.dim1")
        .queryContext(context)
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(
                    new Object[]{"", "a"},
                    new Object[]{"10.1", null},
                    new Object[]{"2", ""}
                ),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }
}
