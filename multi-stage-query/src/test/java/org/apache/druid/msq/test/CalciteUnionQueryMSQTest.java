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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.CalciteUnionQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Runs {@link CalciteUnionQueryTest} but with MSQ engine
 */
@SqlTestFrameworkConfig.ComponentSupplier(StandardMSQComponentSupplier.class)
public class CalciteUnionQueryMSQTest extends CalciteUnionQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new BaseCalciteQueryTest.CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  /**
   * MSQ plans a top-level UNION ALL only as a table-level union, so the arms this query leaves as subqueries, which
   * the native engine runs one at a time, have nowhere to go. Same for the one below.
   */
  @Test
  @Override
  public void testUnionAllWithLookupArm()
  {
    assertQueryIsUnplannable(
        "SELECT k AS c FROM lookup.lookyloo WHERE k = 'abc'\n"
        + "UNION ALL\n"
        + "SELECT dim1 AS c FROM foo WHERE dim1 = 'abc'",
        "Query requires union with an input that reads from something other than a regular table, which is not supported."
    );
  }

  @Test
  @Override
  public void testUnionAllWithValuesArm()
  {
    assertQueryIsUnplannable(
        "SELECT c FROM (VALUES ('x'), ('y')) AS t(c)\n"
        + "UNION ALL\n"
        + "SELECT dim1 AS c FROM foo WHERE dim1 = 'abc'",
        "Query requires union with an input that reads from something other than a regular table, which is not supported."
    );
  }

  @Disabled("Ignored till MSQ can plan UNION ALL with any operand")
  @Test
  public void testUnionOnSubqueries()
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (\n"
        + "  (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "  UNION ALL\n"
        + "  (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(
                                                new UnionDataSource(
                                                    ImmutableList.of(
                                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                                        new TableDataSource(CalciteTests.DATASOURCE1)
                                                    )
                                                )
                                            )
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{12L, 4L}
        )
    );
  }

}
