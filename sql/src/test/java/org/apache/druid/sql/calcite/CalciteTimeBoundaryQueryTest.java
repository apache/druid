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

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

public class CalciteTimeBoundaryQueryTest extends BaseCalciteQueryTest
{
  // __time for foo is [2000-01-01, 2000-01-02, 2000-01-03, 2001-01-01, 2001-01-02, 2001-01-03]
  @Test
  public void testMaxTimeQuery()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MAX(__time) AS maxTime FROM foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .bound(TimeBoundaryQuery.MAX_TIME)
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2001-01-03").getMillis()})
    );
  }

  @Test
  public void testMinTimeQuery()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MIN(__time) AS minTime FROM foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .bound(TimeBoundaryQuery.MIN_TIME)
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2000-01-01").getMillis()})
    );
  }

  @Test
  public void testMinTimeQueryWithTimeFilters()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MIN(__time) AS minTime FROM foo where __time >= '2001-01-01' and __time < '2003-01-01'",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .intervals(
                      new MultipleIntervalSegmentSpec(
                          ImmutableList.of(Intervals.of("2001-01-01T00:00:00.000Z/2003-01-01T00:00:00.000Z"))
                      )
                  )
                  .bound(TimeBoundaryQuery.MIN_TIME)
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2001-01-01").getMillis()})
    );
  }

  @Test
  public void testMinTimeQueryWithTimeAndColumnFilters()
  {
    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    HashMap<String, Object> expectedContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    expectedContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT MIN(__time) AS minTime FROM foo\n"
        + "where __time >= '2001-01-01' and __time < '2003-01-01'\n"
        + "and dim2 = 'abc'",
        queryContext,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource("foo")
                  .intervals(
                      new MultipleIntervalSegmentSpec(
                          ImmutableList.of(Intervals.of("2001-01-01T00:00:00.000Z/2003-01-01T00:00:00.000Z"))
                      )
                  )
                  .bound(TimeBoundaryQuery.MIN_TIME)
                  .filters(equality("dim2", "abc", ColumnType.STRING))
                  .context(expectedContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2001-01-02").getMillis()})
    );
  }

  @Test
  public void testMinTimeQueryWithTimeAndExpressionFilters()
  {
    // Cannot vectorize due to UPPER expression.
    cannotVectorize();

    HashMap<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    testQuery(
        "SELECT MIN(__time) AS minTime FROM foo\n"
        + "where __time >= '2001-01-01' and __time < '2003-01-01'\n"
        + "and upper(dim2) = 'ABC'",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("foo")
                  .intervals(
                      new MultipleIntervalSegmentSpec(
                          ImmutableList.of(Intervals.of("2001-01-01T00:00:00.000Z/2003-01-01T00:00:00.000Z"))
                      )
                  )
                  .virtualColumns(expressionVirtualColumn("v0", "upper(\"dim2\")", ColumnType.STRING))
                  .filters(equality("v0", "ABC", ColumnType.STRING))
                  .aggregators(new LongMinAggregatorFactory("a0", "__time"))
                  .context(queryContext)
                  .build()
        ),
        ImmutableList.of(new Object[]{DateTimes.of("2001-01-02").getMillis()})
    );
  }

  // Currently, if both min(__time) and max(__time) are present, we don't convert it
  // to a timeBoundary query. (ref : https://github.com/apache/druid/issues/12479)
  @Test
  public void testMinMaxTimeQuery()
  {
    HashMap<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    testQuery(
        "SELECT MIN(__time) AS minTime, MAX(__time) as maxTime FROM foo",
        context,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource("foo")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .aggregators(
                      new LongMinAggregatorFactory("a0", "__time"),
                      new LongMaxAggregatorFactory("a1", "__time")
                  )
                  .context(context)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            DateTimes.of("2000-01-01").getMillis(),
            DateTimes.of("2001-01-03").getMillis()
        })
    );
  }

  @Test
  public void testMaxTimeQueryWithJoin()
  {
    // Cannot vectorize due to JOIN.
    cannotVectorize();

    HashMap<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);

    testBuilder()
        .sql("SELECT MAX(t1.__time)\n"
             + "FROM foo t1\n"
             + "INNER JOIN foo t2 ON CAST(t1.m1 AS BIGINT) = t2.cnt\n")
        .queryContext(context)
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeBoundaryQueryBuilder()
                      .dataSource(
                          join(
                              new TableDataSource(CalciteTests.DATASOURCE1),
                              new QueryDataSource(
                                  newScanQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .columns("cnt")
                                      .context(context)
                                      .build()
                              ),
                              "j0.",
                              equalsCondition(makeExpression("CAST(\"m1\", 'LONG')"), makeColumnExpression("j0.cnt")),
                              JoinType.INNER
                          )

                      )
                      .bound(TimeBoundaryQuery.MAX_TIME)
                      .context(context)
                      .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[]{946684800000L}))
        .run();
  }
}
