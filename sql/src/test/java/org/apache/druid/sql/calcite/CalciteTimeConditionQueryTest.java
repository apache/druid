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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Tests for the {@link QueryContextParameters#SQL_TIME_CONDITION} context parameter.
 */
public class CalciteTimeConditionQueryTest extends BaseCalciteQueryTest
{
  // __time for foo is [2000-01-01, 2000-01-02, 2000-01-03, 2001-01-01, 2001-01-02, 2001-01-03]

  @Test
  public void testScan()
  {
    final Map<String, Object> context = makeContext("2000-01-02/2000-01-03, 2001-01-01/2001-01-02");

    testBuilder()
        .sql("SELECT __time, dim1 FROM foo")
        .queryContext(context)
        .expectedQueries(
            ImmutableList.of(
                newScanQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(
                        querySegmentSpec(
                            Intervals.of("2000-01-02/2000-01-03"),
                            Intervals.of("2001-01-01/2001-01-02")
                        )
                    )
                    .columns("__time", "dim1")
                    .columnTypes(ColumnType.LONG, ColumnType.STRING)
                    .context(context)
                    .build()
            )
        )
        // Unordered, since MSQ does not guarantee the order of scan results across segments.
        .expectedResults(
            new UnorderedResultsVerifier(
                ImmutableList.of(
                    new Object[]{timestamp("2000-01-02"), "10.1"},
                    new Object[]{timestamp("2001-01-01"), "1"}
                ),
                ResultMatchMode.EQUALS,
                null
            )
        )
        .run();
  }

  @Test
  public void testScanWithEmptyIntersection()
  {
    final Map<String, Object> context = makeContext("2000/2001");

    testQuery(
        "SELECT dim1 FROM foo WHERE __time >= TIMESTAMP '2001-01-01 00:00:00'",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec())
                .columns("dim1")
                .columnTypes(ColumnType.STRING)
                .context(context)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testTimeseriesWithTimeFilter()
  {
    final Map<String, Object> context = makeContext("2000/2001");

    testQuery(
        "SELECT COUNT(*) FROM foo WHERE __time >= TIMESTAMP '2000-01-02 00:00:00'",
        context,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(context)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testArrayIsInvalid()
  {
    testQueryThrows(
        "SELECT COUNT(*) FROM foo",
        makeContext(List.of("2000/2001")),
        DruidException.class,
        e -> Assertions.assertEquals(
            "Query context parameter [sqlTimeCondition] should be a string, but got [[2000/2001]]",
            e.getMessage()
        )
    );
  }

  @Test
  public void testTimeBoundary()
  {
    final Map<String, Object> context = ImmutableMap.<String, Object>builder()
                                                    .putAll(makeContext("2000/2001"))
                                                    .put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true)
                                                    .build();

    testQuery(
        "SELECT MAX(__time) FROM foo",
        context,
        ImmutableList.of(
            Druids.newTimeBoundaryQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2001")))
                  .bound(TimeBoundaryQuery.MAX_TIME)
                  .context(
                      ImmutableMap.<String, Object>builder()
                                  .putAll(context)
                                  .put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0")
                                  .build()
                  )
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-03")}
        )
    );
  }

  @NotYetSupported(Modes.DD_UNION)
  @Test
  public void testUnionDataSource()
  {
    final Map<String, Object> context = makeContext("2000-01-01/2000-01-03");

    testQuery(
        "SELECT dim1, COUNT(*) FROM (SELECT * FROM foo UNION ALL SELECT * FROM foo) GROUP BY dim1",
        context,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new UnionDataSource(
                                ImmutableList.of(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new TableDataSource(CalciteTests.DATASOURCE1)
                                )
                            )
                        )
                        .setInterval(querySegmentSpec(Intervals.of("2000-01-01/2000-01-03")))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(context)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2L},
            new Object[]{"10.1", 2L}
        )
    );
  }

  @Test
  public void testUnnest()
  {
    final Map<String, Object> context = makeContext("2000-01-01/2000-01-03");

    testQuery(
        "SELECT d3 FROM foo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested (d3)",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    UnnestDataSource.create(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        nestedExpressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                        null
                    )
                )
                .intervals(querySegmentSpec(Intervals.of("2000-01-01/2000-01-03")))
                .columns("j0.unnest")
                .columnTypes(ColumnType.STRING)
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"}
        )
    );
  }

  @Test
  public void testJoinWithLookup()
  {
    final Map<String, Object> context = makeContext("2001/2002");

    testQuery(
        "SELECT foo.dim1, lookyloo.v FROM foo INNER JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Intervals.of("2001/2002")))
                .columns("dim1", "j0.v")
                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"1", "xa"},
            new Object[]{"def", "xabc"}
        )
    );
  }

  @Test
  public void testJoinWithTableSubquery()
  {
    final Map<String, Object> context = makeContext("2000/2001");

    testQuery(
        "SELECT f1.dim1, f2.dim2 FROM foo f1 INNER JOIN foo f2 ON f1.dim1 = f2.dim1",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Intervals.of("2000/2001")))
                                .columns("dim1", "dim2")
                                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                                .context(context)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.dim1")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Intervals.of("2000/2001")))
                .columns("dim1", "j0.dim2")
                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a"},
            new Object[]{"10.1", null},
            new Object[]{"2", ""}
        )
    );
  }

  @Test
  public void testJoinWithBroadcastTable()
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    final Map<String, Object> context = makeContext("2000-01-01/2001-01-02");

    // The broadcast table on the right-hand side is joined directly, so the time condition does not apply to it.
    testQuery(
        "SELECT broadcast.dim4, COUNT(*)\n"
        + "FROM numfoo INNER JOIN broadcast ON numfoo.dim4 = broadcast.dim4\n"
        + "GROUP BY 1",
        context,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE3),
                                new GlobalTableDataSource(CalciteTests.BROADCAST_DATASOURCE),
                                "j0.",
                                equalsCondition(makeColumnExpression("dim4"), makeColumnExpression("j0.dim4")),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Intervals.of("2000-01-01/2001-01-02")))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.dim4", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(context)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 9L},
            new Object[]{"b", 3L}
        )
    );
  }

  @Test
  public void testJoinWithSubqueryOnLeft()
  {
    final Map<String, Object> context = makeContext("2000-01-01/2000-01-02,2000-01-03/2001");

    // Time condition applies to the subquery on the left-hand side, but not the outer query, which reads the join.
    testQuery(
        "SELECT base.dim2, lookyloo.v, base.cnt\n"
        + "FROM (SELECT dim2, COUNT(*) AS cnt FROM foo GROUP BY dim2) base\n"
        + "LEFT JOIN lookup.lookyloo ON base.dim2 = lookyloo.k",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(
                                            querySegmentSpec(
                                                Intervals.of("2000-01-01/2000-01-02"),
                                                Intervals.of("2000-01-03/2001")
                                            )
                                        )
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                        .setContext(context)
                                        .build()
                        ),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(makeColumnExpression("d0"), makeColumnExpression("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("d0", "j0.v", "a0")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.LONG)
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", null, 1L},
            new Object[]{"a", "xa", 1L}
        )
    );
  }

  @Test
  public void testJoinWithNonTimeOrderBy()
  {
    // Cannot vectorize direct queries on lookup tables.
    cannotVectorize();

    final Map<String, Object> context = makeContext("2001/2002");

    testBuilder()
        .sql(
            "SELECT f1.dim1, f2.v\n"
            + "FROM foo f1 JOIN (SELECT k, v FROM lookup.lookyloo GROUP BY k, v) f2 ON f1.dim2 = f2.k\n"
            + "ORDER BY f1.dim1"
        )
        .queryContext(context)
        .expectedResults(
            ImmutableList.of(
                new Object[]{"1", "xa"},
                new Object[]{"def", "xabc"}
            )
        )
        .run();
  }

  @NotYetSupported(Modes.DD_WINDOW)
  @Test
  public void testWindowOverJoin()
  {
    // Cannot vectorize direct queries on lookup tables.
    cannotVectorize();

    final Map<String, Object> context = makeContext("2000-01-01/2000-01-02");

    testBuilder()
        .sql(
            "SELECT f1.__time, f1.dim2, COUNT(*) OVER () AS c\n"
            + "FROM foo f1 JOIN (SELECT k FROM lookup.lookyloo GROUP BY k) f2 ON f1.dim2 = f2.k"
        )
        .queryContext(context)
        .expectedResults(
            ImmutableList.of(
                new Object[]{timestamp("2000-01-01"), "a", 1L}
            )
        )
        .run();
  }

  @Test
  public void testSubquery()
  {
    final Map<String, Object> context = makeContext("2000/2001");

    // Time condition applies to the inner query, which is on a table, but not the outer query.
    testQuery(
        "SELECT SUM(cnt), COUNT(*) FROM (SELECT dim2, SUM(cnt) AS cnt FROM foo GROUP BY dim2)",
        context,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Intervals.of("2000/2001")))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(context)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            aggregators(
                                new LongSumAggregatorFactory("_a0", "a0"),
                                new CountAggregatorFactory("_a1")
                            )
                        )
                        .setContext(context)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 3L}
        )
    );
  }

  @Test
  public void testLookupIsUnaffected()
  {
    // Cannot vectorize direct queries on lookup tables.
    cannotVectorize();

    final Map<String, Object> context = makeContext("2000/2001");

    testQuery(
        "SELECT COUNT(*) FROM lookup.lookyloo",
        context,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(new LookupDataSource("lookyloo"))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(context)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testRestrictedTable_superUser()
  {
    final Map<String, Object> context = makeContext("2001-01-01/2001-01-03");

    testBuilder()
        .sql("SELECT COUNT(*) FROM druid.restrictedDatasource_m1_is_6")
        .queryContext(context)
        .authResult(CalciteTests.SUPER_USER_AUTH_RESULT)
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(
                          RestrictedDataSource.create(
                              TableDataSource.create(CalciteTests.RESTRICTED_DATASOURCE),
                              CalciteTests.POLICY_NO_RESTRICTION_SUPERUSER
                          )
                      )
                      .intervals(querySegmentSpec(Intervals.of("2001-01-01/2001-01-03")))
                      .granularity(Granularities.ALL)
                      .aggregators(aggregators(new CountAggregatorFactory("a0")))
                      .context(context)
                      .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[]{2L}))
        .run();
  }

  @NotYetSupported(Modes.DD_RESTRICTED_DATASOURCE_SUPPORT)
  @Test
  public void testRestrictedTable_regularUser()
  {
    final Map<String, Object> context = makeContext("2001-01-01/2001-01-03");

    // The policy only admits the row with m1 = 6, at 2001-01-03, which is outside the time condition.
    testBuilder()
        .sql("SELECT COUNT(*) FROM druid.restrictedDatasource_m1_is_6")
        .queryContext(context)
        .authResult(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .expectedQueries(
            ImmutableList.of(
                Druids.newTimeseriesQueryBuilder()
                      .dataSource(
                          RestrictedDataSource.create(
                              TableDataSource.create(CalciteTests.RESTRICTED_DATASOURCE),
                              CalciteTests.POLICY_RESTRICTION
                          )
                      )
                      .intervals(querySegmentSpec(Intervals.of("2001-01-01/2001-01-03")))
                      .granularity(Granularities.ALL)
                      .aggregators(aggregators(new CountAggregatorFactory("a0")))
                      .context(context)
                      .build()
            )
        )
        .expectedResults(ImmutableList.of(new Object[]{0L}))
        .run();
  }

  @Test
  public void testInvalidTimeCondition()
  {
    testQueryThrows(
        "SELECT COUNT(*) FROM foo",
        makeContext("2000/2001,foo"),
        DruidException.class,
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Query context parameter [sqlTimeCondition] must be a comma-separated string of ISO8601 intervals, "
            + "but got [2000/2001,foo]"
        )
    );
  }

  protected static Map<String, Object> makeContext(final Object timeCondition)
  {
    return ImmutableMap.<String, Object>builder()
                       .putAll(QUERY_CONTEXT_DEFAULT)
                       .put(QueryContextParameters.SQL_TIME_CONDITION.getName(), timeCondition)
                       .build();
  }
}
