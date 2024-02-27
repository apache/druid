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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.SingleValueAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;


/**
 * Calcite tests which involve subqueries and materializing the intermediate results on {@link org.apache.druid.server.ClientQuerySegmentWalker}
 * The tests are run with two different codepaths:
 * 1. Where the memory limit is not set. The intermediate results are materialized as inline rows
 * 2. Where the memory limit is set. The intermediate results are materialized as frames
 */
@RunWith(Parameterized.class)
public class CalciteSubqueryTest extends BaseCalciteQueryTest
{

  public String testName;
  public Map<String, Object> queryContext;

  public CalciteSubqueryTest(
      String testName,
      Map<String, Object> queryContext
  )
  {
    this.testName = testName;
    this.queryContext = queryContext;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    constructors.add(
        new Object[]{"without memory limit", QUERY_CONTEXT_DEFAULT}
    );
    constructors.add(
        new Object[]{"with memory limit", QUERY_CONTEXT_WITH_SUBQUERY_MEMORY_LIMIT}
    );
    return constructors;
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWhereToOuterFilter()
  {
    // Cannot vectorize topN operator.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 1)\n"
        + "WHERE cnt > 0",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                new TopNQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .granularity(Granularities.ALL)
                                    .dimension(new DefaultDimensionSpec("dim2", "d0"))
                                    .aggregators(new LongSumAggregatorFactory("a0", "cnt"))
                                    .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                    .threshold(1)
                                    .build()
                            )
                        )
                        .setDimFilter(range("a0", ColumnType.LONG, 0L, null, true, false))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(queryContext)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{3L, 1L}
        ) :
        ImmutableList.of(
            new Object[]{2L, 1L}
        )
    );
  }

  @Test
  public void testExactCountDistinctOfSemiJoinResult()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + "  ) AND __time >= '2000-01-01' AND __time < '2002-01-01'\n"
        + ")",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(
                                                join(
                                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                                    new QueryDataSource(
                                                        GroupByQuery
                                                            .builder()
                                                            .setDataSource(CalciteTests.DATASOURCE1)
                                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                                            .setGranularity(Granularities.ALL)
                                                            .setDimFilter(not(equality("dim1", "", ColumnType.STRING)))
                                                            .setDimensions(
                                                                dimensions(
                                                                    new ExtractionDimensionSpec(
                                                                        "dim1",
                                                                        "d0",
                                                                        new SubstringDimExtractionFn(0, 1)
                                                                    )
                                                                )
                                                            )
                                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                                            .build()
                                                    ),
                                                    "j0.",
                                                    equalsCondition(
                                                        DruidExpression.ofExpression(
                                                            ColumnType.STRING,
                                                            null,
                                                            args -> "substring(\"dim2\", 0, 1)",
                                                            Collections.emptyList()
                                                        ),
                                                        DruidExpression.ofColumn(ColumnType.STRING, "j0.d0")
                                                    ),
                                                    JoinType.INNER
                                                )
                                            )
                                            .setInterval(querySegmentSpec(Intervals.of("2000-01-01/2002-01-01")))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testTwoExactCountDistincts()
  {
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        queryContext,
        "SELECT COUNT(distinct dim1), COUNT(distinct dim2) FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(
                                    GroupByQuery
                                        .builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                )
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setAggregatorSpecs(
                                    new FilteredAggregatorFactory(
                                        new CountAggregatorFactory("a0"),
                                        notNull("d0")
                                    )
                                )
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(
                                    GroupByQuery
                                        .builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                )
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setAggregatorSpecs(
                                    new FilteredAggregatorFactory(
                                        new CountAggregatorFactory("a0"),
                                        notNull("d0")
                                    )
                                )
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        "1",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("a0", "j0.a0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.sqlCompatible() ? 6L : 5L, NullHandling.sqlCompatible() ? 3L : 2L}
        )
    );
  }

  @Test
  public void testViewAndJoin()
  {
    cannotVectorize();
    Map<String, Object> queryContextModified = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "SELECT COUNT(*) FROM view.cview as a INNER JOIN druid.foo d on d.dim2 = a.dim2 WHERE a.dim1_firstchar <> 'z' ",
        queryContextModified,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              new TableDataSource(CalciteTests.DATASOURCE1),
                              new QueryDataSource(
                                  newScanQueryBuilder().dataSource(CalciteTests.DATASOURCE3)
                                                       .intervals(querySegmentSpec(Filtration.eternity()))
                                                       .columns("dim2")
                                                       .context(queryContextModified)
                                                       .build()
                              ),
                              "j0.",
                              "(\"dim2\" == \"j0.dim2\")",
                              JoinType.INNER,
                              range("dim2", ColumnType.STRING, "a", "a", false, false)
                          ),
                          new QueryDataSource(
                              newScanQueryBuilder().dataSource(CalciteTests.DATASOURCE1)
                                                   .intervals(querySegmentSpec(Filtration.eternity()))
                                                   .columns("dim2")
                                                   .context(queryContextModified)
                                                   .build()
                          ),
                          "_j0.",
                          "('a' == \"_j0.dim2\")",
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      NullHandling.replaceWithDefault()
                      ? VirtualColumns.EMPTY
                      : VirtualColumns.create(
                          expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING)
                      )
                  )
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? not(selector("dim1", "z", new SubstringDimExtractionFn(0, 1)))
                      : not(equality("v0", "z", ColumnType.STRING))
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(queryContextModified)
                  .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{8L}
        )
        // in sql compatible mode, expression filter correctly does not match null values...
        : ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testGroupByWithPostAggregatorReferencingTimeFloorColumnOnTimeseries()
  {
    cannotVectorize();

    testQuery(
        "SELECT TIME_FORMAT(\"date\", 'yyyy-MM'), SUM(x)\n"
        + "FROM (\n"
        + "    SELECT\n"
        + "        FLOOR(__time to hour) as \"date\",\n"
        + "        COUNT(*) as x\n"
        + "    FROM foo\n"
        + "    GROUP BY 1\n"
        + ")\n"
        + "GROUP BY 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .granularity(Granularities.HOUR)
                                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                                  .build()
                        )
                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_format(\"d0\",'yyyy-MM','UTC')",
                                ColumnType.STRING
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .addDimension(new DefaultDimensionSpec("v0", "_d0"))
                        .addAggregator(new LongSumAggregatorFactory("_a0", "a0"))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2000-01", 3L},
            new Object[]{"2001-01", 3L}
        )
    );
  }

  @Test
  public void testUsingSubqueryAsFilterWithInnerSort()
  {
    // Regression test for https://github.com/apache/druid/issues/4208

    testQuery(
        "SELECT dim1, dim2 FROM druid.foo\n"
        + " WHERE dim2 IN (\n"
        + "   SELECT dim2\n"
        + "   FROM druid.foo\n"
        + "   GROUP BY dim2\n"
        + "   ORDER BY dim2 DESC\n"
        + " )",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.ofColumn(ColumnType.STRING, "dim2"), DruidExpression.ofColumn(ColumnType.STRING, "j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", "a"},
            new Object[]{"1", "a"},
            new Object[]{"def", "abc"}
        ) :
        ImmutableList.of(
            new Object[]{"", "a"},
            new Object[]{"2", ""},
            new Object[]{"1", "a"},
            new Object[]{"def", "abc"}
        )
    );
  }

  @Test
  public void testUsingSubqueryAsFilterOnTwoColumns()
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2 FROM druid.foo "
        + " WHERE (dim1, dim2) IN ("
        + "   SELECT dim1, dim2 FROM ("
        + "     SELECT dim1, dim2, COUNT(*)"
        + "     FROM druid.foo"
        + "     WHERE dim2 = 'abc'"
        + "     GROUP BY dim1, dim2"
        + "     HAVING COUNT(*) = 1"
        + "   )"
        + " )",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimFilter(equality("dim2", "abc", ColumnType.STRING))
                                        .setDimensions(dimensions(
                                            new DefaultDimensionSpec("dim1", "d0"),
                                            new DefaultDimensionSpec("dim2", "d1")
                                        ))
                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                        .setPostAggregatorSpecs(
                                            expressionPostAgg("p0", "'abc'", ColumnType.STRING)
                                        )
                                        .setHavingSpec(having(equality("a0", 1L, ColumnType.LONG)))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ),
                        "j0.",
                        StringUtils.format(
                            "(%s && %s)",
                            equalsCondition(DruidExpression.ofColumn(ColumnType.STRING, "dim1"), DruidExpression.ofColumn(ColumnType.STRING, "j0.d0")),
                            equalsCondition(DruidExpression.ofColumn(ColumnType.STRING, "dim2"), DruidExpression.ofColumn(ColumnType.STRING, "j0.p0"))
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "'abc'", ColumnType.STRING))
                .columns("__time", "cnt", "dim1", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc"}
        )
    );
  }

  @Test
  public void testMinMaxAvgDailyCountWithLimit()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT * FROM ("
        + "  SELECT max(cnt), min(cnt), avg(cnt), TIME_EXTRACT(max(t), 'EPOCH') last_time, count(1) num_days FROM (\n"
        + "      SELECT TIME_FLOOR(__time, 'P1D') AS t, count(1) cnt\n"
        + "      FROM \"foo\"\n"
        + "      GROUP BY 1\n"
        + "  )"
        + ") LIMIT 1\n",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .aggregators(new CountAggregatorFactory("a0"))
                                      .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                                      .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault ?
                            aggregators(
                                new LongMaxAggregatorFactory("_a0", "a0"),
                                new LongMinAggregatorFactory("_a1", "a0"),
                                new DoubleSumAggregatorFactory("_a2:sum", "a0"),
                                new CountAggregatorFactory("_a2:count"),
                                new LongMaxAggregatorFactory("_a3", "d0"),
                                new CountAggregatorFactory("_a4")
                            ) : aggregators(
                                new LongMaxAggregatorFactory("_a0", "a0"),
                                new LongMinAggregatorFactory("_a1", "a0"),
                                new DoubleSumAggregatorFactory("_a2:sum", "a0"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a2:count"),
                                    notNull("a0")
                                ),
                                new LongMaxAggregatorFactory("_a3", "d0"),
                                new CountAggregatorFactory("_a4")
                            )
                        )
                        .setPostAggregatorSpecs(
                            new ArithmeticPostAggregator(
                                "_a2",
                                "quotient",
                                ImmutableList.of(
                                    new FieldAccessPostAggregator(null, "_a2:sum"),
                                    new FieldAccessPostAggregator(null, "_a2:count")
                                )
                            ),
                            expressionPostAgg("p0", "timestamp_extract(\"_a3\",'EPOCH','UTC')", ColumnType.LONG)
                        )
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(new Object[]{1L, 1L, 1.0, 978480000L, 6L})
    );
  }

  @Test
  public void testEmptyGroupWithOffsetDoesntInfiniteLoop()
  {
    testQuery(
        "SELECT r0.c, r1.c\n"
        + "FROM (\n"
        + "  SELECT COUNT(*) AS c\n"
        + "  FROM \"foo\"\n"
        + "  GROUP BY ()\n"
        + "  OFFSET 1\n"
        + ") AS r0\n"
        + "LEFT JOIN (\n"
        + "  SELECT COUNT(*) AS c\n"
        + "  FROM \"foo\"\n"
        + "  GROUP BY ()\n"
        + ") AS r1 ON TRUE LIMIT 10",
        queryContext,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      join(
                          new QueryDataSource(
                              GroupByQuery.builder()
                                          .setDataSource(CalciteTests.DATASOURCE1)
                                          .setInterval(querySegmentSpec(Filtration.eternity()))
                                          .setGranularity(Granularities.ALL)
                                          .setAggregatorSpecs(
                                              aggregators(
                                                  new CountAggregatorFactory("a0")
                                              )
                                          )
                                          .setLimitSpec(DefaultLimitSpec.builder().offset(1).limit(10).build())
                                          .setContext(QUERY_CONTEXT_DEFAULT)
                                          .build()
                          ),
                          new QueryDataSource(
                              Druids.newTimeseriesQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .granularity(Granularities.ALL)
                                    .aggregators(new CountAggregatorFactory("a0"))
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                          ),
                          "j0.",
                          "1",
                          JoinType.LEFT,
                          null
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("a0", "j0.a0")
                  .limit(10)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of()
    );
  }


  @Test
  public void testMaxSubqueryRows()
  {
    if ("without memory limit".equals(testName)) {
      expectedException.expect(ResourceLimitExceededException.class);
      expectedException.expectMessage("Subquery generated results beyond maximum[1]");
      Map<String, Object> modifiedQueryContext = new HashMap<>(queryContext);
      modifiedQueryContext.put(QueryContexts.MAX_SUBQUERY_ROWS_KEY, 1);

      testQuery(
          "SELECT\n"
          + "  SUM(cnt),\n"
          + "  COUNT(*)\n"
          + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 2) \n"
          + "WHERE cnt > 0",
          modifiedQueryContext,
          ImmutableList.of(),
          ImmutableList.of()
      );
    } else {
      // Since the results are materializable as frames, we are able to use the memory limit and donot rely on the
      // row limit for the subquery
      Map<String, Object> modifiedQueryContext = new HashMap<>(queryContext);
      modifiedQueryContext.put(QueryContexts.MAX_SUBQUERY_ROWS_KEY, 1);

      testQuery(
          "SELECT\n"
          + "  SUM(cnt),\n"
          + "  COUNT(*)\n"
          + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 1)\n"
          + "WHERE cnt > 0",
          modifiedQueryContext,
          ImmutableList.of(
              GroupByQuery.builder()
                          .setDataSource(
                              new QueryDataSource(
                                  new TopNQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .granularity(Granularities.ALL)
                                      .dimension(new DefaultDimensionSpec("dim2", "d0"))
                                      .aggregators(new LongSumAggregatorFactory("a0", "cnt"))
                                      .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                      .threshold(1)
                                      .build()
                              )
                          )
                          .setDimFilter(range("a0", ColumnType.LONG, 0L, null, true, false))
                          .setInterval(querySegmentSpec(Filtration.eternity()))
                          .setGranularity(Granularities.ALL)
                          .setAggregatorSpecs(aggregators(
                              new LongSumAggregatorFactory("_a0", "a0"),
                              new CountAggregatorFactory("_a1")
                          ))
                          .setContext(queryContext)
                          .build()
          ),
          NullHandling.replaceWithDefault() ?
          ImmutableList.of(
              new Object[]{3L, 1L}
          ) :
          ImmutableList.of(
              new Object[]{2L, 1L}
          )
      );
    }
  }

  @Test
  public void testZeroMaxNumericInFilter()
  {
    expectedException.expect(UOE.class);
    expectedException.expectMessage("[maxNumericInFilters] must be greater than 0");

    Map<String, Object> modifiedQueryContext = new HashMap<>(queryContext);
    modifiedQueryContext.put(QueryContexts.MAX_NUMERIC_IN_FILTERS, 0);


    testQuery(
        PLANNER_CONFIG_DEFAULT,
        modifiedQueryContext,
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE dim6 IN (\n"
        + "1,2,3\n"
        + ")\n",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testUseTimeFloorInsteadOfGranularityOnJoinResult()
  {
    cannotVectorize();

    testQuery(
        "WITH main AS (SELECT * FROM foo LIMIT 2)\n"
        + "SELECT TIME_FLOOR(__time, 'PT1H') AS \"time\", dim1, COUNT(*)\n"
        + "FROM main\n"
        + "WHERE dim1 IN (SELECT dim1 FROM main GROUP BY 1 ORDER BY COUNT(*) DESC LIMIT 5)\n"
        + "GROUP BY 1, 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Intervals.ETERNITY))
                                        .columns("__time", "dim1")
                                        .limit(2)
                                        .build()
                                ),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(
                                                    new QueryDataSource(
                                                        newScanQueryBuilder()
                                                            .dataSource(CalciteTests.DATASOURCE1)
                                                            .intervals(querySegmentSpec(Intervals.ETERNITY))
                                                            .columns("dim1")
                                                            .limit(2)
                                                            .build()
                                                    )
                                                )
                                                .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                                .setGranularity(Granularities.ALL)
                                                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                                .setLimitSpec(
                                                    new DefaultLimitSpec(
                                                        ImmutableList.of(
                                                            new OrderByColumnSpec(
                                                                "a0",
                                                                OrderByColumnSpec.Direction.DESCENDING,
                                                                StringComparators.NUMERIC
                                                            )
                                                        ),
                                                        5
                                                    )
                                                )
                                                .build()
                                ),
                                "j0.",
                                "(\"dim1\" == \"j0.d0\")",
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'PT1H',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("dim1", "d1")
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{946684800000L, "", 1L}, new Object[]{946771200000L, "10.1", 1L})
        : ImmutableList.of(new Object[]{946771200000L, "10.1", 1L})
    );
  }

  @Test
  public void testJoinWithTimeDimension()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        queryContext,
        "SELECT count(*) FROM druid.foo t1 inner join druid.foo t2 on t1.__time = t2.__time",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(JoinDataSource.create(
                                   new TableDataSource(CalciteTests.DATASOURCE1),
                                   new QueryDataSource(
                                       Druids.newScanQueryBuilder()
                                             .dataSource(CalciteTests.DATASOURCE1)
                                             .intervals(querySegmentSpec(Filtration.eternity()))
                                             .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                             .columns("__time")
                                             .legacy(false)
                                             .context(queryContext)
                                             .build()),
                                   "j0.",
                                   "(\"__time\" == \"j0.__time\")",
                                   JoinType.INNER,
                                   null,
                                   ExprMacroTable.nil(),
                                   CalciteTests.createJoinableFactoryWrapper()
                               ))
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(new CountAggregatorFactory("a0")))
                               .context(queryContext)
                               .build()),
        ImmutableList.of(new Object[]{6L})
    );
  }

  @Test
  public void testUsingSubqueryWithLimit()
  {
    // Cannot vectorize scan query.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) AS cnt FROM ( SELECT * FROM druid.foo LIMIT 10 ) tmpA",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(expressionVirtualColumn("v0", "0", ColumnType.LONG))
                                .columns("v0")
                                .limit(10)
                                .context(queryContext)
                                .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testSelfJoin()
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo x, druid.foo y\n",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new QueryDataSource(
                              newScanQueryBuilder()
                                  .dataSource(CalciteTests.DATASOURCE1)
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .columns(
                                      ImmutableList.of(
                                          "__time",
                                          "cnt",
                                          "dim1",
                                          "dim2",
                                          "dim3",
                                          "m1",
                                          "m2",
                                          "unique_dim1"
                                      )
                                  )
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .context(queryContext)
                                  .build()
                          ),
                          "j0.",
                          "1",
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(queryContext)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{36L}
        )
    );
  }

  @Test
  public void testJoinWithSubqueries()
  {
    cannotVectorize();

    List<Object[]> results = new ArrayList<>(ImmutableList.of(
        new Object[]{"", NullHandling.defaultStringValue()},
        new Object[]{"10.1", NullHandling.defaultStringValue()},
        new Object[]{"2", NullHandling.defaultStringValue()},
        new Object[]{"1", NullHandling.defaultStringValue()},
        new Object[]{"def", NullHandling.defaultStringValue()},
        new Object[]{"abc", NullHandling.defaultStringValue()}
    ));

    if (NullHandling.replaceWithDefault()) {
      results.add(new Object[]{NullHandling.defaultStringValue(), NullHandling.defaultStringValue()});
    }


    testQuery(
        "SELECT a.dim1, b.dim2\n"
        + "FROM (SELECT na.dim1 as dim1, nb.dim2 as dim2 FROM foo na LEFT JOIN foo2 nb ON na.dim1 = nb.dim1) a\n"
        + "FULL OUTER JOIN\n"
        + "(SELECT nc.dim1 as dim1, nd.dim2 as dim2 FROM foo nc LEFT JOIN foo2 nd ON nc.dim1 = nd.dim1) b\n"
        + "ON a.dim1 = b.dim1",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    JoinDataSource.create(
                        JoinDataSource.create(
                            new TableDataSource("foo"),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource("foo2")
                                    .columns("dim1")
                                    .eternityInterval()
                                    .build()
                            ),
                            "j0.",
                            "(\"dim1\" == \"j0.dim1\")",
                            JoinType.LEFT,
                            null,
                            ExprMacroTable.nil(),
                            CalciteTests.createJoinableFactoryWrapper()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(
                                    JoinDataSource.create(
                                        new TableDataSource("foo"),
                                        new QueryDataSource(
                                            newScanQueryBuilder()
                                                .dataSource("foo2")
                                                .columns("dim1", "dim2")
                                                .eternityInterval()
                                                .build()
                                        ),
                                        "j0.",
                                        "(\"dim1\" == \"j0.dim1\")",
                                        JoinType.LEFT,
                                        null,
                                        ExprMacroTable.nil(),
                                        CalciteTests.createJoinableFactoryWrapper()
                                    )
                                )
                                .columns("dim1", "j0.dim2")
                                .eternityInterval()
                                .build()
                        ),
                        "_j0.",
                        "(\"dim1\" == \"_j0.dim1\")",
                        JoinType.FULL,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
                    )
                )
                .columns("_j0.j0.dim2", "dim1")
                .eternityInterval()
                .build()
        ),
        results
    );
  }

  @Test
  public void testSingleValueFloatAgg()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) FROM foo where m1 <= (select min(m1) + 4 from foo)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.DATASOURCE1),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.DATASOURCE1)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new FloatMinAggregatorFactory("a0", "m1"))
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "(\"a0\" + 4)",
                                                                             ColumnType.FLOAT
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatorFactory(
                                                                  "_a0",
                                                                  "v0",
                                                                  ColumnType.FLOAT
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "1",
                      NullHandling.replaceWithDefault() ? JoinType.LEFT : JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(expressionFilter("(\"m1\" <= \"j0._a0\")"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testSingleValueDoubleAgg()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) FROM foo where m1 >= (select max(m1) - 3.5 from foo)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.DATASOURCE1),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.DATASOURCE1)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new FloatMaxAggregatorFactory("a0", "m1"))
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "(\"a0\" - 3.5)",
                                                                             ColumnType.DOUBLE
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatorFactory(
                                                                  "_a0",
                                                                  "v0",
                                                                  ColumnType.DOUBLE
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "1",
                      NullHandling.replaceWithDefault() ? JoinType.LEFT : JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(expressionFilter("(\"m1\" >= \"j0._a0\")"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testSingleValueLongAgg()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) FROM wikipedia where __time >= (select max(__time) - INTERVAL '10' MINUTE from wikipedia)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.WIKIPEDIA),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newTimeseriesQueryBuilder()
                                                                .dataSource(CalciteTests.WIKIPEDIA)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .granularity(Granularities.ALL)
                                                                .aggregators(new LongMaxAggregatorFactory(
                                                                    "a0",
                                                                    "__time"
                                                                ))
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "(\"a0\" - 600000)",
                                                                             ColumnType.LONG
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatorFactory(
                                                                  "_a0",
                                                                  "v0",
                                                                  ColumnType.LONG
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "1",
                      NullHandling.replaceWithDefault() ? JoinType.LEFT : JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(expressionFilter("(\"__time\" >= \"j0._a0\")"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{220L}
        )
    );
  }

  @Test
  public void testSingleValueStringAgg()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT  count(*) FROM wikipedia where channel = (select channel from wikipedia order by __time desc LIMIT 1 OFFSET 6)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(join(
                      new TableDataSource(CalciteTests.WIKIPEDIA),
                      new QueryDataSource(GroupByQuery.builder()
                                                      .setDataSource(new QueryDataSource(
                                                          Druids.newScanQueryBuilder()
                                                                .dataSource(CalciteTests.WIKIPEDIA)
                                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                                .offset(6L)
                                                                .limit(1L)
                                                                .order(ScanQuery.Order.DESCENDING)
                                                                .columns("__time", "channel")
                                                                .legacy(false)
                                                                .context(QUERY_CONTEXT_DEFAULT)
                                                                .build()
                                                      ))
                                                      .setInterval(querySegmentSpec(Filtration.eternity()))
                                                      .setGranularity(Granularities.ALL)
                                                      .setVirtualColumns(expressionVirtualColumn(
                                                                             "v0",
                                                                             "\"channel\"",
                                                                             ColumnType.STRING
                                                                         )
                                                      )
                                                      .setAggregatorSpecs(
                                                          aggregators(
                                                              new SingleValueAggregatorFactory(
                                                                  "a0",
                                                                  "v0",
                                                                  ColumnType.STRING
                                                              )
                                                          )
                                                      )
                                                      .setLimitSpec(NoopLimitSpec.instance())
                                                      .setContext(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                      ),
                      "j0.",
                      "(\"channel\" == \"j0.a0\")",
                      JoinType.INNER
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1256L}
        )
    );
  }

  @Test
  public void testSingleValueStringMultipleRowsAgg()
  {
    skipVectorize();
    cannotVectorize();
    testQueryThrows(
        "SELECT  count(*) FROM wikipedia where channel = (select channel from wikipedia order by __time desc LIMIT 2 OFFSET 6)",
        exception -> exception.expectMessage("Subquery expression returned more than one row")
    );
  }

  @Test
  public void testSingleValueEmptyInnerAgg()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT distinct countryName FROM wikipedia where countryName = ( select countryName from wikipedia where channel in ('abc', 'xyz'))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(join(
                            new TableDataSource(CalciteTests.WIKIPEDIA),
                            new QueryDataSource(Druids.newTimeseriesQueryBuilder()
                                                      .dataSource(CalciteTests.WIKIPEDIA)
                                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                                      .granularity(Granularities.ALL)
                                                      .virtualColumns(expressionVirtualColumn(
                                                                          "v0",
                                                                          "\"countryName\"",
                                                                          ColumnType.STRING
                                                                      )
                                                      )
                                                      .aggregators(
                                                          new SingleValueAggregatorFactory(
                                                              "a0",
                                                              "v0",
                                                              ColumnType.STRING
                                                          )
                                                      )
                                                      .filters(new InDimFilter(
                                                          "channel",
                                                          new HashSet<>(Arrays.asList(
                                                              "abc",
                                                              "xyz"
                                                          ))
                                                      ))
                                                      .context(QUERY_CONTEXT_DEFAULT)
                                                      .build()
                            ),
                            "j0.",
                            "(\"countryName\" == \"j0.a0\")",
                            JoinType.INNER
                        ))
                        .addDimension(new DefaultDimensionSpec("countryName", "d0", ColumnType.STRING))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of()
    );
  }
}
