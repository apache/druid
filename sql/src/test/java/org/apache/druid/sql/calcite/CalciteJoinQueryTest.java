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
import com.google.common.collect.ImmutableSet;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Access;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.druid.query.QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY;

@RunWith(JUnitParamsRunner.class)
public class CalciteJoinQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testExactTopNOnInnerJoinWithLimit() throws Exception
  {
    // Adjust topN threshold, so that the topN engine keeps only 1 slot for aggregates, which should be enough
    // to compute the query with limit 1.
    minTopNThreshold = 1;
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, false);
    testQuery(
        "select f1.\"dim4\", sum(\"m1\") from numfoo f1 inner join (\n"
            + "  select \"dim4\" from numfoo where dim4 <> 'a' group by 1\n"
            + ") f2 on f1.\"dim4\" = f2.\"dim4\" group by 1 limit 1",
        context, // turn on exact topN
        ImmutableList.of(
            new TopNQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim4", "_d0"))
                .aggregators(new DoubleSumAggregatorFactory("a0", "m1"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(1)
                .dataSource(
                    JoinDataSource.create(
                        new TableDataSource("numfoo"),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimFilter(new NotDimFilter(new SelectorDimFilter("dim4", "a", null)))
                                .setDataSource(new TableDataSource("numfoo"))
                                .setDimensions(new DefaultDimensionSpec("dim4", "_d0"))
                                .setContext(context)
                                .build()
                        ),
                        "j0.",
                        "(\"dim4\" == \"j0._d0\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil()
                    )
                )
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"b", 15.0}
        )
    );
  }

  @Test
  public void testJoinOuterGroupByAndSubqueryHasLimit() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim2, AVG(m2) FROM (SELECT * FROM foo AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1 LIMIT 10) AS t3 GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    newScanQueryBuilder()
                        .dataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .columns(ImmutableList.of("m1"))
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("m1"),
                                    DruidExpression.fromColumn("j0.m1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .limit(10)
                        .columns("dim2", "m2")
                        .context(QUERY_CONTEXT_DEFAULT)
                        .build()
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    useDefault
                        ? aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new CountAggregatorFactory("a0:count")
                    )
                        : aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("a0:count"),
                            not(selector("m2", null, null))
                        )
                    )
                )
                .setPostAggregatorSpecs(
                    ImmutableList.of(
                        new ArithmeticPostAggregator(
                            "a0",
                            "quotient",
                            ImmutableList.of(
                                new FieldAccessPostAggregator(null, "a0:sum"),
                                new FieldAccessPostAggregator(null, "a0:count")
                            )
                        )

                    )
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.sqlCompatible()
            ? ImmutableList.of(
            new Object[]{null, 4.0},
            new Object[]{"", 3.0},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
            : ImmutableList.of(
            new Object[]{"", 3.6666666666666665},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOuterGroupByAndSubqueryNoLimit(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT dim2, AVG(m2) FROM (SELECT * FROM foo AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1) AS t3 GROUP BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns(ImmutableList.of("m1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                                .withOverriddenContext(queryContext)
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("m1"),
                            DruidExpression.fromColumn("j0.m1")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    useDefault
                        ? aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new CountAggregatorFactory("a0:count")
                    )
                        : aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("a0:count"),
                            not(selector("m2", null, null))
                        )
                    )
                )
                .setPostAggregatorSpecs(
                    ImmutableList.of(
                        new ArithmeticPostAggregator(
                            "a0",
                            "quotient",
                            ImmutableList.of(
                                new FieldAccessPostAggregator(null, "a0:sum"),
                                new FieldAccessPostAggregator(null, "a0:count")
                            )
                        )

                    )
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
                .withOverriddenContext(queryContext)
        ),
        NullHandling.sqlCompatible()
            ? ImmutableList.of(
            new Object[]{null, 4.0},
            new Object[]{"", 3.0},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
            : ImmutableList.of(
            new Object[]{"", 3.6666666666666665},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
    );
  }

  @Test
  public void testJoinWithLimitBeforeJoining() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT t1.dim2, AVG(t1.m2) FROM (SELECT * FROM foo LIMIT 10) AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1 GROUP BY t1.dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("dim2", "m1", "m2")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .limit(10)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns(ImmutableList.of("m1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("m1"),
                            DruidExpression.fromColumn("j0.m1")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    useDefault
                        ? aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new CountAggregatorFactory("a0:count")
                    )
                        : aggregators(
                        new DoubleSumAggregatorFactory("a0:sum", "m2"),
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("a0:count"),
                            not(selector("m2", null, null))
                        )
                    )
                )
                .setPostAggregatorSpecs(
                    ImmutableList.of(
                        new ArithmeticPostAggregator(
                            "a0",
                            "quotient",
                            ImmutableList.of(
                                new FieldAccessPostAggregator(null, "a0:sum"),
                                new FieldAccessPostAggregator(null, "a0:count")
                            )
                        )

                    )
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.sqlCompatible()
            ? ImmutableList.of(
            new Object[]{null, 4.0},
            new Object[]{"", 3.0},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
            : ImmutableList.of(
            new Object[]{"", 3.6666666666666665},
            new Object[]{"a", 2.5},
            new Object[]{"abc", 5.0}
        )
    );
  }

  @Test
  public void testJoinOnTimeseriesWithFloorOnTime() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100) FROM foo WHERE (TIME_FLOOR(__time, 'PT1H'), m1) IN\n"
            + "   (\n"
            + "     SELECT TIME_FLOOR(__time, 'PT1H') AS t1, MIN(m1) AS t2 FROM foo WHERE dim3 = 'b'\n"
            + "         AND __time BETWEEN '1994-04-29 00:00:00' AND '2020-01-11 00:00:00' GROUP BY 1\n"
            + "    )\n"
            + "GROUP BY 1, 2\n",
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Intervals.of("1994-04-29/2020-01-11T00:00:00.001Z")))
                                .filters(selector("dim3", "b", null))
                                .granularity(new PeriodGranularity(Period.hours(1), null, DateTimeZone.UTC))
                                .aggregators(aggregators(
                                    new FloatMinAggregatorFactory("a0", "m1")
                                ))
                                .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                                .build()),
                        "j0.",
                        "((timestamp_floor(\"__time\",'PT1H',null,'UTC') == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(
                    new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                    new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)

                )
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(aggregators(
                    new StringAnyAggregatorFactory("a0", "dim3", 100)
                ))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f, "[a, b]"},
            new Object[]{946771200000L, 2.0f, "[b, c]"}
        )
    );
  }

  @Test
  public void testJoinOnGroupByInsteadOfTimeseriesWithFloorOnTime() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100) FROM foo WHERE (CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT), m1) IN\n"
            + "   (\n"
            + "     SELECT CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 0 AS t1, MIN(m1) AS t2 FROM foo WHERE dim3 = 'b'\n"
            + "         AND __time BETWEEN '1994-04-29 00:00:00' AND '2020-01-11 00:00:00' GROUP BY 1\n"
            + "    )\n"
            + "GROUP BY 1, 2\n",
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Intervals.of(
                                    "1994-04-29/2020-01-11T00:00:00.001Z")))
                                .setVirtualColumns(
                                    expressionVirtualColumn(
                                        "v0",
                                        "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 0)",
                                        ColumnType.LONG
                                    )
                                )
                                .setDimFilter(selector("dim3", "b", null))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec(
                                    "v0",
                                    "d0",
                                    ColumnType.LONG
                                )))
                                .setAggregatorSpecs(aggregators(
                                    new FloatMinAggregatorFactory("a0", "m1")
                                ))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()),
                        "j0.",
                        "((timestamp_floor(\"__time\",'PT1H',null,'UTC') == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(
                    new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                    new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)

                )
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(aggregators(
                    new StringAnyAggregatorFactory("a0", "dim3", 100)
                ))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f, "[a, b]"},
            new Object[]{946771200000L, 2.0f, "[b, c]"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorWithValueFilterPushdownMatchesNothig(Map<String, Object> queryContext)
      throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.k, COUNT(*)\n"
            + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v = '123'\n"
            + "GROUP BY lookyloo.k",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimFilter(selector("j0.v", "123", null))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("j0.k", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorAllowNulls(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
            + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xa' OR lookyloo.v IS NULL\n"
            + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(or(not(selector("j0.v", "xa", null)), selector("j0.v", null, null)))
                .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorBackwards(Map<String, Object> queryContext) throws Exception
  {
    // Like "testFilterAndGroupByLookupUsingJoinOperator", but with the table and lookup reversed.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
            + "FROM lookup.lookyloo RIGHT JOIN foo ON foo.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xa'\n"
            + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new LookupDataSource("lookyloo"),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("dim2")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("k"), DruidExpression.fromColumn("j0.dim2")),
                        JoinType.RIGHT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimFilter(not(selector("v", "xa", null)))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("v", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorWithNotFilter(Map<String, Object> queryContext)
      throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
            + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xa'\n"
            + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimFilter(not(selector("j0.v", "xa", null)))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinUnionTablesOnLookup(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
            + "FROM\n"
            + "  (SELECT dim2 FROM foo UNION ALL SELECT dim2 FROM numfoo) u\n"
            + "  LEFT JOIN lookup.lookyloo ON u.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xa'\n"
            + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new UnionDataSource(
                            ImmutableList.of(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new TableDataSource(CalciteTests.DATASOURCE3)
                            )
                        ),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimFilter(not(selector("j0.v", "xa", null)))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 6L},
            new Object[]{"xabc", 2L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.k, COUNT(*)\n"
            + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v = 'xa'\n"
            + "GROUP BY lookyloo.k",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimFilter(selector("j0.v", "xa", null))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("j0.k", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingPostAggregationJoinOperator(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT base.dim2, lookyloo.v, base.cnt FROM (\n"
            + "  SELECT dim2, COUNT(*) cnt FROM foo GROUP BY dim2\n"
            + ") base\n"
            + "LEFT JOIN lookup.lookyloo ON base.dim2 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xa' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                .setContext(queryContext)
                                .build()
                        ),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("d0"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xa", null)), selector("j0.v", null, null)))
                .columns("a0", "d0", "j0.v")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.sqlCompatible()
            ? ImmutableList.of(
            new Object[]{NULL_STRING, NULL_STRING, 2L},
            new Object[]{"", NULL_STRING, 1L},
            new Object[]{"abc", "xabc", 1L}
        ) : ImmutableList.of(
            new Object[]{NULL_STRING, NULL_STRING, 3L},
            new Object[]{"abc", "xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testGroupByInnerJoinOnLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT lookyloo.v, COUNT(*)\n"
            + "FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
            + "GROUP BY lookyloo.v",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingInnerJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim2, lookyloo.*\n"
            + "FROM foo INNER JOIN lookup.lookyloo ON foo.dim2 = lookyloo.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a", "xa"},
            new Object[]{"a", "a", "xa"},
            new Object[]{"abc", "abc", "xabc"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinTwoLookupsUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, l1.v, l2.v\n"
            + "FROM foo\n"
            + "LEFT JOIN lookup.lookyloo l1 ON foo.dim1 = l1.k\n"
            + "LEFT JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.v", "dim1", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", NULL_STRING, "xa"},
            new Object[]{"10.1", NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"2", "", NULL_STRING, NULL_STRING},
            new Object[]{"1", "a", NULL_STRING, "xa"},
            new Object[]{"def", "abc", NULL_STRING, "xabc"},
            new Object[]{"abc", NULL_STRING, "xabc", NULL_STRING}
        )
    );
  }



  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithOuterLimit(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
            + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
            + "WHERE l.v = 'xa'\n"
            + "LIMIT 100\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .limit(100)
                .filters(selector("j0.v", "xa", null))
                .columns("dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithoutLimit(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
            + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
            + "WHERE l.v = 'xa'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("j0.v", "xa", null))
                .columns("dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithOuterLimitWithAllColumns(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2, dim3, m1, m2, unique_dim1\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
            + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
            + "WHERE l.v = 'xa'\n"
            + "LIMIT 100\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .limit(100)
                .filters(selector("j0.v", "xa", null))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L, "", "a", "[\"a\",\"b\"]", 1.0F, 1.0, "\"AQAAAEAAAA==\""},
            new Object[]{978307200000L, 1L, "1", "a", "", 4.0F, 4.0, "\"AQAAAQAAAAFREA==\""}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithoutLimitWithAllColumns(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT __time, cnt, dim1, dim2, dim3, m1, m2, unique_dim1\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
            + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
            + "WHERE l.v = 'xa'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("j0.v", "xa", null))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1L, "", "a", "[\"a\",\"b\"]", 1.0F, 1.0, "\"AQAAAEAAAA==\""},
            new Object[]{978307200000L, 1L, "1", "a", "", 4.0F, 4.0, "\"AQAAAQAAAAFREA==\""}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testManyManyInnerJoinOnManyManyLookup(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.dim2 = l.k\n"
            + "INNER JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
            + "INNER JOIN lookup.lookyloo l3 ON foo.dim2 = l3.k\n"
            + "INNER JOIN lookup.lookyloo l4 ON foo.dim2 = l4.k\n"
            + "INNER JOIN lookup.lookyloo l5 ON foo.dim2 = l5.k\n"
            + "INNER JOIN lookup.lookyloo l6 ON foo.dim2 = l6.k\n"
            + "INNER JOIN lookup.lookyloo l7 ON foo.dim2 = l7.k\n"
            + "INNER JOIN lookup.lookyloo l8 ON foo.dim2 = l8.k\n"
            + "INNER JOIN lookup.lookyloo l9 ON foo.dim2 = l9.k\n"
            + "INNER JOIN lookup.lookyloo l10 ON foo.dim2 = l10.k\n"
            + "INNER JOIN lookup.lookyloo l11 ON foo.dim2 = l11.k\n"
            + "INNER JOIN lookup.lookyloo l12 ON foo.dim2 = l12.k\n"
            + "INNER JOIN lookup.lookyloo l13 ON foo.dim2 = l13.k\n"
            + "INNER JOIN lookup.lookyloo l14 ON foo.dim2 = l14.k\n"
            + "INNER JOIN lookup.lookyloo l15 ON foo.dim2 = l15.k\n"
            + "INNER JOIN lookup.lookyloo l16 ON foo.dim2 = l16.k\n"
            + "INNER JOIN lookup.lookyloo l17 ON foo.dim2 = l17.k\n"
            + "INNER JOIN lookup.lookyloo l18 ON foo.dim2 = l18.k\n"
            + "INNER JOIN lookup.lookyloo l19 ON foo.dim2 = l19.k\n"
            + "WHERE l.v = 'xa'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            join(
                                join(
                                    join(
                                        join(
                                            join(
                                                join(
                                                    join(
                                                        join(
                                                            join(
                                                                join(
                                                                    join(
                                                                        join(
                                                                            join(
                                                                                join(
                                                                                    join(
                                                                                        join(
                                                                                            join(
                                                                                                new TableDataSource(
                                                                                                    CalciteTests.DATASOURCE1),
                                                                                                new LookupDataSource(
                                                                                                    "lookyloo"),
                                                                                                "j0.",
                                                                                                equalsCondition(
                                                                                                    DruidExpression.fromColumn(
                                                                                                        "dim2"),
                                                                                                    DruidExpression.fromColumn(
                                                                                                        "j0.k")
                                                                                                ),
                                                                                                JoinType.INNER
                                                                                            ),
                                                                                            new LookupDataSource(
                                                                                                "lookyloo"),
                                                                                            "_j0.",
                                                                                            equalsCondition(
                                                                                                DruidExpression.fromColumn(
                                                                                                    "dim2"),
                                                                                                DruidExpression.fromColumn(
                                                                                                    "_j0.k")
                                                                                            ),
                                                                                            JoinType.INNER
                                                                                        ),
                                                                                        new LookupDataSource("lookyloo"),
                                                                                        "__j0.",
                                                                                        equalsCondition(
                                                                                            DruidExpression.fromColumn(
                                                                                                "dim2"),
                                                                                            DruidExpression.fromColumn(
                                                                                                "__j0.k")
                                                                                        ),
                                                                                        JoinType.INNER
                                                                                    ),
                                                                                    new LookupDataSource("lookyloo"),
                                                                                    "___j0.",
                                                                                    equalsCondition(
                                                                                        DruidExpression.fromColumn(
                                                                                            "dim2"),
                                                                                        DruidExpression.fromColumn(
                                                                                            "___j0.k")
                                                                                    ),
                                                                                    JoinType.INNER
                                                                                ),
                                                                                new LookupDataSource("lookyloo"),
                                                                                "____j0.",
                                                                                equalsCondition(
                                                                                    DruidExpression.fromColumn("dim2"),
                                                                                    DruidExpression.fromColumn(
                                                                                        "____j0.k")
                                                                                ),
                                                                                JoinType.INNER
                                                                            ),
                                                                            new LookupDataSource("lookyloo"),
                                                                            "_____j0.",
                                                                            equalsCondition(
                                                                                DruidExpression.fromColumn("dim2"),
                                                                                DruidExpression.fromColumn("_____j0.k")
                                                                            ),
                                                                            JoinType.INNER
                                                                        ),
                                                                        new LookupDataSource("lookyloo"),
                                                                        "______j0.",
                                                                        equalsCondition(
                                                                            DruidExpression.fromColumn("dim2"),
                                                                            DruidExpression.fromColumn("______j0.k")
                                                                        ),
                                                                        JoinType.INNER
                                                                    ),
                                                                    new LookupDataSource("lookyloo"),
                                                                    "_______j0.",
                                                                    equalsCondition(
                                                                        DruidExpression.fromColumn("dim2"),
                                                                        DruidExpression.fromColumn("_______j0.k")
                                                                    ),
                                                                    JoinType.INNER
                                                                ),
                                                                new LookupDataSource("lookyloo"),
                                                                "________j0.",
                                                                equalsCondition(
                                                                    DruidExpression.fromColumn("dim2"),
                                                                    DruidExpression.fromColumn("________j0.k")
                                                                ),
                                                                JoinType.INNER
                                                            ),
                                                            new LookupDataSource("lookyloo"),
                                                            "_________j0.",
                                                            equalsCondition(
                                                                DruidExpression.fromColumn("dim2"),
                                                                DruidExpression.fromColumn("_________j0.k")
                                                            ),
                                                            JoinType.INNER
                                                        ),
                                                        new LookupDataSource("lookyloo"),
                                                        "__________j0.",
                                                        equalsCondition(
                                                            DruidExpression.fromColumn("dim2"),
                                                            DruidExpression.fromColumn("__________j0.k")
                                                        ),
                                                        JoinType.INNER
                                                    ),
                                                    new LookupDataSource("lookyloo"),
                                                    "___________j0.",
                                                    equalsCondition(
                                                        DruidExpression.fromColumn("dim2"),
                                                        DruidExpression.fromColumn("___________j0.k")
                                                    ),
                                                    JoinType.INNER
                                                ),
                                                new LookupDataSource("lookyloo"),
                                                "____________j0.",
                                                equalsCondition(
                                                    DruidExpression.fromColumn("dim2"),
                                                    DruidExpression.fromColumn("____________j0.k")
                                                ),
                                                JoinType.INNER
                                            ),
                                            new LookupDataSource("lookyloo"),
                                            "_____________j0.",
                                            equalsCondition(
                                                DruidExpression.fromColumn("dim2"),
                                                DruidExpression.fromColumn("_____________j0.k")
                                            ),
                                            JoinType.INNER
                                        ),
                                        new LookupDataSource("lookyloo"),
                                        "______________j0.",
                                        equalsCondition(
                                            DruidExpression.fromColumn("dim2"),
                                            DruidExpression.fromColumn("______________j0.k")
                                        ),
                                        JoinType.INNER
                                    ),
                                    new LookupDataSource("lookyloo"),
                                    "_______________j0.",
                                    equalsCondition(
                                        DruidExpression.fromColumn("dim2"),
                                        DruidExpression.fromColumn("_______________j0.k")
                                    ),
                                    JoinType.INNER
                                ),
                                new LookupDataSource("lookyloo"),
                                "________________j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim2"),
                                    DruidExpression.fromColumn("________________j0.k")
                                ),
                                JoinType.INNER
                            ),
                            new LookupDataSource("lookyloo"),
                            "_________________j0.",
                            equalsCondition(
                                DruidExpression.fromColumn("dim2"),
                                DruidExpression.fromColumn("_________________j0.k")
                            ),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "__________________j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim2"),
                            DruidExpression.fromColumn("__________________j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("j0.v", "xa", null))
                .columns("dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinQueryOfLookup(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize the subquery.
    cannotVectorize();

    testQuery(
        "SELECT dim1, dim2, t1.v, t1.v\n"
            + "FROM foo\n"
            + "INNER JOIN \n"
            + "  (SELECT SUBSTRING(k, 1, 1) k, LATEST(v, 10) v FROM lookup.lookyloo GROUP BY 1) t1\n"
            + "  ON foo.dim2 = t1.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(
                                    new ExtractionDimensionSpec(
                                        "k",
                                        "d0",
                                        new SubstringDimExtractionFn(0, 1)
                                    )
                                )
                                .setAggregatorSpecs(new StringLastAggregatorFactory("a0", "v", null, 10))
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.a0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "xabc", "xabc"},
            new Object[]{"1", "a", "xabc", "xabc"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinQueryOfLookupRemovable(Map<String, Object> queryContext) throws Exception
  {
    // Like "testInnerJoinQueryOfLookup", but the subquery is removable.

    testQuery(
        "SELECT dim1, dim2, t1.sk\n"
            + "FROM foo\n"
            + "INNER JOIN \n"
            + "  (SELECT k, SUBSTRING(v, 1, 3) sk FROM lookup.lookyloo) t1\n"
            + "  ON foo.dim2 = t1.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"j0.v\", 0, 3)", ColumnType.STRING))
                .columns("dim1", "dim2", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "xa"},
            new Object[]{"1", "a", "xa"},
            new Object[]{"def", "abc", "xab"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTwoLookupsToTableUsingNumericColumn(Map<String, Object> queryContext) throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l1 ON l1.k = foo.m1\n"
            + "INNER JOIN lookup.lookyloo l2 ON l2.k = l1.k",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(new LookupDataSource("lookyloo"))
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .virtualColumns(
                                        expressionVirtualColumn(
                                            "v0",
                                            "CAST(\"k\", 'DOUBLE')",
                                            ColumnType.FLOAT
                                        )
                                    )
                                    .columns("k", "v0")
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.fromColumn("m1"),
                                DruidExpression.fromColumn("j0.v0")
                            ),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("j0.k"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(new CountAggregatorFactory("a0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTwoLookupsToTableUsingNumericColumnInReverse(Map<String, Object> queryContext)
      throws Exception
  {
    // Like "testInnerJoinTwoLookupsToTableUsingNumericColumn", but the tables are specified backwards.

    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
            + "FROM lookup.lookyloo l1\n"
            + "INNER JOIN lookup.lookyloo l2 ON l1.k = l2.k\n"
            + "INNER JOIN foo on l2.k = foo.m1",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new LookupDataSource("lookyloo"),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(
                                DruidExpression.fromColumn("k"),
                                DruidExpression.fromColumn("j0.k")
                            ),
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("m1")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("CAST(\"j0.k\", 'DOUBLE')"),
                            DruidExpression.fromColumn("_j0.m1")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(new CountAggregatorFactory("a0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinLookupTableTable(Map<String, Object> queryContext) throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT l.k, l.v, SUM(f.m1), SUM(nf.m1)\n"
            + "FROM lookup.lookyloo l\n"
            + "INNER JOIN druid.foo f on f.dim1 = l.k\n"
            + "INNER JOIN druid.numfoo nf on nf.dim1 = l.k\n"
            + "GROUP BY 1, 2 ORDER BY 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            new LookupDataSource("lookyloo"),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("dim1", "m1")
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.fromColumn("k"),
                                DruidExpression.fromColumn("j0.dim1")
                            ),
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE3)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("dim1", "m1")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("k"),
                            DruidExpression.fromColumn("_j0.dim1")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("k", "d0"),
                        new DefaultDimensionSpec("v", "d1")
                    )
                )
                .setAggregatorSpecs(
                    aggregators(
                        new DoubleSumAggregatorFactory("a0", "j0.m1"),
                        new DoubleSumAggregatorFactory("a1", "_j0.m1")
                    )
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                        null
                    )
                )
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "xabc", 6d, 6d}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinLookupTableTableChained(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT l.k, l.v, SUM(f.m1), SUM(nf.m1)\n"
            + "FROM lookup.lookyloo l\n"
            + "INNER JOIN druid.foo f on f.dim1 = l.k\n"
            + "INNER JOIN druid.numfoo nf on nf.dim1 = f.dim1\n"
            + "GROUP BY 1, 2 ORDER BY 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            new LookupDataSource("lookyloo"),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("dim1", "m1")
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.fromColumn("k"),
                                DruidExpression.fromColumn("j0.dim1")
                            ),
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE3)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("dim1", "m1")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("j0.dim1"),
                            DruidExpression.fromColumn("_j0.dim1")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("k", "d0"),
                        new DefaultDimensionSpec("v", "d1")
                    )
                )
                .setAggregatorSpecs(
                    aggregators(
                        new DoubleSumAggregatorFactory("a0", "j0.m1"),
                        new DoubleSumAggregatorFactory("a1", "_j0.m1")
                    )
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                        null
                    )
                )
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "xabc", 6d, 6d}
        )
    );
  }


  @Test
  public void testWhereInSelectNullFromLookup() throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.
    cannotVectorize();

    testQuery(
        "SELECT * FROM foo where dim1 IN (SELECT NULL FROM lookup.lookyloo)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setVirtualColumns(
                                    expressionVirtualColumn("v0", "null", ColumnType.STRING)
                                )
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "null", ColumnType.STRING)
                )
                .columns("__time", "cnt", "dim2", "dim3", "m1", "m2", "unique_dim1", "v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testCommaJoinLeftFunction() throws Exception
  {
    testQuery(
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
            + "FROM foo, lookup.lookyloo l\n"
            + "WHERE SUBSTRING(foo.dim2, 1, 1) = l.k\n",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.k", "j0.v")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "a", "xa"},
            new Object[]{"1", "a", "a", "xa"},
            new Object[]{"def", "abc", "a", "xa"}
        )
    );
  }

  // This SQL currently does not result in an optimum plan.
  // Unfortunately, we have disabled pushing down predicates (conditions and filters) due to https://github.com/apache/druid/pull/9773
  // Hence, comma join will result in a cross join with filter on outermost
  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCommaJoinTableLookupTableMismatchedTypes(Map<String, Object> queryContext) throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
            + "FROM foo, lookup.lookyloo l, numfoo\n"
            + "WHERE foo.cnt = l.k AND l.k = numfoo.cnt\n",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            "1",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE3)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("cnt")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        "1",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(new CountAggregatorFactory("a0"))
                .filters(and(
                    expressionFilter("(\"cnt\" == CAST(\"j0.k\", 'LONG'))"),
                    expressionFilter("(CAST(\"j0.k\", 'LONG') == \"_j0.cnt\")")
                ))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinTableLookupTableMismatchedTypesWithoutComma(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.cnt = l.k\n"
            + "INNER JOIN numfoo ON l.k = numfoo.cnt\n",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource(new LookupDataSource("lookyloo"))
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .virtualColumns(
                                        expressionVirtualColumn("v0", "CAST(\"k\", 'LONG')", ColumnType.LONG)
                                    )
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .columns("k", "v0")
                                    .context(queryContext)
                                    .build()
                            ),
                            "j0.",
                            equalsCondition(
                                DruidExpression.fromColumn("cnt"),
                                DruidExpression.fromColumn("j0.v0")
                            ),
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE3)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("cnt")
                                .context(queryContext)
                                .build()
                        ),
                        "_j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("CAST(\"j0.k\", 'LONG')"),
                            DruidExpression.fromColumn("_j0.cnt")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(new CountAggregatorFactory("a0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{0L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinCastLeft(Map<String, Object> queryContext) throws Exception
  {
    // foo.m1 is FLOAT, l.k is STRING.

    testQuery(
        "SELECT foo.m1, l.k, l.v\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON CAST(foo.m1 AS VARCHAR) = l.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("CAST(\"m1\", 'STRING')"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinCastRight(Map<String, Object> queryContext) throws Exception
  {
    // foo.m1 is FLOAT, l.k is STRING.

    testQuery(
        "SELECT foo.m1, l.k, l.v\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.m1 = CAST(l.k AS FLOAT)\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(new LookupDataSource("lookyloo"))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(
                                    expressionVirtualColumn("v0", "CAST(\"k\", 'DOUBLE')", ColumnType.FLOAT)
                                )
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("k", "v", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("m1"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{6f, "6", "x6"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinMismatchedTypes(Map<String, Object> queryContext) throws Exception
  {
    // foo.m1 is FLOAT, l.k is STRING. Comparing them generates a CAST.

    testQuery(
        "SELECT foo.m1, l.k, l.v\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.m1 = l.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(new LookupDataSource("lookyloo"))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(
                                    expressionVirtualColumn("v0", "CAST(\"k\", 'DOUBLE')", ColumnType.FLOAT)
                                )
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("k", "v", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("m1"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{6f, "6", "x6"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinLeftFunction(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON SUBSTRING(foo.dim2, 1, 1) = l.k\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "a", "xa"},
            new Object[]{"1", "a", "a", "xa"},
            new Object[]{"def", "abc", "a", "xa"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinRightFunction(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
            + "FROM foo\n"
            + "INNER JOIN lookup.lookyloo l ON foo.dim2 = SUBSTRING(l.k, 1, 2)\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(new LookupDataSource("lookyloo"))
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(
                                    expressionVirtualColumn("v0", "substring(\"k\", 0, 2)", ColumnType.STRING)
                                )
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .columns("k", "v", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", "a", "xa"},
            new Object[]{"1", "a", "a", "xa"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinLookupOntoLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim2, l1.v, l2.v\n"
            + "FROM foo\n"
            + "LEFT JOIN lookup.lookyloo l1 ON foo.dim2 = l1.k\n"
            + "LEFT JOIN lookup.lookyloo l2 ON l1.k = l2.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new LookupDataSource("lookyloo"),
                            "j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(DruidExpression.fromColumn("j0.k"), DruidExpression.fromColumn("_j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.v", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "xa", "xa"},
            new Object[]{NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"", NULL_STRING, NULL_STRING},
            new Object[]{"a", "xa", "xa"},
            new Object[]{"abc", "xabc", "xabc"},
            new Object[]{NULL_STRING, NULL_STRING, NULL_STRING}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinThreeLookupsUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, l1.v, l2.v, l3.v\n"
            + "FROM foo\n"
            + "LEFT JOIN lookup.lookyloo l1 ON foo.dim1 = l1.k\n"
            + "LEFT JOIN lookup.lookyloo l2 ON foo.dim2 = l2.k\n"
            + "LEFT JOIN lookup.lookyloo l3 ON l2.k = l3.k",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        join(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new LookupDataSource("lookyloo"),
                                "j0.",
                                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                                JoinType.LEFT
                            ),
                            new LookupDataSource("lookyloo"),
                            "_j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "__j0.",
                        equalsCondition(DruidExpression.fromColumn("_j0.k"), DruidExpression.fromColumn("__j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__j0.v", "_j0.v", "dim1", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", "a", NULL_STRING, "xa", "xa"},
            new Object[]{"10.1", NULL_STRING, NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"2", "", NULL_STRING, NULL_STRING, NULL_STRING},
            new Object[]{"1", "a", NULL_STRING, "xa", "xa"},
            new Object[]{"def", "abc", NULL_STRING, "xabc", "xabc"},
            new Object[]{"abc", NULL_STRING, "xabc", NULL_STRING, NULL_STRING}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingLeftJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, lookyloo.*\n"
            + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xxx", null)), selector("j0.v", null, null)))
                .columns("dim1", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", NULL_STRING, NULL_STRING},
            new Object[]{"10.1", NULL_STRING, NULL_STRING},
            new Object[]{"2", NULL_STRING, NULL_STRING},
            new Object[]{"1", NULL_STRING, NULL_STRING},
            new Object[]{"def", NULL_STRING, NULL_STRING},
            new Object[]{"abc", "abc", "xabc"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingRightJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, lookyloo.*\n"
            + "FROM foo RIGHT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.RIGHT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xxx", null)), selector("j0.v", null, null)))
                .columns("dim1", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc", "xabc"},
            new Object[]{NULL_STRING, "a", "xa"},
            new Object[]{NULL_STRING, "nosuchkey", "mysteryvalue"},
            new Object[]{NULL_STRING, "6", "x6"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingFullJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, m1, cnt, lookyloo.*\n"
            + "FROM foo FULL JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k\n"
            + "WHERE lookyloo.v <> 'xxx' OR lookyloo.v IS NULL",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.FULL
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(or(not(selector("j0.v", "xxx", null)), selector("j0.v", null, null)))
                .columns("cnt", "dim1", "j0.k", "j0.v", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"10.1", 2f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"2", 3f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"1", 4f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"def", 5f, 1L, NULL_STRING, NULL_STRING},
            new Object[]{"abc", 6f, 1L, "abc", "xabc"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "a", "xa"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "nosuchkey", "mysteryvalue"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "6", "x6"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInAggregationSubquery(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT DISTINCT __time FROM druid.foo WHERE __time IN (SELECT MAX(__time) FROM druid.foo)",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                                .withOverriddenContext(queryContext)
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("__time"),
                            DruidExpression.fromColumn("j0.a0")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
                .withOverriddenContext(queryContext)
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-03")}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testNotInAggregationSubquery(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT DISTINCT __time FROM druid.foo WHERE __time NOT IN (SELECT MAX(__time) FROM druid.foo)",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new QueryDataSource(
                                GroupByQuery
                                    .builder()
                                    .setDataSource(
                                        Druids.newTimeseriesQueryBuilder()
                                            .dataSource(CalciteTests.DATASOURCE1)
                                            .intervals(querySegmentSpec(Filtration.eternity()))
                                            .granularity(Granularities.ALL)
                                            .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                            .context(QUERY_CONTEXT_DEFAULT)
                                            .build()
                                    )
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setGranularity(Granularities.ALL)
                                    .setAggregatorSpecs(
                                        new CountAggregatorFactory("_a0"),
                                        NullHandling.sqlCompatible()
                                            ? new FilteredAggregatorFactory(
                                            new CountAggregatorFactory("_a1"),
                                            not(selector("a0", null, null))
                                        )
                                            : new CountAggregatorFactory("_a1")
                                    )
                                    .setContext(queryContext)
                                    .build()
                            ),
                            "j0.",
                            "1",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                .postAggregators(expressionPostAgg("p0", "1"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        "(\"__time\" == \"_j0.a0\")",
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(
                    or(
                        selector("j0._a0", "0", null),
                        and(selector("_j0.p0", null, null), expressionFilter("(\"j0._a1\" >= \"j0._a0\")"))
                    )
                )
                .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01")},
            new Object[]{timestamp("2000-01-02")},
            new Object[]{timestamp("2000-01-03")},
            new Object[]{timestamp("2001-01-01")},
            new Object[]{timestamp("2001-01-02")}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryWithExtractionFns(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim2, COUNT(*) FROM druid.foo "
            + "WHERE substring(dim2, 1, 1) IN (SELECT substring(dim1, 1, 1) FROM druid.foo WHERE dim1 <> '')"
            + "group by dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimFilter(not(selector("dim1", "", null)))
                                .setDimensions(
                                    dimensions(new ExtractionDimensionSpec(
                                        "dim1",
                                        "d0",
                                        new SubstringDimExtractionFn(
                                            0,
                                            1
                                        )
                                    ))
                                )
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                            DruidExpression.fromColumn("j0.d0")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinWithIsNullFilter(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1, l.v from druid.foo f inner join lookup.lookyloo l on f.dim1 = l.k where f.dim2 is null",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim1"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("dim2", null, null))
                .columns("dim1", "j0.v")
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "xabc"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  @Ignore // regression test for https://github.com/apache/druid/issues/9924
  public void testInnerJoinOnMultiValueColumn(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT dim3, l.v, count(*) from druid.foo f inner join lookup.lookyloo l on f.dim3 = l.k "
            + "group by 1, 2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim3"),
                            DruidExpression.fromColumn("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("dim3", "d0"),
                        new DefaultDimensionSpec("j0.v", "d1")
                    )
                )
                .build()
        ),
        ImmutableList.of(
            new Object[]{"2", "x2", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithTimeFilter(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1' AND \"__time\" >= '1999'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(
                                    querySegmentSpec(
                                        Intervals.utc(
                                            DateTimes.of("1999-01-01").getMillis(),
                                            JodaUtils.MAX_INSTANT
                                        )
                                    )
                                )
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(
                                    querySegmentSpec(
                                        Intervals.utc(
                                            DateTimes.of("1999-01-01").getMillis(),
                                            JodaUtils.MAX_INSTANT
                                        )
                                    )
                                )
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .filters(new SelectorDimFilter("v0", "10.1", null))
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithTimeFilter_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1' AND \"__time\" >= '1999'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(
                                    querySegmentSpec(
                                        Intervals.utc(
                                            DateTimes.of("1999-01-01").getMillis(),
                                            JodaUtils.MAX_INSTANT
                                        )
                                    )
                                )
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromExpression("'10.1'"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.LEFT,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(
                    Intervals.utc(
                        DateTimes.of("1999-01-01").getMillis(),
                        JodaUtils.MAX_INSTANT
                    )
                ))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .filters(new SelectorDimFilter("v0", "10.1", null))
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSourcesWithOuterWhere_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.LEFT,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSources(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinOnTwoInlineDataSources_withLeftDirectAccess(Map<String, Object> queryContext) throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 LEFT JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.LEFT,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext) throws Exception
  {
    Druids.ScanQueryBuilder baseScanBuilder = newScanQueryBuilder()
        .dataSource(
            join(
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .filters(new SelectorDimFilter("dim1", "10.1", null))
                        .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                        .columns(ImmutableList.of("__time", "v0"))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .filters(new SelectorDimFilter("dim1", "10.1", null))
                        .columns(ImmutableList.of("dim1"))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                JoinType.INNER
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
        .columns("__time", "_v0")
        .context(queryContext);

    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            NullHandling.sqlCompatible() ? baseScanBuilder.build() :
                baseScanBuilder.filters(new NotDimFilter(new SelectorDimFilter("v0", null, null))).build()),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1 WHERE t1.dim1 = '10.1'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.INNER,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSources(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "_v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSources_withLeftDirectAccess(Map<String, Object> queryContext)
      throws Exception
  {
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "with abc as\n"
            + "(\n"
            + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
            + ")\n"
            + "SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromExpression("'10.1'"),
                            DruidExpression.fromColumn("j0.dim1")
                        ),
                        JoinType.INNER,
                        selector("dim1", "10.1", null)
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("__time", "v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  // This query is expected to fail as we do not support join with constant in the on condition
  // (see issue https://github.com/apache/druid/issues/9942 for more information)
  // TODO: Remove expected Exception when https://github.com/apache/druid/issues/9942 is fixed
  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOnConstantShouldFail(Map<String, Object> queryContext)
  {
    assertQueryIsUnplannable("SELECT t1.dim1 from foo as t1 LEFT JOIN foo as t2 on t1.dim1 = '10.1'",
        "Possible error: SQL is resulting in a join that has unsupported operand types.");
  }

  @Test
  public void testLeftJoinRightTableCanBeEmpty() throws Exception
  {
    // HashJoinSegmentStorageAdapter is not vectorizable
    cannotVectorize();

    final DataSource rightTable;
    if (useDefault) {
      rightTable = InlineDataSource.fromIterable(
          ImmutableList.of(),
          RowSignature.builder().add("dim2", ColumnType.STRING).add("m2", ColumnType.DOUBLE).build()
      );
    } else {
      rightTable = new QueryDataSource(
          Druids.newScanQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .filters(new SelectorDimFilter("m2", null, null))
              .columns("dim2")
              .legacy(false)
              .build()
      );
    }

    testQuery(
        "SELECT v1.dim2, count(1) "
            + "FROM (SELECT * FROM foo where m1 > 2) v1 "
            + "LEFT OUTER JOIN ("
            + "  select dim2 from (select * from foo where m2 is null)"
            + ") sm ON v1.dim2 = sm.dim2 "
            + "group by 1",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(
                    JoinDataSource.create(
                        new QueryDataSource(
                            Druids.newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .filters(new BoundDimFilter(
                                    "m1",
                                    "2",
                                    null,
                                    true,
                                    false,
                                    null,
                                    null,
                                    StringComparators.NUMERIC
                                ))
                                .columns("dim2")
                                .legacy(false)
                                .build()
                        ),
                        rightTable,
                        "j0.",
                        "(\"dim2\" == \"j0.dim2\")",
                        JoinType.LEFT,
                        null,
                        ExprMacroTable.nil()
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING)
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        useDefault
            ? ImmutableList.of(
            new Object[]{"", 2L},
            new Object[]{"a", 1L},
            new Object[]{"abc", 1L}
        )
            : ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{"", 1L},
            new Object[]{"a", 1L},
            new Object[]{"abc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinSubqueryWithNullKeyFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();

    ScanQuery nullCompatibleModePlan = newScanQueryBuilder()
        .dataSource(
            join(
                new TableDataSource(CalciteTests.DATASOURCE1),
                new QueryDataSource(
                    GroupByQuery
                        .builder()
                        .setDataSource(new LookupDataSource("lookyloo"))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                        )
                        .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                        .build()
                ),
                "j0.",
                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                JoinType.INNER
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("dim1", "j0.d0")
        .context(queryContext)
        .build();

    ScanQuery nonNullCompatibleModePlan = newScanQueryBuilder()
        .dataSource(
            join(
                new TableDataSource(CalciteTests.DATASOURCE1),
                new QueryDataSource(
                    GroupByQuery
                        .builder()
                        .setDataSource(new LookupDataSource("lookyloo"))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                        )
                        .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                        .build()
                ),
                "j0.",
                equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                JoinType.LEFT
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("dim1", "j0.d0")
        .filters(new NotDimFilter(new SelectorDimFilter("j0.d0", null, null)))
        .context(queryContext)
        .build();

    boolean isJoinFilterRewriteEnabled = queryContext.getOrDefault(JOIN_FILTER_REWRITE_ENABLE_KEY, true)
        .toString()
        .equals("true");
    testQuery(
        "SELECT dim1, l1.k\n"
            + "FROM foo\n"
            + "LEFT JOIN (select k || '' as k from lookup.lookyloo group by 1) l1 ON foo.dim1 = l1.k\n"
            + "WHERE l1.k IS NOT NULL\n",
        queryContext,
        ImmutableList.of(NullHandling.sqlCompatible() ? nullCompatibleModePlan : nonNullCompatibleModePlan),
        NullHandling.sqlCompatible() || !isJoinFilterRewriteEnabled
            ? ImmutableList.of(new Object[]{"abc", "abc"})
            : ImmutableList.of(
            new Object[]{"10.1", ""},
            // this result is incorrect. TODO : fix this result when the JoinFilterAnalyzer bug is fixed
            new Object[]{"2", ""},
            new Object[]{"1", ""},
            new Object[]{"def", ""},
            new Object[]{"abc", "abc"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinSubqueryWithSelectorFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();

    // disable the cost model where inner join is treated like a filter
    // this leads to cost(left join) < cost(converted inner join) for the below query
    queryContext = QueryContextForJoinProvider.withOverrides(
        queryContext,
        ImmutableMap.of("computeInnerJoinCostAsFilter", "false")
    );
    testQuery(
        "SELECT dim1, l1.k\n"
            + "FROM foo\n"
            + "LEFT JOIN (select k || '' as k from lookup.lookyloo group by 1) l1 ON foo.dim1 = l1.k\n"
            + "WHERE l1.k = 'abc'\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setVirtualColumns(
                                    expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                                )
                                .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.d0")
                .filters(selector("j0.d0", "abc", null))
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinWithNotNullFilter(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT s.dim1, t.dim1\n"
            + "FROM foo as s\n"
            + "LEFT JOIN foo as t "
            + "ON s.dim1 = t.dim1 "
            + "and s.dim1 IS NOT NULL\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .columns(ImmutableList.of("dim1"))
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .context(QUERY_CONTEXT_DEFAULT)
                            .build()),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.dim1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", ""},
            new Object[]{"10.1", "10.1"},
            new Object[]{"2", "2"},
            new Object[]{"1", "1"},
            new Object[]{"def", "def"},
            new Object[]{"abc", "abc"}
        )
    );
  }



  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinSubqueryWithSelectorFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();

    testQuery(
        "SELECT dim1, l1.k "
            + "FROM foo INNER JOIN (select k || '' as k from lookup.lookyloo group by 1) l1 "
            + "ON foo.dim1 = l1.k and l1.k = 'abc'",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(new LookupDataSource("lookyloo"))
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setVirtualColumns(
                                    expressionVirtualColumn("v0", "concat(\"k\",'')", ColumnType.STRING)
                                )
                                .setDimensions(new DefaultDimensionSpec("v0", "d0"))
                                .build()
                        ),
                        "j0.",
                        StringUtils.format(
                            "(%s && %s)",
                            equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                            equalsCondition(
                                DruidExpression.fromExpression("'abc'"),
                                DruidExpression.fromColumn("j0.d0")
                            )
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.d0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc"}
        )
    );
  }

  @Test
  public void testSemiJoinWithOuterTimeExtractScan() throws Exception
  {
    testQuery(
        "SELECT dim1, EXTRACT(MONTH FROM __time) FROM druid.foo\n"
            + " WHERE dim2 IN (\n"
            + "   SELECT dim2\n"
            + "   FROM druid.foo\n"
            + "   WHERE dim1 = 'def'\n"
            + " ) AND dim1 <> ''",
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
                                .setDimFilter(selector("dim1", "def", null))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                )
                .filters(not(selector("dim1", "", null)))
                .columns("dim1", "v0")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTwoSemiJoinsSimultaneously(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT dim1, COUNT(*) FROM foo\n"
            + "WHERE dim1 IN ('abc', 'def')"
            + "AND __time IN (SELECT MAX(__time) FROM foo WHERE cnt = 1)\n"
            + "AND __time IN (SELECT MAX(__time) FROM foo WHERE cnt <> 2)\n"
            + "GROUP BY 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .granularity(Granularities.ALL)
                                    .filters(selector("cnt", "1", null))
                                    .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "j0.",
                            "(\"__time\" == \"j0.a0\")",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .filters(not(selector("cnt", "2", null)))
                                .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        "(\"__time\" == \"_j0.a0\")",
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(in("dim1", ImmutableList.of("abc", "def"), null))
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{"abc", 1L})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSemiAndAntiJoinSimultaneouslyUsingWhereInSubquery(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();

    testQuery(
        "SELECT dim1, COUNT(*) FROM foo\n"
            + "WHERE dim1 IN ('abc', 'def')\n"
            + "AND __time IN (SELECT MAX(__time) FROM foo)\n"
            + "AND __time NOT IN (SELECT MIN(__time) FROM foo)\n"
            + "GROUP BY 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    Druids.newTimeseriesQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .granularity(Granularities.ALL)
                                        .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                        .context(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                ),
                                "j0.",
                                "(\"__time\" == \"j0.a0\")",
                                JoinType.INNER
                            ),
                            new QueryDataSource(
                                GroupByQuery.builder()
                                    .setDataSource(
                                        new QueryDataSource(
                                            Druids.newTimeseriesQueryBuilder()
                                                .dataSource(CalciteTests.DATASOURCE1)
                                                .intervals(querySegmentSpec(Filtration.eternity()))
                                                .granularity(Granularities.ALL)
                                                .aggregators(
                                                    new LongMinAggregatorFactory("a0", "__time")
                                                )
                                                .context(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                        )
                                    )
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setGranularity(Granularities.ALL)
                                    .setAggregatorSpecs(
                                        new CountAggregatorFactory("_a0"),
                                        NullHandling.sqlCompatible()
                                            ? new FilteredAggregatorFactory(
                                            new CountAggregatorFactory("_a1"),
                                            not(selector("a0", null, null))
                                        )
                                            : new CountAggregatorFactory("_a1")
                                    )
                                    .setContext(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "_j0.",
                            "1",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(new LongMinAggregatorFactory("a0", "__time"))
                                .postAggregators(expressionPostAgg("p0", "1"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "__j0.",
                        "(\"__time\" == \"__j0.a0\")",
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(
                    and(
                        in("dim1", ImmutableList.of("abc", "def"), null),
                        or(
                            selector("_j0._a0", "0", null),
                            and(
                                selector("__j0.p0", null, null),
                                expressionFilter("(\"_j0._a1\" >= \"_j0._a0\")")
                            )
                        )
                    )
                )
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{"abc", 1L})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSemiAndAntiJoinSimultaneouslyUsingExplicitJoins(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();

    testQuery(
        "SELECT dim1, COUNT(*) FROM\n"
            + "foo\n"
            + "INNER JOIN (SELECT MAX(__time) t FROM foo) t0 on t0.t = foo.__time\n"
            + "LEFT JOIN (SELECT MIN(__time) t FROM foo) t1 on t1.t = foo.__time\n"
            + "WHERE dim1 IN ('abc', 'def') AND t1.t is null\n"
            + "GROUP BY 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .granularity(Granularities.ALL)
                                    .aggregators(new LongMaxAggregatorFactory("a0", "__time"))
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "j0.",
                            "(\"__time\" == \"j0.a0\")",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(new LongMinAggregatorFactory("a0", "__time"))
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "_j0.",
                        "(\"__time\" == \"_j0.a0\")",
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(
                    and(
                        in("dim1", ImmutableList.of("abc", "def"), null),
                        selector("_j0.a0", null, null)
                    )
                )
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING)))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{"abc", 1L})
    );
  }

  @Test
  public void testSemiJoinWithOuterTimeExtractAggregateWithOrderBy() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT dim1), EXTRACT(MONTH FROM __time) FROM druid.foo\n"
            + " WHERE dim2 IN (\n"
            + "   SELECT dim2\n"
            + "   FROM druid.foo\n"
            + "   WHERE dim1 = 'def'\n"
            + " ) AND dim1 <> ''"
            + "GROUP BY EXTRACT(MONTH FROM __time)\n"
            + "ORDER BY EXTRACT(MONTH FROM __time)",
        ImmutableList.of(
            GroupByQuery
                .builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery
                                .builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                .setDimFilter(selector("dim1", "def", null))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                )
                .setDimFilter(not(selector("dim1", "", null)))
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)))
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    aggregators(
                        new CardinalityAggregatorFactory(
                            "a0",
                            null,
                            ImmutableList.of(
                                new DefaultDimensionSpec("dim1", "dim1", ColumnType.STRING)
                            ),
                            false,
                            true
                        )
                    )
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.ASCENDING,
                                StringComparators.NUMERIC
                            )
                        ),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1L}
        )
    );
  }

  // This query is expected to fail as we do not support join on multi valued column
  // (see issue https://github.com/apache/druid/issues/9924 for more information)
  // TODO: Remove expected Exception when https://github.com/apache/druid/issues/9924 is fixed
  @Test(expected = QueryException.class)
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOnMultiValuedColumnShouldThrowException(Map<String, Object> queryContext) throws Exception
  {
    final String query = "SELECT dim3, l.v from druid.foo f inner join lookup.lookyloo l on f.dim3 = l.k\n";

    testQuery(
        query,
        queryContext,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUnionAllTwoQueriesLeftQueryIsJoin(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "(SELECT COUNT(*) FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k)  UNION ALL SELECT SUM(cnt) FROM foo",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
                .withOverriddenContext(queryContext),
            Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
                .withOverriddenContext(queryContext)
        ),
        ImmutableList.of(new Object[]{1L}, new Object[]{6L})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUnionAllTwoQueriesRightQueryIsJoin(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "(SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k) ",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
                .withOverriddenContext(queryContext),
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
                .withOverriddenContext(queryContext)
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{1L})
    );
  }

  @Test
  public void testUnionAllTwoQueriesBothQueriesAreJoin() throws Exception
  {
    cannotVectorize();

    testQuery(
        "("
            + "SELECT COUNT(*) FROM foo LEFT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k "
            + "                               UNION ALL                                       "
            + "SELECT COUNT(*) FROM foo INNER JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k"
            + ") ",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build(),
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.INNER
                    ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{1L})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNFilterJoin(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    // Filters on top N values of some dimension by using an inner join.
    testQuery(
        "SELECT t1.dim1, SUM(t1.cnt)\n"
            + "FROM druid.foo t1\n"
            + "  INNER JOIN (\n"
            + "  SELECT\n"
            + "    SUM(cnt) AS sum_cnt,\n"
            + "    dim2\n"
            + "  FROM druid.foo\n"
            + "  GROUP BY dim2\n"
            + "  ORDER BY 1 DESC\n"
            + "  LIMIT 2\n"
            + ") t2 ON (t1.dim2 = t2.dim2)\n"
            + "GROUP BY t1.dim1\n"
            + "ORDER BY 1\n",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            new TopNQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                                .aggregators(new LongSumAggregatorFactory("a0", "cnt"))
                                .metric("a0")
                                .threshold(2)
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim2"),
                            DruidExpression.fromColumn("j0.d0")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.ASCENDING,
                                StringComparators.LEXICOGRAPHIC
                            )
                        ),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNFilterJoinWithProjection(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    // Filters on top N values of some dimension by using an inner join. Also projects the outer dimension.

    testQuery(
        "SELECT SUBSTRING(t1.dim1, 1, 10), SUM(t1.cnt)\n"
            + "FROM druid.foo t1\n"
            + "  INNER JOIN (\n"
            + "  SELECT\n"
            + "    SUM(cnt) AS sum_cnt,\n"
            + "    dim2\n"
            + "  FROM druid.foo\n"
            + "  GROUP BY dim2\n"
            + "  ORDER BY 1 DESC\n"
            + "  LIMIT 2\n"
            + ") t2 ON (t1.dim2 = t2.dim2)\n"
            + "GROUP BY SUBSTRING(t1.dim1, 1, 10)",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            new TopNQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                                .aggregators(new LongSumAggregatorFactory("a0", "cnt"))
                                .metric("a0")
                                .threshold(2)
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim2"),
                            DruidExpression.fromColumn("j0.d0")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    dimensions(
                        new ExtractionDimensionSpec(
                            "dim1",
                            "d0",
                            ColumnType.STRING,
                            new SubstringDimExtractionFn(0, 10)
                        )
                    )
                )
                .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 1L},
            new Object[]{"1", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  @Ignore("Stopped working after the ability to join on subqueries was added to DruidJoinRule")
  public void testRemovableLeftJoin(Map<String, Object> queryContext) throws Exception
  {
    // LEFT JOIN where the right-hand side can be ignored.

    testQuery(
        "SELECT t1.dim1, SUM(t1.cnt)\n"
            + "FROM druid.foo t1\n"
            + "  LEFT JOIN (\n"
            + "  SELECT\n"
            + "    SUM(cnt) AS sum_cnt,\n"
            + "    dim2\n"
            + "  FROM druid.foo\n"
            + "  GROUP BY dim2\n"
            + "  ORDER BY 1 DESC\n"
            + "  LIMIT 2\n"
            + ") t2 ON (t1.dim2 = t2.dim2)\n"
            + "GROUP BY t1.dim1\n"
            + "ORDER BY 1\n",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.ASCENDING,
                                StringComparators.LEXICOGRAPHIC
                            )
                        ),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"1", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCountDistinctOfLookupUsingJoinOperator(Map<String, Object> queryContext) throws Exception
  {
    // Cannot yet vectorize the JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT lookyloo.v)\n"
            + "FROM foo LEFT JOIN lookup.lookyloo ON foo.dim1 = lookyloo.k",
        queryContext,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new LookupDataSource("lookyloo"),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(
                    new CardinalityAggregatorFactory(
                        "a0",
                        null,
                        ImmutableList.of(DefaultDimensionSpec.of("j0.v")),
                        false,
                        true
                    )
                ))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryAsPartOfAndFilter(Map<String, Object> queryContext) throws Exception
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
            + "WHERE dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 <> '')\n"
            + "AND dim1 <> 'xxx'\n"
            + "group by dim1, dim2 ORDER BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimFilter(not(selector("dim1", "", null)))
                                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim2"),
                            DruidExpression.fromColumn("j0.d0")
                        ),
                        JoinType.INNER
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(not(selector("dim1", "xxx", null)))
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("dim1", "d0"),
                        new DefaultDimensionSpec("dim2", "d1")
                    )
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryAsPartOfOrFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo\n"
            + "WHERE dim1 = 'xxx' OR dim2 IN (SELECT dim1 FROM druid.foo WHERE dim1 LIKE '%bc')\n"
            + "group by dim1, dim2 ORDER BY dim2",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(
                    join(
                        join(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .filters(new LikeDimFilter("dim1", "%bc", null, null))
                                    .granularity(Granularities.ALL)
                                    .aggregators(new CountAggregatorFactory("a0"))
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            ),
                            "j0.",
                            "1",
                            JoinType.INNER
                        ),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                .setDataSource(CalciteTests.DATASOURCE1)
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setVirtualColumns(expressionVirtualColumn("v0", "1", ColumnType.LONG))
                                .setDimFilter(new LikeDimFilter("dim1", "%bc", null, null))
                                .setDimensions(
                                    dimensions(
                                        new DefaultDimensionSpec("dim1", "d0"),
                                        new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                                    )
                                )
                                .setContext(queryContext)
                                .build()
                        ),
                        "_j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim2"),
                            DruidExpression.fromColumn("_j0.d0")
                        ),
                        JoinType.LEFT
                    )
                )
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimFilter(
                    or(
                        selector("dim1", "xxx", null),
                        and(
                            not(selector("j0.a0", "0", null)),
                            not(selector("_j0.d1", null, null)),
                            not(selector("dim2", null, null))
                        )
                    )
                )
                .setDimensions(
                    dimensions(
                        new DefaultDimensionSpec("dim1", "d0"),
                        new DefaultDimensionSpec("dim2", "d1")
                    )
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def", "abc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testNestedGroupByOnInlineDataSourceWithFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "with abc as"
            + "("
            + "  SELECT dim1, m2 from druid.foo where \"__time\" >= '2001-01-02'"
            + ")"
            + ", def as"
            + "("
            + "  SELECT t1.dim1, SUM(t2.m2) as \"metricSum\" "
            + "  from abc as t1 inner join abc as t2 on t1.dim1 = t2.dim1"
            + "  where t1.dim1='def'"
            + "  group by 1"
            + ")"
            + "SELECT count(*) from def",
        queryContext,
        ImmutableList.of(
            GroupByQuery
                .builder()
                .setDataSource(
                    GroupByQuery
                        .builder()
                        .setDataSource(
                            join(
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Intervals.of(
                                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                                        .columns("dim1")
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(queryContext)
                                        .build()
                                ),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Intervals.of(
                                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                                        .columns("dim1", "m2")
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(queryContext)
                                        .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.fromColumn("dim1"),
                                    DruidExpression.fromColumn("j0.dim1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(selector("dim1", "def", null))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ColumnType.STRING))
                        .build()
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setGranularity(Granularities.ALL)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testGroupByJoinAsNativeQueryWithUnoptimizedFilter(Map<String, Object> queryContext)
  {
    // The query below is the same as the inner groupBy on a join datasource from the test
    // testNestedGroupByOnInlineDataSourceWithFilter, except that the selector filter
    // dim1=def has been rewritten into an unoptimized filter, dim1 IN (def).
    //
    // The unoptimized filter will be optimized into dim1=def by the query toolchests in their
    // pre-merge decoration function, when it calls DimFilter.optimize().
    //
    // This test's goal is to ensure that the join filter rewrites function correctly when there are
    // unoptimized filters in the join query. The rewrite logic must apply to the optimized form of the filters,
    // as this is what will be passed to HashJoinSegmentAdapter.makeCursors(), where the result of the join
    // filter pre-analysis is used.
    //
    // A native query is used because the filter types where we support optimization are the AND/OR/NOT and
    // IN filters. However, when expressed in a SQL query, our SQL planning layer is smart enough to already apply
    // these optimizations in the native query it generates, making it impossible to test the unoptimized filter forms
    // using SQL queries.
    //
    // The test method is placed here for convenience as this class provides the necessary setup.
    Query query = GroupByQuery
        .builder()
        .setDataSource(
            join(
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of(
                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of(
                            "2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1", "m2")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(
                    DruidExpression.fromColumn("dim1"),
                    DruidExpression.fromColumn("j0.dim1")
                ),
                JoinType.INNER
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(querySegmentSpec(Filtration.eternity()))
        .setDimFilter(in("dim1", Collections.singletonList("def"), null))  // provide an unoptimized IN filter
        .setDimensions(
            dimensions(
                new DefaultDimensionSpec("v0", "d0")
            )
        )
        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ColumnType.STRING))
        .build();

    QueryLifecycleFactory qlf = CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate);
    QueryLifecycle ql = qlf.factorize();
    Sequence seq = ql.runSimple(query, CalciteTests.SUPER_USER_AUTH_RESULT, Access.OK);
    List<Object> results = seq.toList();
    Assert.assertEquals(
        ImmutableList.of(ResultRow.of("def")),
        results
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCountOnSemiJoinSingleColumn(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT dim1 FROM foo WHERE dim1 IN (SELECT dim1 FROM foo WHERE dim1 = '10.1')\n",
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
                                .setDimFilter(
                                    selector("dim1", "10.1", null)
                                )
                                .setGranularity(Granularities.ALL)
                                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                .setContext(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .columns("v0")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNOnStringWithNonSortedOrUniqueDictionary(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT druid.broadcast.dim4, COUNT(*)\n"
            + "FROM druid.numfoo\n"
            + "INNER JOIN druid.broadcast ON numfoo.dim4 = broadcast.dim4\n"
            + "GROUP BY 1 ORDER BY 2 LIMIT 4",
        queryContext,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE3),
                        new GlobalTableDataSource(CalciteTests.BROADCAST_DATASOURCE),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim4"),
                            DruidExpression.fromColumn("j0.dim4")
                        ),
                        JoinType.INNER

                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("j0.dim4", "_d0", ColumnType.STRING))
                .threshold(4)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(queryContext)
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 9L},
            new Object[]{"b", 9L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNOnStringWithNonSortedOrUniqueDictionaryOrderByDim(Map<String, Object> queryContext)
      throws Exception
  {
    testQuery(
        "SELECT druid.broadcast.dim4, COUNT(*)\n"
            + "FROM druid.numfoo\n"
            + "INNER JOIN druid.broadcast ON numfoo.dim4 = broadcast.dim4\n"
            + "GROUP BY 1 ORDER BY 1 DESC LIMIT 4",
        queryContext,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE3),
                        new GlobalTableDataSource(CalciteTests.BROADCAST_DATASOURCE),
                        "j0.",
                        equalsCondition(
                            DruidExpression.fromColumn("dim4"),
                            DruidExpression.fromColumn("j0.dim4")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("j0.dim4", "_d0", ColumnType.STRING))
                .threshold(4)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(queryContext)
                .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
                .build()
        ),
        ImmutableList.of(
            new Object[]{"b", 9L},
            new Object[]{"a", 9L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testVirtualColumnOnMVFilterJoinExpression(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT foo1.dim3, foo2.dim3 FROM druid.numfoo as foo1 INNER JOIN druid.numfoo as foo2 "
        + "ON MV_FILTER_ONLY(foo1.dim3, ARRAY['a']) = MV_FILTER_ONLY(foo2.dim3, ARRAY['a'])\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE3),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE3)
                                .intervals(querySegmentSpec(Intervals.of(
                                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                                .virtualColumns(new ListFilteredVirtualColumn(
                                    "v0",
                                    new DefaultDimensionSpec("dim3", "dim3", ColumnType.STRING),
                                    ImmutableSet.of("a"),
                                    true
                                ))
                                .columns("dim3", "v0")
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(new ListFilteredVirtualColumn(
                    "v0",
                    new DefaultDimensionSpec("dim3", "dim3", ColumnType.STRING),
                    ImmutableSet.of("a"),
                    true
                ))
                .columns("dim3", "j0.dim3")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{"[\"a\",\"b\"]", "[\"a\",\"b\"]"})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testVirtualColumnOnMVFilterMultiJoinExpression(Map<String, Object> queryContext) throws Exception
  {
    testQuery(
        "SELECT foo1.dim3, foo2.dim3 FROM druid.numfoo as foo1 INNER JOIN "
        + "(SELECT foo3.dim3 FROM druid.numfoo as foo3 INNER JOIN druid.numfoo as foo4 "
        + "   ON MV_FILTER_ONLY(foo3.dim3, ARRAY['a']) = MV_FILTER_ONLY(foo4.dim3, ARRAY['a'])) as foo2 "
        + "ON MV_FILTER_ONLY(foo1.dim3, ARRAY['a']) = MV_FILTER_ONLY(foo2.dim3, ARRAY['a'])\n",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE3),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(
                                    join(
                                        new TableDataSource(CalciteTests.DATASOURCE3),
                                        new QueryDataSource(
                                            newScanQueryBuilder()
                                                .dataSource(CalciteTests.DATASOURCE3)
                                                .intervals(querySegmentSpec(Intervals.of(
                                                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                                                .virtualColumns(new ListFilteredVirtualColumn(
                                                    "v0",
                                                    new DefaultDimensionSpec("dim3", "dim3", ColumnType.STRING),
                                                    ImmutableSet.of("a"),
                                                    true
                                                ))
                                                .columns("v0")
                                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                .context(queryContext)
                                                .build()
                                        ),
                                        "j0.",
                                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.v0")),
                                        JoinType.INNER
                                    )
                                )
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .virtualColumns(new ListFilteredVirtualColumn(
                                    "v0",
                                    new DefaultDimensionSpec("dim3", "dim3", ColumnType.STRING),
                                    ImmutableSet.of("a"),
                                    true
                                ))
                                .columns("dim3", "v0")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(DruidExpression.fromColumn("v0"), DruidExpression.fromColumn("j0.v0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(new ListFilteredVirtualColumn(
                    "v0",
                    new DefaultDimensionSpec("dim3", "dim3", ColumnType.STRING),
                    ImmutableSet.of("a"),
                    true
                ))
                .columns("dim3", "j0.dim3")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{"[\"a\",\"b\"]", "[\"a\",\"b\"]"})
    );
  }
}
