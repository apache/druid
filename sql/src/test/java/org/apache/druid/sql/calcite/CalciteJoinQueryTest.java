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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.security.Access;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class CalciteJoinQueryTest extends BaseCalciteQueryTest
{
  /**
   * Used for testing {@link org.apache.druid.sql.calcite.planner.JoinAlgorithm#SORT_MERGE}.
   */
  private final boolean sortBasedJoin;

  public CalciteJoinQueryTest()
  {
    this(false);
  }

  /**
   * Constructor used by MSQ subclasses. Necessary because results come in a different order when using sort-based join.
   */
  protected CalciteJoinQueryTest(boolean sortBasedJoin)
  {
    this.sortBasedJoin = sortBasedJoin;
  }

  @SqlTestFrameworkConfig(minTopNThreshold = 1)
  @Test
  public void testInnerJoinWithLimitAndAlias()
  {

    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, false);
    testQuery(
        "select t1.b1 from (select __time as b1 from numfoo group by 1 order by 1) as t1 inner join (\n"
        + "  select __time as b2 from foo group by 1 order by 1\n"
        + ") as t2 on t1.b1 = t2.b2 ",
        context, // turn on exact topN
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(
                    JoinDataSource.create(
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(new TableDataSource("numfoo"))
                                        .setDimensions(new DefaultDimensionSpec("__time", "_d0", ColumnType.LONG))
                                        .setContext(context)
                                        .build()
                        ),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(new TableDataSource("foo"))
                                        .setDimensions(new DefaultDimensionSpec("__time", "d0", ColumnType.LONG))
                                        .setContext(context)
                                        .build()
                        ),
                        "j0.",
                        "(\"_d0\" == \"j0.d0\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
                    )
                )
                .columns("_d0")
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L},
            new Object[]{946771200000L},
            new Object[]{946857600000L},
            new Object[]{978307200000L},
            new Object[]{978393600000L},
            new Object[]{978480000000L}
        )
    );
  }


  // Adjust topN threshold, so that the topN engine keeps only 1 slot for aggregates, which should be enough
  // to compute the query with limit 1.
  @SqlTestFrameworkConfig(minTopNThreshold = 1)
  @Test
  public void testExactTopNOnInnerJoinWithLimit()
  {
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
                                        .setDimFilter(not(equality("dim4", "a", ColumnType.STRING)))
                                        .setDataSource(new TableDataSource("numfoo"))
                                        .setDimensions(new DefaultDimensionSpec("dim4", "_d0"))
                                        .setContext(context)
                                        .build()
                        ),
                        "j0.",
                        "(\"dim4\" == \"j0._d0\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
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
  public void testJoinOuterGroupByAndSubqueryHasLimit()
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
                                            DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                            DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
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
                                    notNull("m2")
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
  public void testJoinOuterGroupByAndSubqueryNoLimit(Map<String, Object> queryContext)
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
                                    DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                    DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
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
                                    notNull("m2")
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
  public void testJoinWithLimitBeforeJoining()
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
                                    DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                    DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
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
                                    notNull("m2")
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
  public void testJoinOnTimeseriesWithFloorOnTime()
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100, true) FROM foo WHERE (TIME_FLOOR(__time, 'PT1H'), m1) IN\n"
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
                                          .filters(equality("dim3", "b", ColumnType.STRING))
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
                            new StringAnyAggregatorFactory("a0", "dim3", 100, true)
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
  public void testJoinOnGroupByInsteadOfTimeseriesWithFloorOnTime()
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100) FROM foo WHERE (CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 1, m1) IN\n"
        + "   (\n"
        + "     SELECT CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 1 AS t1, MIN(m1) AS t2 FROM foo WHERE dim3 = 'b'\n"
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
                                                        "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 1)",
                                                        ColumnType.LONG
                                                    )
                                                )
                                                .setDimFilter(equality("dim3", "b", ColumnType.STRING))
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
                                "(((timestamp_floor(\"__time\",'PT1H',null,'UTC') + 1) == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
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
                            new StringAnyAggregatorFactory("a0", "dim3", 100, true)
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
  public void testJoinOnGroupByInsteadOfTimeseriesWithFloorOnTimeWithNoAggregateMultipleValues()
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT CAST(__time AS BIGINT), m1, ANY_VALUE(dim3, 100, false) FROM foo WHERE (CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 1, m1) IN\n"
        + "   (\n"
        + "     SELECT CAST(TIME_FLOOR(__time, 'PT1H') AS BIGINT) + 1 AS t1, MIN(m1) AS t2 FROM foo WHERE dim3 = 'b'\n"
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
                                                        "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 1)",
                                                        ColumnType.LONG
                                                    )
                                                )
                                                .setDimFilter(equality("dim3", "b", ColumnType.STRING))
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
                                "(((timestamp_floor(\"__time\",'PT1H',null,'UTC') + 1) == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
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
                            new StringAnyAggregatorFactory("a0", "dim3", 100, false)
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f, "a"}, // picks up first from [a, b]
            new Object[]{946771200000L, 2.0f, "b"} // picks up first from [b, c]
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorWithValueFilterPushdownMatchesNothing(Map<String, Object> queryContext)

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
                                equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("j0.v", "123", ColumnType.STRING))
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
  public void testFilterAndGroupByLookupUsingJoinOperatorAllowNulls(Map<String, Object> queryContext)
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
                                equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            or(
                                isNull("j0.v"),
                                not(equality("j0.v", "xa", ColumnType.STRING))
                            )
                        )
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
  public void testFilterAndGroupByLookupUsingJoinOperatorBackwards(Map<String, Object> queryContext)
  {
    // Like "testFilterAndGroupByLookupUsingJoinOperator", but with the table and lookup reversed.

    // MSQ refuses to do RIGHT join with broadcast.
    msqIncompatible();

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
                                equalsCondition(makeColumnExpression("k"), makeColumnExpression("j0.dim2")),
                                NullHandling.sqlCompatible() ? JoinType.INNER : JoinType.RIGHT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(not(equality("v", "xa", ColumnType.STRING)))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
        : ImmutableList.of(
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorWithNotFilter(Map<String, Object> queryContext)

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
                                equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                                NullHandling.sqlCompatible() ? JoinType.INNER : JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(not(equality("j0.v", "xa", ColumnType.STRING)))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{NULL_STRING, 3L},
            new Object[]{"xabc", 1L}
        )
        : ImmutableList.of(
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinUnionTablesOnLookup(Map<String, Object> queryContext)
  {
    // MSQ does not support UNION ALL.
    msqIncompatible();
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
                                equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                                NullHandling.sqlCompatible() ? JoinType.INNER : JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(not(equality("j0.v", "xa", ColumnType.STRING)))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("j0.v", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(queryContext)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{NULL_STRING, 6L},
            new Object[]{"xabc", 2L}
        )
        : ImmutableList.of(
            new Object[]{"xabc", 2L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperator(Map<String, Object> queryContext)
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
                                equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("j0.v", "xa", ColumnType.STRING))
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
                        equalsCondition(makeColumnExpression("d0"), makeColumnExpression("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        isNull("j0.v"),
                        not(equality("j0.v", "xa", ColumnType.STRING))

                    )
                )
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
  public void testGroupByInnerJoinOnLookupUsingJoinOperator(Map<String, Object> queryContext)
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
                                equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
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
  public void testSelectOnLookupUsingInnerJoinOperator(Map<String, Object> queryContext)
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
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
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
  public void testLeftJoinTwoLookupsUsingJoinOperator(Map<String, Object> queryContext)
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
                            equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("_j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.v", "dim1", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            ImmutableList.of(
                new Object[]{"", "a", NULL_STRING, "xa"},
                new Object[]{"10.1", NULL_STRING, NULL_STRING, NULL_STRING},
                new Object[]{"2", "", NULL_STRING, NULL_STRING},
                new Object[]{"1", "a", NULL_STRING, "xa"},
                new Object[]{"def", "abc", NULL_STRING, "xabc"},
                new Object[]{"abc", NULL_STRING, "xabc", NULL_STRING}
            ),
            1
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTableLookupLookupWithFilterWithOuterLimit(Map<String, Object> queryContext)
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
                            equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .limit(100)
                .filters(equality("j0.v", "xa", ColumnType.STRING))
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
  public void testInnerJoinTableLookupLookupWithFilterWithoutLimit(Map<String, Object> queryContext)
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
                            equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(equality("j0.v", "xa", ColumnType.STRING))
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
                            equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .limit(100)
                .filters(equality("j0.v", "xa", ColumnType.STRING))
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
                            equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("_j0.k")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(equality("j0.v", "xa", ColumnType.STRING))
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
  public void testManyManyInnerJoinOnManyManyLookup(Map<String, Object> queryContext)
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
                                                                                                    makeColumnExpression(
                                                                                                        "dim2"),
                                                                                                    makeColumnExpression(
                                                                                                        "j0.k")
                                                                                                ),
                                                                                                JoinType.INNER
                                                                                            ),
                                                                                            new LookupDataSource(
                                                                                                "lookyloo"),
                                                                                            "_j0.",
                                                                                            equalsCondition(
                                                                                                makeColumnExpression(
                                                                                                    "dim2"),
                                                                                                makeColumnExpression(
                                                                                                    "_j0.k")
                                                                                            ),
                                                                                            JoinType.INNER
                                                                                        ),
                                                                                        new LookupDataSource("lookyloo"),
                                                                                        "__j0.",
                                                                                        equalsCondition(
                                                                                            makeColumnExpression(
                                                                                                "dim2"),
                                                                                            makeColumnExpression(
                                                                                                "__j0.k")
                                                                                        ),
                                                                                        JoinType.INNER
                                                                                    ),
                                                                                    new LookupDataSource("lookyloo"),
                                                                                    "___j0.",
                                                                                    equalsCondition(
                                                                                        makeColumnExpression(
                                                                                            "dim2"),
                                                                                        makeColumnExpression(
                                                                                            "___j0.k")
                                                                                    ),
                                                                                    JoinType.INNER
                                                                                ),
                                                                                new LookupDataSource("lookyloo"),
                                                                                "____j0.",
                                                                                equalsCondition(
                                                                                    makeColumnExpression("dim2"),
                                                                                    makeColumnExpression(
                                                                                        "____j0.k")
                                                                                ),
                                                                                JoinType.INNER
                                                                            ),
                                                                            new LookupDataSource("lookyloo"),
                                                                            "_____j0.",
                                                                            equalsCondition(
                                                                                makeColumnExpression("dim2"),
                                                                                makeColumnExpression("_____j0.k")
                                                                            ),
                                                                            JoinType.INNER
                                                                        ),
                                                                        new LookupDataSource("lookyloo"),
                                                                        "______j0.",
                                                                        equalsCondition(
                                                                            makeColumnExpression("dim2"),
                                                                            makeColumnExpression("______j0.k")
                                                                        ),
                                                                        JoinType.INNER
                                                                    ),
                                                                    new LookupDataSource("lookyloo"),
                                                                    "_______j0.",
                                                                    equalsCondition(
                                                                        makeColumnExpression("dim2"),
                                                                        makeColumnExpression("_______j0.k")
                                                                    ),
                                                                    JoinType.INNER
                                                                ),
                                                                new LookupDataSource("lookyloo"),
                                                                "________j0.",
                                                                equalsCondition(
                                                                    makeColumnExpression("dim2"),
                                                                    makeColumnExpression("________j0.k")
                                                                ),
                                                                JoinType.INNER
                                                            ),
                                                            new LookupDataSource("lookyloo"),
                                                            "_________j0.",
                                                            equalsCondition(
                                                                makeColumnExpression("dim2"),
                                                                makeColumnExpression("_________j0.k")
                                                            ),
                                                            JoinType.INNER
                                                        ),
                                                        new LookupDataSource("lookyloo"),
                                                        "__________j0.",
                                                        equalsCondition(
                                                            makeColumnExpression("dim2"),
                                                            makeColumnExpression("__________j0.k")
                                                        ),
                                                        JoinType.INNER
                                                    ),
                                                    new LookupDataSource("lookyloo"),
                                                    "___________j0.",
                                                    equalsCondition(
                                                        makeColumnExpression("dim2"),
                                                        makeColumnExpression("___________j0.k")
                                                    ),
                                                    JoinType.INNER
                                                ),
                                                new LookupDataSource("lookyloo"),
                                                "____________j0.",
                                                equalsCondition(
                                                    makeColumnExpression("dim2"),
                                                    makeColumnExpression("____________j0.k")
                                                ),
                                                JoinType.INNER
                                            ),
                                            new LookupDataSource("lookyloo"),
                                            "_____________j0.",
                                            equalsCondition(
                                                makeColumnExpression("dim2"),
                                                makeColumnExpression("_____________j0.k")
                                            ),
                                            JoinType.INNER
                                        ),
                                        new LookupDataSource("lookyloo"),
                                        "______________j0.",
                                        equalsCondition(
                                            makeColumnExpression("dim2"),
                                            makeColumnExpression("______________j0.k")
                                        ),
                                        JoinType.INNER
                                    ),
                                    new LookupDataSource("lookyloo"),
                                    "_______________j0.",
                                    equalsCondition(
                                        makeColumnExpression("dim2"),
                                        makeColumnExpression("_______________j0.k")
                                    ),
                                    JoinType.INNER
                                ),
                                new LookupDataSource("lookyloo"),
                                "________________j0.",
                                equalsCondition(
                                    makeColumnExpression("dim2"),
                                    makeColumnExpression("________________j0.k")
                                ),
                                JoinType.INNER
                            ),
                            new LookupDataSource("lookyloo"),
                            "_________________j0.",
                            equalsCondition(
                                makeColumnExpression("dim2"),
                                makeColumnExpression("_________________j0.k")
                            ),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "__________________j0.",
                        equalsCondition(
                            makeColumnExpression("dim2"),
                            makeColumnExpression("__________________j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(equality("j0.v", "xa", ColumnType.STRING))
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
  public void testInnerJoinQueryOfLookup(Map<String, Object> queryContext)
  {
    // Cannot vectorize the subquery.
    cannotVectorize();

    testQuery(
        "SELECT dim1, dim2, t1.v, t1.v\n"
        + "FROM foo\n"
        + "INNER JOIN \n"
        + "  (SELECT SUBSTRING(k, 1, 1) k, ANY_VALUE(v, 10) v FROM lookup.lookyloo GROUP BY 1) t1\n"
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
                                .setAggregatorSpecs(new StringAnyAggregatorFactory("a0", "v", 10, true))
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.d0")),
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
  public void testTimeColumnAggregationsOnLookups(Map<String, Object> queryContext)
  {
    try {
      testQuery(
          "SELECT k, LATEST(v) v FROM lookup.lookyloo GROUP BY k",
          queryContext,
          ImmutableList.of(),
          ImmutableList.of()
      );
      Assert.fail("Expected exception to be thrown.");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          new DruidExceptionMatcher(DruidException.Persona.ADMIN, DruidException.Category.INVALID_INPUT, "general")
              .expectMessageIs(
                  "Query could not be planned. A possible reason is "
                  + "[LATEST and EARLIEST aggregators implicitly depend on the __time column, "
                  + "but the table queried doesn't contain a __time column.  "
                  + "Please use LATEST_BY or EARLIEST_BY and specify the column explicitly.]"
              )
      );
    }
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinQueryOfLookupRemovable(Map<String, Object> queryContext)
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
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
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
  public void testInnerJoinTwoLookupsToTableUsingNumericColumn(Map<String, Object> queryContext)
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
                                  DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                                  DruidExpression.ofColumn(ColumnType.FLOAT, "j0.v0")
                              ),
                              JoinType.INNER
                          ),
                          new LookupDataSource("lookyloo"),
                          "_j0.",
                          equalsCondition(makeColumnExpression("j0.k"), makeColumnExpression("_j0.k")),
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
                                  makeColumnExpression("k"),
                                  makeColumnExpression("j0.k")
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
                              makeExpression(ColumnType.DOUBLE, "CAST(\"j0.k\", 'DOUBLE')"),
                              DruidExpression.ofColumn(ColumnType.DOUBLE, "_j0.m1")
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
  public void testInnerJoinLookupTableTable(Map<String, Object> queryContext)
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
                                        makeColumnExpression("k"),
                                        makeColumnExpression("j0.dim1")
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
                                    makeColumnExpression("k"),
                                    makeColumnExpression("_j0.dim1")
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
  public void testInnerJoinLookupTableTableChained(Map<String, Object> queryContext)
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
                                        makeColumnExpression("k"),
                                        makeColumnExpression("j0.dim1")
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
                                    makeColumnExpression("j0.dim1"),
                                    makeColumnExpression("_j0.dim1")
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
  public void testWhereInSelectNullFromLookup()
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
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.d0")),
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
  public void testCommaJoinLeftFunction()
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
                            makeExpression("substring(\"dim2\", 0, 1)"),
                            makeColumnExpression("j0.k")
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
  public void testCommaJoinTableLookupTableMismatchedTypes(Map<String, Object> queryContext)
  {
    // Regression test for https://github.com/apache/druid/issues/9646.

    // Empty-dataset aggregation queries in MSQ return an empty row, rather than a single row as SQL requires.
    msqIncompatible();

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
                          NullHandling.sqlCompatible() ?
                          equalsCondition(
                              DruidExpression.fromExpression("CAST(\"j0.k\", 'LONG')"),
                              DruidExpression.ofColumn(ColumnType.LONG, "_j0.cnt")
                          )
                                                       : "1",
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(new CountAggregatorFactory("a0"))
                  .filters(
                      NullHandling.sqlCompatible() ?
                      expressionFilter("(\"cnt\" == CAST(\"j0.k\", 'LONG'))")
                                                   : and(
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
  public void testJoinTableLookupTableMismatchedTypesWithoutComma(Map<String, Object> queryContext)
  {
    // Empty-dataset aggregation queries in MSQ return an empty row, rather than a single row as SQL requires.
    msqIncompatible();

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
                                  DruidExpression.ofColumn(ColumnType.LONG, "cnt"),
                                  DruidExpression.ofColumn(ColumnType.LONG, "j0.v0")
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
                              makeExpression(ColumnType.LONG, "CAST(\"j0.k\", 'LONG')"),
                              DruidExpression.ofColumn(ColumnType.LONG, "_j0.cnt")
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
  public void testInnerJoinCastLeft(Map<String, Object> queryContext)
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
                            makeExpression("CAST(\"m1\", 'STRING')"),
                            makeColumnExpression("j0.k")
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
  public void testInnerJoinCastRight(Map<String, Object> queryContext)
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
                        equalsCondition(
                            DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                            DruidExpression.ofColumn(ColumnType.FLOAT, "j0.v0")
                        ),
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
  public void testInnerJoinMismatchedTypes(Map<String, Object> queryContext)
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
                        equalsCondition(
                            DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                            DruidExpression.ofColumn(ColumnType.FLOAT, "j0.v0")
                        ),
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
  public void testInnerJoinLeftFunction(Map<String, Object> queryContext)
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
                            makeExpression("substring(\"dim2\", 0, 1)"),
                            makeColumnExpression("j0.k")
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
  public void testInnerJoinRightFunction(Map<String, Object> queryContext)
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
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.v0")),
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
  public void testLeftJoinLookupOntoLookupUsingJoinOperator(Map<String, Object> queryContext)
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
                            equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "_j0.",
                        equalsCondition(makeColumnExpression("j0.k"), makeColumnExpression("_j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.v", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            ImmutableList.of(
                new Object[]{"a", "xa", "xa"},
                new Object[]{NULL_STRING, NULL_STRING, NULL_STRING},
                new Object[]{"", NULL_STRING, NULL_STRING},
                new Object[]{"a", "xa", "xa"},
                new Object[]{"abc", "xabc", "xabc"},
                new Object[]{NULL_STRING, NULL_STRING, NULL_STRING}
            ),
            0
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testLeftJoinThreeLookupsUsingJoinOperator(Map<String, Object> queryContext)
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
                                equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
                                JoinType.LEFT
                            ),
                            new LookupDataSource("lookyloo"),
                            "_j0.",
                            equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("_j0.k")),
                            JoinType.LEFT
                        ),
                        new LookupDataSource("lookyloo"),
                        "__j0.",
                        equalsCondition(makeColumnExpression("_j0.k"), makeColumnExpression("__j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__j0.v", "_j0.v", "dim1", "dim2", "j0.v")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            ImmutableList.of(
                new Object[]{"", "a", NULL_STRING, "xa", "xa"},
                new Object[]{"10.1", NULL_STRING, NULL_STRING, NULL_STRING, NULL_STRING},
                new Object[]{"2", "", NULL_STRING, NULL_STRING, NULL_STRING},
                new Object[]{"1", "a", NULL_STRING, "xa", "xa"},
                new Object[]{"def", "abc", NULL_STRING, "xabc", "xabc"},
                new Object[]{"abc", NULL_STRING, "xabc", NULL_STRING, NULL_STRING}
            ),
            1,
            0
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingLeftJoinOperator(Map<String, Object> queryContext)
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
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        isNull("j0.v"),
                        not(equality("j0.v", "xxx", ColumnType.STRING))
                    )
                )
                .columns("dim1", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            ImmutableList.of(
                new Object[]{"", NULL_STRING, NULL_STRING},
                new Object[]{"10.1", NULL_STRING, NULL_STRING},
                new Object[]{"2", NULL_STRING, NULL_STRING},
                new Object[]{"1", NULL_STRING, NULL_STRING},
                new Object[]{"def", NULL_STRING, NULL_STRING},
                new Object[]{"abc", "abc", "xabc"}
            ),
            0
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingRightJoinOperator(Map<String, Object> queryContext)
  {
    // MSQ refuses to do RIGHT join with broadcast.
    msqIncompatible();

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
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
                        JoinType.RIGHT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        isNull("j0.v"),
                        not(equality("j0.v", "xxx", ColumnType.STRING))
                    )
                )
                .columns("dim1", "j0.k", "j0.v")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "abc", "xabc"},
            new Object[]{NULL_STRING, "6", "x6"},
            new Object[]{NULL_STRING, "a", "xa"},
            new Object[]{NULL_STRING, "nosuchkey", "mysteryvalue"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testSelectOnLookupUsingFullJoinOperator(Map<String, Object> queryContext)
  {
    // MSQ refuses to do FULL join with broadcast.
    msqIncompatible();

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
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
                        JoinType.FULL
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        isNull("j0.v"),
                        not(equality("j0.v", "xxx", ColumnType.STRING))
                    )
                )
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
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "6", "x6"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "a", "xa"},
            new Object[]{NULL_STRING, NULL_FLOAT, NULL_LONG, "nosuchkey", "mysteryvalue"}
        )
    );
  }


  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInAggregationSubquery(Map<String, Object> queryContext)
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    Map<String, Object> updatedQueryContext = new HashMap<>(queryContext);
    updatedQueryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    Map<String, Object> maxTimeQueryContext = new HashMap<>(queryContext);
    maxTimeQueryContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT DISTINCT __time FROM druid.foo WHERE __time IN (SELECT MAX(__time) FROM druid.foo)",
        updatedQueryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .bound(TimeBoundaryQuery.MAX_TIME)
                                          .context(maxTimeQueryContext)
                                          .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    DruidExpression.ofColumn(ColumnType.LONG, "__time"),
                                    DruidExpression.ofColumn(ColumnType.LONG, "j0.a0")
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
  public void testNotInAggregationSubquery(Map<String, Object> queryContext)
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    Map<String, Object> updatedQueryContext = new HashMap<>(queryContext);
    updatedQueryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    Map<String, Object> maxTimeQueryContext = new HashMap<>(queryContext);
    maxTimeQueryContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT DISTINCT __time FROM druid.foo WHERE __time NOT IN (SELECT MAX(__time) FROM druid.foo)",
        updatedQueryContext,
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
                                                Druids.newTimeBoundaryQueryBuilder()
                                                      .dataSource(CalciteTests.DATASOURCE1)
                                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                                      .bound(TimeBoundaryQuery.MAX_TIME)
                                                      .context(maxTimeQueryContext)
                                                      .build()
                                            )
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setAggregatorSpecs(
                                                new CountAggregatorFactory("_a0"),
                                                NullHandling.sqlCompatible()
                                                ? new FilteredAggregatorFactory(
                                                    new CountAggregatorFactory("_a1"),
                                                    notNull("a0")
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
                                          .postAggregators(expressionPostAgg("p0", "1", ColumnType.LONG))
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
                                equality("j0._a0", 0L, ColumnType.LONG),
                                and(isNull("_j0.p0"), expressionFilter("(\"j0._a1\" >= \"j0._a0\")"))
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
  public void testUsingSubqueryWithExtractionFns(Map<String, Object> queryContext)
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
                                                .setDimFilter(
                                                    not(equality("dim1", "", ColumnType.STRING))
                                                )
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
                                    makeExpression("substring(\"dim2\", 0, 1)"),
                                    makeColumnExpression("j0.d0")
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
  public void testInnerJoinWithIsNullFilter(Map<String, Object> queryContext)
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
                            makeColumnExpression("dim1"),
                            makeColumnExpression("j0.k")
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(isNull("dim2"))
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
  public void testInnerJoinOnMultiValueColumn(Map<String, Object> queryContext)
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
                                    makeColumnExpression("dim3"),
                                    makeColumnExpression("j0.k")
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
  public void testLeftJoinOnTwoInlineDataSourcesWithTimeFilter(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .virtualColumns(expressionVirtualColumn("v0", "'10.1'", ColumnType.STRING))
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .virtualColumns(expressionVirtualColumn("v0", "'10.1'", ColumnType.STRING))
                                .columns(ImmutableList.of("v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.v0")),
                        JoinType.LEFT
                    )
                )
                .virtualColumns(expressionVirtualColumn("_v0", "'10.1'", ColumnType.STRING))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("_v0", "'10.1'", ColumnType.STRING))
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
  public void testLeftJoinOnTwoInlineDataSourcesWithTimeFilter_withLeftDirectAccess(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                                .columns(ImmutableList.of("v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeExpression("'10.1'"), makeColumnExpression("j0.v0")),
                        JoinType.LEFT,
                        equality("dim1", "10.1", ColumnType.STRING)
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
  public void testLeftJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .virtualColumns(expressionVirtualColumn("v0", "'10.1'", ColumnType.STRING))
                                .columns(ImmutableList.of("__time", "v0"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .virtualColumns(expressionVirtualColumn("_v0", "'10.1'", ColumnType.STRING))
                .intervals(querySegmentSpec(Filtration.eternity()))
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
  public void testLeftJoinOnTwoInlineDataSourcesWithOuterWhere_withLeftDirectAccess(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            makeExpression("'10.1'"),
                            makeColumnExpression("j0.dim1")
                        ),
                        JoinType.LEFT,
                        equality("dim1", "10.1", ColumnType.STRING)
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
  public void testLeftJoinOnTwoInlineDataSources(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.dim1")),
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
  public void testLeftJoinOnTwoInlineDataSources_withLeftDirectAccess(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            makeExpression("'10.1'"),
                            makeColumnExpression("j0.dim1")
                        ),
                        JoinType.LEFT,
                        equality("dim1", "10.1", ColumnType.STRING)
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
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext)
  {
    Druids.ScanQueryBuilder baseScanBuilder = newScanQueryBuilder()
        .dataSource(
            join(
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .eternityInterval()
                        .filters(equality("dim1", "10.1", ColumnType.STRING))
                        .virtualColumns(expressionVirtualColumn("v0", "'10.1'", ColumnType.STRING))
                        .columns(ImmutableList.of("__time", "v0"))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .eternityInterval()
                        .filters(equality("dim1", "10.1", ColumnType.STRING))
                        .columns(ImmutableList.of("dim1"))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.dim1")),
                JoinType.INNER
            )
        )
        .virtualColumns(expressionVirtualColumn("_v0", "'10.1'", ColumnType.STRING))
        .intervals(querySegmentSpec(Filtration.eternity()))
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
            baseScanBuilder.build()),
        ImmutableList.of(
            new Object[]{"10.1", 946771200000L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere_withLeftDirectAccess(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            makeExpression("'10.1'"),
                            makeColumnExpression("j0.dim1")
                        ),
                        JoinType.INNER,
                        equality("dim1", "10.1", ColumnType.STRING)
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
  public void testInnerJoinOnTwoInlineDataSources(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.dim1")),
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
  public void testGroupByOverGroupByOverInnerJoinOnTwoInlineDataSources(Map<String, Object> queryContext)
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "with abc as\n"
        + "(\n"
        + "  SELECT dim1, \"__time\", m1 from foo WHERE \"dim1\" = '10.1'\n"
        + ")\n"
        + "SELECT dim1 from (SELECT dim1,__time FROM (SELECT t1.dim1, t1.\"__time\" from abc as t1 INNER JOIN abc as t2 on t1.dim1 = t2.dim1) GROUP BY 1,2) GROUP BY dim1\n",
        queryContext,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(
                    new QueryDataSource(
                        GroupByQuery.builder()
                                    .setDataSource(
                                        join(
                                            new QueryDataSource(
                                                newScanQueryBuilder()
                                                    .dataSource(CalciteTests.DATASOURCE1)
                                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                                    .filters(equality("dim1", "10.1", ColumnType.STRING))
                                                    .virtualColumns(expressionVirtualColumn(
                                                        "v0",
                                                        "\'10.1\'",
                                                        ColumnType.STRING
                                                    ))
                                                    .columns(ImmutableList.of("__time", "v0"))
                                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                    .context(queryContext)
                                                    .build()
                                            ),
                                            new QueryDataSource(
                                                newScanQueryBuilder()
                                                    .dataSource(CalciteTests.DATASOURCE1)
                                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                                    .filters(equality("dim1", "10.1", ColumnType.STRING))
                                                    .columns(ImmutableList.of("dim1"))
                                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                                    .context(queryContext)
                                                    .build()
                                            ),
                                            "j0.",
                                            equalsCondition(
                                                makeColumnExpression("v0"),
                                                makeColumnExpression("j0.dim1")
                                            ),
                                            JoinType.INNER
                                        ))
                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                    .setVirtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ColumnType.STRING))
                                    .setGranularity(Granularities.ALL)
                                    .setDimensions(new DefaultDimensionSpec(
                                        "_v0",
                                        "d0",
                                        ColumnType.STRING
                                    ), new DefaultDimensionSpec(
                                        "__time",
                                        "d1",
                                        ColumnType.LONG
                                    ))
                                    .setContext(queryContext)
                                    .build()
                    )
                )
                .setVirtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ColumnType.STRING))
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(new DefaultDimensionSpec(
                    "v0",
                    "_d0",
                    ColumnType.STRING
                ))
                .setContext(queryContext)
                .setGranularity(Granularities.ALL)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinOnTwoInlineDataSources_withLeftDirectAccess(Map<String, Object> queryContext)
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
                                .filters(equality("dim1", "10.1", ColumnType.STRING))
                                .columns(ImmutableList.of("dim1"))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(
                            makeExpression("'10.1'"),
                            makeColumnExpression("j0.dim1")
                        ),
                        JoinType.INNER,
                        equality("dim1", "10.1", ColumnType.STRING)
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
    assertQueryIsUnplannable(
        "SELECT t1.dim1 from foo as t1 LEFT JOIN foo as t2 on t1.dim1 = '10.1'",
        "SQL is resulting in a join that has unsupported operand types."
    );
  }

  @Test
  public void testLeftJoinRightTableCanBeEmpty()
  {
    // HashJoinSegmentStorageAdapter is not vectorizable
    cannotVectorize();

    final DataSource rightTable;
    rightTable = new QueryDataSource(
        Druids.newScanQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(querySegmentSpec(Filtration.eternity()))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .filters(equality("m2", "1000", ColumnType.DOUBLE))
              .columns("dim2")
              .legacy(false)
              .build()
    );


    testQuery(
        "SELECT v1.dim2, count(1) "
        + "FROM (SELECT * FROM foo where m1 > 2) v1 "
        + "LEFT OUTER JOIN ("
        + "  select dim2 from (select * from foo where m2 = 1000)"
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
                                  .filters(range(
                                      "m1",
                                      ColumnType.LONG,
                                      2L,
                                      null,
                                      true,
                                      false
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
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
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
  public void testLeftJoinSubqueryWithNullKeyFilter(Map<String, Object> queryContext)
  {
    // JoinFilterAnalyzer bug causes incorrect results on this test in replace-with-default mode.
    // This test case was originally added in https://github.com/apache/druid/pull/11434 with a note about this.
    Assume.assumeFalse(NullHandling.replaceWithDefault() && QueryContext.of(queryContext).getEnableJoinFilterRewrite());

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
                equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.d0")),
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
                equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.d0")),
                JoinType.LEFT
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("dim1", "j0.d0")
        .filters(notNull("j0.d0"))
        .context(queryContext)
        .build();

    boolean isJoinFilterRewriteEnabled = queryContext.getOrDefault(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
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
  public void testLeftJoinSubqueryWithSelectorFilter(Map<String, Object> queryContext)
  {
    // Cannot vectorize due to 'concat' expression.
    cannotVectorize();

    // disable the cost model where inner join is treated like a filter
    // this leads to cost(left join) < cost(converted inner join) for the below query
    queryContext = QueryContexts.override(
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
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.d0")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.d0")
                .filters(equality("j0.d0", "abc", ColumnType.STRING))
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
  public void testLeftJoinWithNotNullFilter(Map<String, Object> queryContext)
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
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.dim1")),
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.dim1")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            ImmutableList.of(
                new Object[]{"", ""},
                new Object[]{"10.1", "10.1"},
                new Object[]{"2", "2"},
                new Object[]{"1", "1"},
                new Object[]{"def", "def"},
                new Object[]{"abc", "abc"}
            ),
            0
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoin(Map<String, Object> queryContext)
  {
    testQuery(
        "SELECT s.dim1, t.dim1\n"
        + "FROM foo as s\n"
        + "INNER JOIN foo as t "
        + "ON s.dim1 = t.dim1",
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
                        "(\"dim1\" == \"j0.dim1\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "j0.dim1")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            NullHandling.sqlCompatible()
            ? ImmutableList.of(
                new Object[]{"", ""},
                new Object[]{"10.1", "10.1"},
                new Object[]{"2", "2"},
                new Object[]{"1", "1"},
                new Object[]{"def", "def"},
                new Object[]{"abc", "abc"}
            )
            : ImmutableList.of(
                new Object[]{"10.1", "10.1"},
                new Object[]{"2", "2"},
                new Object[]{"1", "1"},
                new Object[]{"def", "def"},
                new Object[]{"abc", "abc"}
            ),
            0
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinWithExplicitIsNotDistinctFromCondition(Map<String, Object> queryContext)
  {
    // Like "testInnerJoin", but uses IS NOT DISTINCT FROM instead of equals.

    testQuery(
        "SELECT s.dim1, t.dim1\n"
        + "FROM foo as s\n"
        + "INNER JOIN foo as t "
        + "ON s.dim1 IS NOT DISTINCT FROM t.dim1",
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
                        "notdistinctfrom(\"dim1\",\"j0.dim1\")",
                        JoinType.INNER
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
  public void testInnerJoinSubqueryWithSelectorFilter(Map<String, Object> queryContext)
  {
    if (sortBasedJoin) {
      // Cannot handle the [l1.k = 'abc'] condition.
      msqIncompatible();
    }

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
                            equalsCondition(
                                makeColumnExpression("dim1"),
                                makeColumnExpression("j0.d0")
                            ),
                            equalsCondition(
                                makeExpression("'abc'"),
                                makeColumnExpression("j0.d0")
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
  public void testSemiJoinWithOuterTimeExtractScan()
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
                                        .setDimFilter(equality("dim1", "def", ColumnType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.d0")),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                )
                .filters(
                    not(equality("dim1", "", ColumnType.STRING))
                )
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
  public void testTwoSemiJoinsSimultaneously(Map<String, Object> queryContext)
  {
    // Fully removing the join allows this query to vectorize.
    if (!isRewriteJoinToFilter(queryContext)) {
      cannotVectorize();
    }

    Map<String, Object> updatedQueryContext = new HashMap<>(queryContext);
    updatedQueryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    Map<String, Object> maxTimeQueryContext = new HashMap<>(queryContext);
    maxTimeQueryContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT dim1, COUNT(*) FROM foo\n"
        + "WHERE dim1 IN ('abc', 'def')"
        + "AND __time IN (SELECT MAX(__time) FROM foo WHERE cnt = 1)\n"
        + "AND __time IN (SELECT MAX(__time) FROM foo WHERE cnt <> 2)\n"
        + "GROUP BY 1",
        updatedQueryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        Druids.newTimeBoundaryQueryBuilder()
                                              .dataSource(CalciteTests.DATASOURCE1)
                                              .intervals(querySegmentSpec(Filtration.eternity()))
                                              .bound(TimeBoundaryQuery.MAX_TIME)
                                              .filters(equality("cnt", 1L, ColumnType.LONG))
                                              .context(maxTimeQueryContext)
                                              .build()
                                    ),
                                    "j0.",
                                    "(\"__time\" == \"j0.a0\")",
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .bound(TimeBoundaryQuery.MAX_TIME)
                                          .filters(not(equality("cnt", 2L, ColumnType.LONG)))
                                          .context(maxTimeQueryContext)
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
  public void testSemiAndAntiJoinSimultaneouslyUsingWhereInSubquery(Map<String, Object> queryContext)
  {
    cannotVectorize();

    Map<String, Object> updatedQueryContext = new HashMap<>(queryContext);
    updatedQueryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    Map<String, Object> minTimeQueryContext = new HashMap<>(queryContext);
    minTimeQueryContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    Map<String, Object> maxTimeQueryContext = new HashMap<>(queryContext);
    maxTimeQueryContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT dim1, COUNT(*) FROM foo\n"
        + "WHERE dim1 IN ('abc', 'def')\n"
        + "AND __time IN (SELECT MAX(__time) FROM foo)\n"
        + "AND __time NOT IN (SELECT MIN(__time) FROM foo)\n"
        + "GROUP BY 1",
        updatedQueryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    join(
                                        new TableDataSource(CalciteTests.DATASOURCE1),
                                        new QueryDataSource(
                                            Druids.newTimeBoundaryQueryBuilder()
                                                  .dataSource(CalciteTests.DATASOURCE1)
                                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                                  .bound(TimeBoundaryQuery.MAX_TIME)
                                                  .context(maxTimeQueryContext)
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
                                                            Druids.newTimeBoundaryQueryBuilder()
                                                                  .dataSource(CalciteTests.DATASOURCE1)
                                                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                                                  .bound(TimeBoundaryQuery.MIN_TIME)
                                                                  .context(minTimeQueryContext)
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
                                                            notNull("a0")
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
                                          .postAggregators(expressionPostAgg("p0", "1", ColumnType.LONG))
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
                                    equality("_j0._a0", 0L, ColumnType.LONG),
                                    and(
                                        isNull("__j0.p0"),
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
  public void testSemiAndAntiJoinSimultaneouslyUsingExplicitJoins(Map<String, Object> queryContext)
  {
    cannotVectorize();

    Map<String, Object> updatedQueryContext = new HashMap<>(queryContext);
    updatedQueryContext.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, true);
    Map<String, Object> minTimeQueryContext = new HashMap<>(queryContext);
    minTimeQueryContext.put(TimeBoundaryQuery.MIN_TIME_ARRAY_OUTPUT_NAME, "a0");
    Map<String, Object> maxTimeQueryContext = new HashMap<>(queryContext);
    maxTimeQueryContext.put(TimeBoundaryQuery.MAX_TIME_ARRAY_OUTPUT_NAME, "a0");
    testQuery(
        "SELECT dim1, COUNT(*) FROM\n"
        + "foo\n"
        + "INNER JOIN (SELECT MAX(__time) t FROM foo) t0 on t0.t = foo.__time\n"
        + "LEFT JOIN (SELECT MIN(__time) t FROM foo) t1 on t1.t = foo.__time\n"
        + "WHERE dim1 IN ('abc', 'def') AND t1.t is null\n"
        + "GROUP BY 1",
        updatedQueryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        Druids.newTimeBoundaryQueryBuilder()
                                              .dataSource(CalciteTests.DATASOURCE1)
                                              .intervals(querySegmentSpec(Filtration.eternity()))
                                              .bound(TimeBoundaryQuery.MAX_TIME)
                                              .context(maxTimeQueryContext)
                                              .build()
                                    ),
                                    "j0.",
                                    "(\"__time\" == \"j0.a0\")",
                                    JoinType.INNER
                                ),
                                new QueryDataSource(
                                    Druids.newTimeBoundaryQueryBuilder()
                                          .dataSource(CalciteTests.DATASOURCE1)
                                          .intervals(querySegmentSpec(Filtration.eternity()))
                                          .bound(TimeBoundaryQuery.MIN_TIME)
                                          .context(minTimeQueryContext)
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
                                isNull("_j0.a0")
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
  public void testSemiJoinWithOuterTimeExtractAggregateWithOrderBy()
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
                                .setDimFilter(equality("dim1", "def", ColumnType.STRING))
                                .setContext(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("dim2"), makeColumnExpression("j0.d0")),
                        JoinType.INNER
                    )
                )
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ColumnType.LONG)
                )
                .setDimFilter(
                    not(
                        NullHandling.replaceWithDefault()
                        ? isNull("dim1")
                        : equality("dim1", "", ColumnType.STRING)
                    )
                )
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
                .setLimitSpec(NoopLimitSpec.instance())
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
  public void testJoinOnMultiValuedColumnShouldThrowException(Map<String, Object> queryContext)
  {
    // MSQ throws a slightly different error than QueryException.
    msqIncompatible();

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
  public void testUnionAllTwoQueriesLeftQueryIsJoin(Map<String, Object> queryContext)
  {
    // MSQ does not support UNION ALL.
    msqIncompatible();

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
                          equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
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
  public void testUnionAllTwoQueriesRightQueryIsJoin(Map<String, Object> queryContext)
  {
    // MSQ does not support UNION ALL.
    msqIncompatible();

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
                          equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
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
  public void testUnionAllTwoQueriesBothQueriesAreJoin()
  {
    // MSQ does not support UNION ALL.
    msqIncompatible();
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
                          equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
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
                          equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
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
  public void testTopNFilterJoin(Map<String, Object> queryContext)
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
                                    makeColumnExpression("dim2"),
                                    makeColumnExpression("j0.d0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(NoopLimitSpec.instance())
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
  public void testTopNFilterJoinWithProjection(Map<String, Object> queryContext)
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
                                    makeColumnExpression("dim2"),
                                    makeColumnExpression("j0.d0")
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
  public void testRemovableLeftJoin(Map<String, Object> queryContext)
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
                        .setLimitSpec(NoopLimitSpec.instance())
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
  public void testCountDistinctOfLookupUsingJoinOperator(Map<String, Object> queryContext)
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
                          equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.k")),
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
  public void testJoinWithNonEquiCondition(Map<String, Object> queryContext)
  {
    // Native JOIN operator cannot handle the condition, so a SQL JOIN with greater-than is translated into a
    // cross join with a filter.
    cannotVectorize();

    // We don't handle non-equi join conditions for non-sql compatible mode.
    Assume.assumeFalse(NullHandling.replaceWithDefault());

    testQuery(
        "SELECT x.m1, y.m1 FROM foo x INNER JOIN foo y ON x.m1 > y.m1",
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
                                .columns("m1")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        "1",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(expressionFilter("(\"m1\" > \"j0.m1\")"))
                .columns("j0.m1", "m1")
                .context(queryContext)
                .build()
        ),
        sortIfSortBased(
            ImmutableList.of(
                new Object[]{2.0f, 1.0f},
                new Object[]{3.0f, 1.0f},
                new Object[]{3.0f, 2.0f},
                new Object[]{4.0f, 1.0f},
                new Object[]{4.0f, 2.0f},
                new Object[]{4.0f, 3.0f},
                new Object[]{5.0f, 1.0f},
                new Object[]{5.0f, 2.0f},
                new Object[]{5.0f, 3.0f},
                new Object[]{5.0f, 4.0f},
                new Object[]{6.0f, 1.0f},
                new Object[]{6.0f, 2.0f},
                new Object[]{6.0f, 3.0f},
                new Object[]{6.0f, 4.0f},
                new Object[]{6.0f, 5.0f}
            ),
            1,
            0
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinWithEquiAndNonEquiCondition(Map<String, Object> queryContext)
  {
    // Native JOIN operator cannot handle the condition, so a SQL JOIN with greater-than is translated into a
    // cross join with a filter.
    cannotVectorize();

    // We don't handle non-equi join conditions for non-sql compatible mode.
    Assume.assumeFalse(NullHandling.replaceWithDefault());

    testQuery(
        "SELECT x.m1, y.m1 FROM foo x INNER JOIN foo y ON x.m1 = y.m1 AND x.m1 + y.m1 = 6.0",
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
                                .columns("m1")
                                .context(queryContext)
                                .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("m1"), makeColumnExpression("j0.m1")),
                        JoinType.INNER
                    )
                )
                .virtualColumns(expressionVirtualColumn("v0", "(\"m1\" + \"j0.m1\")", ColumnType.DOUBLE))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    equality("v0", 6.0, ColumnType.DOUBLE)
                )
                .columns("j0.m1", "m1")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{3.0f, 3.0f})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryAsPartOfAndFilter(Map<String, Object> queryContext)
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
                                                .setDimFilter(
                                                    not(
                                                        NullHandling.replaceWithDefault()
                                                        ? isNull("dim1")
                                                        : equality("dim1", "", ColumnType.STRING)
                                                    )
                                                )
                                                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                                .setContext(QUERY_CONTEXT_DEFAULT)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    makeColumnExpression("dim2"),
                                    makeColumnExpression("j0.d0")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(not(equality("dim1", "xxx", ColumnType.STRING)))
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
  public void testUsingSubqueryAsPartOfOrFilter(Map<String, Object> queryContext)
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();
    skipVectorize();
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
                                                .setDimFilter(new LikeDimFilter("dim1", "%bc", null, null))
                                                .setDimensions(
                                                    dimensions(
                                                        new DefaultDimensionSpec("dim1", "d0")
                                                    )
                                                )
                                                .setPostAggregatorSpecs(expressionPostAgg("a0", "1", ColumnType.LONG))
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "_j0.",
                                equalsCondition(
                                    makeColumnExpression("dim2"),
                                    makeColumnExpression("_j0.d0")
                                ),
                                JoinType.LEFT
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            or(
                                equality("dim1", "xxx", ColumnType.STRING),
                                and(
                                    not(equality("j0.a0", 0L, ColumnType.LONG)),
                                    notNull("_j0.a0"),
                                    notNull("dim2")
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
  public void testNestedGroupByOnInlineDataSourceWithFilter(Map<String, Object> queryContext)
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
                                    makeColumnExpression("dim1"),
                                    makeColumnExpression("j0.dim1")
                                ),
                                JoinType.INNER
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(equality("dim1", "def", ColumnType.STRING))
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
                    makeColumnExpression("dim1"),
                    makeColumnExpression("j0.dim1")
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

    QueryLifecycle ql = queryFramework().queryLifecycle();
    Sequence seq = ql.runSimple(query, CalciteTests.SUPER_USER_AUTH_RESULT, Access.OK).getResults();
    List<Object> results = seq.toList();
    Assert.assertEquals(
        ImmutableList.of(ResultRow.of("def")),
        results
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCountOnSemiJoinSingleColumn(Map<String, Object> queryContext)
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
                                            equality("dim1", "10.1", ColumnType.STRING)
                                        )
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                        .setContext(queryContext)
                                        .build()
                        ),
                        "j0.",
                        equalsCondition(makeColumnExpression("dim1"), makeColumnExpression("j0.d0")),
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
  public void testTopNOnStringWithNonSortedOrUniqueDictionary(Map<String, Object> queryContext)
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
                            makeColumnExpression("dim4"),
                            makeColumnExpression("j0.dim4")
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
                            makeColumnExpression("dim4"),
                            makeColumnExpression("j0.dim4")
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
  public void testVirtualColumnOnMVFilterJoinExpression(Map<String, Object> queryContext)
  {
    // Doesn't work in MSQ, although it's not really MSQ's fault. In MSQ, the second field (foo2.dim3) is returned as
    // the string "[a, b]" because it gets run through DimensionHandlerUtils.convertObjectToString in
    // IndexedTableDimensionSelector. In native, this doesn't happen, because we don't have as much type information,
    // and we end up using IndexedTableColumnValueSelector instead. This is really a problem with
    // IndexedTableColumnSelectorFactory: it assumes strings are not multi-valued, even though they might be.
    msqIncompatible();

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
                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.v0")),
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
  public void testVirtualColumnOnMVFilterMultiJoinExpression(Map<String, Object> queryContext)
  {
    // Doesn't work in MSQ, although it's not really MSQ's fault. In MSQ, the second field (foo2.dim3) is returned as
    // the string "[a, b]" because it gets run through DimensionHandlerUtils.convertObjectToString in
    // IndexedTableDimensionSelector. In native, this doesn't happen, because we don't have as much type information,
    // and we end up using IndexedTableColumnValueSelector instead. This is really a problem with
    // IndexedTableColumnSelectorFactory: it assumes strings are not multi-valued, even though they might be.
    msqIncompatible();

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
                                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("j0.v0")),
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
                        "_j0.",
                        equalsCondition(makeColumnExpression("v0"), makeColumnExpression("_j0.v0")),
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
                .columns("_j0.dim3", "dim3")
                .context(queryContext)
                .build()
        ),
        ImmutableList.of(new Object[]{"[\"a\",\"b\"]", "[\"a\",\"b\"]"})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinWithFilterPushdownAndManyFiltersEmptyResults(Map<String, Object> queryContext)
  {
    // create the query we expect
    ScanQuery query = newScanQueryBuilder()
        .dataSource(
            join(
                new TableDataSource(CalciteTests.DATASOURCE1),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(new TableDataSource("foo"))
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .columns("m1")
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(
                    DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                    DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
                ),
                JoinType.INNER
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("j0.m1", "m1")
        .filters(or(
            and(
                equality("dim2", "D", ColumnType.STRING),
                in("dim1", ImmutableList.of("A", "C"), null)
            ),
            and(
                equality("dim2", "C", ColumnType.STRING),
                in("dim1", ImmutableList.of("A", "B"), null)
            ),
            and(
                equality("dim2", "E", ColumnType.STRING),
                in("dim1", ImmutableList.of("C", "H"), null)
            ),
            and(
                equality("dim2", "Q", ColumnType.STRING),
                in("dim1", ImmutableList.of("P", "S"), null)
            ),
            and(
                equality("dim1", "A", ColumnType.STRING),
                equality("dim2", "B", ColumnType.STRING)
            ),
            and(
                equality("dim1", "D", ColumnType.STRING),
                equality("dim2", "H", ColumnType.STRING)
            ),
            and(
                equality("dim1", "I", ColumnType.STRING),
                equality("dim2", "J", ColumnType.STRING)
            ),
            and(
                equality("dim1", "I", ColumnType.STRING),
                equality("dim2", "K", ColumnType.STRING)
            ),
            and(
                equality("dim1", "J", ColumnType.STRING),
                equality("dim2", "I", ColumnType.STRING)
            ),
            and(
                equality("dim1", "Q", ColumnType.STRING),
                equality("dim2", "R", ColumnType.STRING)
            ),
            and(
                equality("dim1", "Q", ColumnType.STRING),
                equality("dim2", "S", ColumnType.STRING)
            ),
            and(
                equality("dim1", "X", ColumnType.STRING),
                equality("dim2", "Y", ColumnType.STRING)
            ),
            and(
                equality("dim1", "Z", ColumnType.STRING),
                equality("dim2", "U", ColumnType.STRING)
            ),
            and(
                equality("dim1", "U", ColumnType.STRING),
                equality("dim2", "Z", ColumnType.STRING)
            ),
            and(
                equality("dim1", "X", ColumnType.STRING),
                equality("dim2", "A", ColumnType.STRING)
            )
        ))
        .context(queryContext)
        .build();

    Assert.assertTrue("filter pushdown must be enabled", query.context().getEnableJoinFilterPushDown());

    // no results will be produced since the filter values aren't in the table
    testQuery(
        "SELECT f1.m1, f2.m1\n"
        + "FROM foo f1\n"
        + "INNER JOIN foo f2 ON f1.m1 = f2.m1 where (f1.dim1, f1.dim2) in (('A', 'B'), ('C', 'D'), ('A', 'C'), ('C', 'E'), ('D', 'H'), ('A', 'D'), ('B', 'C'), \n"
        + "('H', 'E'), ('I', 'J'), ('I', 'K'), ('J', 'I'), ('Q', 'R'), ('Q', 'S'), ('S', 'Q'), ('X', 'Y'), ('Z', 'U'), ('U', 'Z'), ('P', 'Q'), ('X', 'A'))\n",
        queryContext,
        ImmutableList.of(query),
        ImmutableList.of()
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinWithFilterPushdownAndManyFiltersNonEmptyResults(Map<String, Object> queryContext)
  {
    // create the query we expect
    ScanQuery query = newScanQueryBuilder()
        .dataSource(
            join(
                new TableDataSource(CalciteTests.DATASOURCE1),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(new TableDataSource("foo"))
                        .intervals(querySegmentSpec(Filtration.eternity()))
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .columns("m1")
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition(
                    DruidExpression.ofColumn(ColumnType.FLOAT, "m1"),
                    DruidExpression.ofColumn(ColumnType.FLOAT, "j0.m1")
                ),
                JoinType.INNER
            )
        )
        .intervals(querySegmentSpec(Filtration.eternity()))
        .columns("j0.m1", "m1")
        .filters(or(
            and(
                equality("dim2", "D", ColumnType.STRING),
                in("dim1", ImmutableList.of("A", "C"), null)
            ),
            and(
                equality("dim2", "C", ColumnType.STRING),
                in("dim1", ImmutableList.of("A", "B"), null)
            ),
            and(
                equality("dim2", "E", ColumnType.STRING),
                in("dim1", ImmutableList.of("C", "H"), null)
            ),
            and(
                equality("dim2", "Q", ColumnType.STRING),
                in("dim1", ImmutableList.of("P", "S"), null)
            ),
            and(
                equality("dim1", "1", ColumnType.STRING),
                equality("dim2", "a", ColumnType.STRING)
            ),
            and(
                equality("dim1", "D", ColumnType.STRING),
                equality("dim2", "H", ColumnType.STRING)
            ),
            and(
                equality("dim1", "I", ColumnType.STRING),
                equality("dim2", "J", ColumnType.STRING)
            ),
            and(
                equality("dim1", "I", ColumnType.STRING),
                equality("dim2", "K", ColumnType.STRING)
            ),
            and(
                equality("dim1", "J", ColumnType.STRING),
                equality("dim2", "I", ColumnType.STRING)
            ),
            and(
                equality("dim1", "Q", ColumnType.STRING),
                equality("dim2", "R", ColumnType.STRING)
            ),
            and(
                equality("dim1", "Q", ColumnType.STRING),
                equality("dim2", "S", ColumnType.STRING)
            ),
            and(
                equality("dim1", "X", ColumnType.STRING),
                equality("dim2", "Y", ColumnType.STRING)
            ),
            and(
                equality("dim1", "Z", ColumnType.STRING),
                equality("dim2", "U", ColumnType.STRING)
            ),
            and(
                equality("dim1", "U", ColumnType.STRING),
                equality("dim2", "Z", ColumnType.STRING)
            ),
            and(
                equality("dim1", "X", ColumnType.STRING),
                equality("dim2", "A", ColumnType.STRING)
            )
        ))
        .context(queryContext)
        .build();

    Assert.assertTrue("filter pushdown must be enabled", query.context().getEnableJoinFilterPushDown());

    // (dim1, dim2, m1) in foo look like
    // [, a, 1.0]
    // [10.1, , 2.0]
    // [2, , 3.0]
    // [1, a, 4.0]
    // [def, abc, 5.0]
    // [abc, , 6.0]
    // So (1, a) filter will produce results for 4.0
    testQuery(
        "SELECT f1.m1, f2.m1\n"
        + "FROM foo f1\n"
        + "INNER JOIN foo f2 ON f1.m1 = f2.m1 where (f1.dim1, f1.dim2) in (('1', 'a'), ('C', 'D'), ('A', 'C'), ('C', 'E'), ('D', 'H'), ('A', 'D'), ('B', 'C'), \n"
        + "('H', 'E'), ('I', 'J'), ('I', 'K'), ('J', 'I'), ('Q', 'R'), ('Q', 'S'), ('S', 'Q'), ('X', 'Y'), ('Z', 'U'), ('U', 'Z'), ('P', 'Q'), ('X', 'A'))\n",
        queryContext,
        ImmutableList.of(query),
        ImmutableList.of(new Object[]{4.0F, 4.0F})
    );
  }

  @Test
  public void testPlanWithInFilterMoreThanInSubQueryThreshold()
  {
    String query = "SELECT l1 FROM numfoo WHERE l1 IN (4842, 4844, 4845, 14905, 4853, 29064)";

    Map<String, Object> queryContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    queryContext.put(QueryContexts.IN_SUB_QUERY_THRESHOLD_KEY, 3);

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        queryContext,
        DEFAULT_PARAMETERS,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      JoinDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          InlineDataSource.fromIterable(
                              ImmutableList.of(
                                  new Object[]{4842L},
                                  new Object[]{4844L},
                                  new Object[]{4845L},
                                  new Object[]{14905L},
                                  new Object[]{4853L},
                                  new Object[]{29064L}
                              ),
                              RowSignature.builder()
                                          .add("ROW_VALUE", ColumnType.LONG)
                                          .build()
                          ),
                          "j0.",
                          "(\"l1\" == \"j0.ROW_VALUE\")",
                          JoinType.INNER,
                          null,
                          ExprMacroTable.nil(),
                          CalciteTests.createJoinableFactoryWrapper()
                      )
                  )
                  .columns("l1")
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .context(queryContext)
                  .legacy(false)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .build()
        ),
        (sql, result) -> {
          // Ignore the results, only need to check that the type of query is a join.
        },
        null
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testRegressionFilteredAggregatorsSubqueryJoins(Map<String, Object> queryContext)
  {
    cannotVectorize();
    testQuery(
        "select\n" +
        "count(*) filter (where trim(both from dim1) in (select dim2 from foo)),\n" +
        "min(m1) filter (where 'A' not in (select m2 from foo))\n" +
        "from foo as t0\n" +
        "where __time in (select __time from foo)",
        queryContext,
        useDefault ?
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              join(
                                  new TableDataSource(CalciteTests.DATASOURCE1),
                                  new QueryDataSource(
                                      GroupByQuery.builder()
                                                  .setDataSource(CalciteTests.DATASOURCE1)
                                                  .setInterval(querySegmentSpec(Filtration.eternity()))
                                                  .setDimensions(
                                                      new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)
                                                  )
                                                  .setGranularity(Granularities.ALL)
                                                  .setLimitSpec(NoopLimitSpec.instance())
                                                  .build()
                                  ),
                                  "j0.",
                                  equalsCondition(makeColumnExpression("__time"), makeColumnExpression("j0.d0")),
                                  JoinType.INNER
                              ),
                              new QueryDataSource(
                                  GroupByQuery.builder()
                                              .setDataSource(CalciteTests.DATASOURCE1)
                                              .setInterval(querySegmentSpec(Filtration.eternity()))
                                              .setDimensions(
                                                  new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING)
                                              )
                                              .setGranularity(Granularities.ALL)
                                              .setPostAggregatorSpecs(
                                                  expressionPostAgg("a0", "1", ColumnType.LONG)
                                              )
                                              .setLimitSpec(NoopLimitSpec.instance())
                                              .build()
                              ),
                              "_j0.",
                              "(trim(\"dim1\",' ') == \"_j0.d0\")",
                              JoinType.LEFT
                          ),
                          new QueryDataSource(
                              GroupByQuery.builder()
                                          .setDataSource(CalciteTests.DATASOURCE1)
                                          .setInterval(querySegmentSpec(Filtration.eternity()))
                                          .setVirtualColumns(expressionVirtualColumn("v0", "1", ColumnType.LONG))
                                          .setDimFilter(equality("m2", "0.0", ColumnType.STRING))
                                          .setDimensions(
                                              new DefaultDimensionSpec("v0", "d0", ColumnType.LONG)
                                          )
                                          .setVirtualColumns(expressionVirtualColumn("v0", "1", ColumnType.LONG))
                                          .setGranularity(Granularities.ALL)
                                          .setLimitSpec(NoopLimitSpec.instance())
                                          .build()
                          ),
                          "__j0.",
                          "1",
                          JoinType.LEFT
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .aggregators(
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0"),
                          and(
                              notNull("_j0.a0"),
                              notNull("dim1")
                          ),
                          "a0"
                      ),
                      new FilteredAggregatorFactory(
                          new FloatMinAggregatorFactory("a1", "m1"),
                          isNull("__j0.d0"),
                          "a1"
                      )
                  )
                  .context(queryContext)
                  .build()
        ) :
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          join(
                              join(
                                  new TableDataSource(CalciteTests.DATASOURCE1),
                                  new QueryDataSource(
                                      GroupByQuery.builder()
                                                  .setDataSource(CalciteTests.DATASOURCE1)
                                                  .setInterval(querySegmentSpec(Filtration.eternity()))
                                                  .setDimensions(
                                                      new DefaultDimensionSpec("__time", "d0", ColumnType.LONG)
                                                  )
                                                  .setGranularity(Granularities.ALL)
                                                  .setLimitSpec(NoopLimitSpec.instance())
                                                  .build()
                                  ),
                                  "j0.",
                                  equalsCondition(makeColumnExpression("__time"), makeColumnExpression("j0.d0")),
                                  JoinType.INNER
                              ),
                              new QueryDataSource(
                                  GroupByQuery.builder()
                                              .setDataSource(CalciteTests.DATASOURCE1)
                                              .setInterval(querySegmentSpec(Filtration.eternity()))
                                              .setDimensions(
                                                  new DefaultDimensionSpec("dim2", "d0", ColumnType.STRING)
                                              )
                                              .setPostAggregatorSpecs(
                                                  expressionPostAgg("a0", "1", ColumnType.LONG)
                                              )
                                              .setGranularity(Granularities.ALL)
                                              .setLimitSpec(NoopLimitSpec.instance())
                                              .build()
                              ),
                              "_j0.",
                              "(trim(\"dim1\",' ') == \"_j0.d0\")",
                              JoinType.LEFT
                          ),
                          new QueryDataSource(
                              new TopNQueryBuilder().dataSource(CalciteTests.DATASOURCE1)
                                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                                    .filters(isNull("m2"))
                                                    .virtualColumns(expressionVirtualColumn(
                                                        "v0",
                                                        "0",
                                                        ColumnType.LONG
                                                    ))
                                                    .dimension(new DefaultDimensionSpec("v0", "d0", ColumnType.LONG))
                                                    .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(
                                                        null,
                                                        StringComparators.NUMERIC
                                                    )))
                                                    .aggregators(new CountAggregatorFactory("a0"))
                                                    .threshold(1)
                                                    .build()
                          ),
                          "__j0.",
                          "1",
                          JoinType.LEFT
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .aggregators(
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0"),
                          and(
                              notNull("_j0.a0"),
                              notNull("dim1")
                          ),
                          "a0"
                      ),
                      new FilteredAggregatorFactory(
                          new FloatMinAggregatorFactory("a1", "m1"),
                          or(
                              isNull("__j0.a0"),
                              not(
                                  NullHandling.sqlCompatible()
                                  ? istrue(
                                      or(
                                          not(expressionFilter("\"__j0.d0\"")),
                                          notNull("__j0.d0")
                                      )
                                  )
                                  : or(
                                      not(expressionFilter("\"__j0.d0\"")),
                                      notNull("__j0.d0")
                                  )
                              )
                          ),
                          "a1"
                      )
                  )
                  .context(queryContext)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{useDefault ? 1L : 2L, 1.0f}
        )
    );
  }

  @SqlTestFrameworkConfig(minTopNThreshold = 1)
  @Test
  public void testJoinWithAliasAndOrderByNoGroupBy()
  {
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(PlannerConfig.CTX_KEY_USE_APPROXIMATE_TOPN, false);
    testQuery(
        "select t1.__time from druid.foo as t1 join\n"
        + "  druid.numfoo as t2 on t1.dim2 = t2.dim2\n"
        + " order by t1.__time ASC ",
        context, // turn on exact topN
        ImmutableList.of(
            newScanQueryBuilder()
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dataSource(
                    JoinDataSource.create(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE3)
                                .intervals(querySegmentSpec(Intervals.of(
                                    "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z")))
                                .columns("dim2")
                                .context(context)
                                .build()
                        ),
                        "j0.",
                        "(\"dim2\" == \"j0.dim2\")",
                        JoinType.INNER,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper()
                    )
                )
                .columns("__time")
                .order(ScanQuery.Order.ASCENDING)
                .context(context)
                .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{946684800000L},
            new Object[]{946684800000L},
            new Object[]{946857600000L},
            new Object[]{978307200000L},
            new Object[]{978307200000L},
            new Object[]{978393600000L}
        )
        : ImmutableList.of(
            new Object[]{946684800000L},
            new Object[]{946684800000L},
            new Object[]{978307200000L},
            new Object[]{978307200000L},
            new Object[]{978393600000L}
        )
    );
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private List<Object[]> sortIfSortBased(final List<Object[]> results, final int... keyColumns)
  {
    if (sortBasedJoin) {
      final List<Object[]> retVal = new ArrayList<>(results);
      retVal.sort(
          (a, b) -> {
            for (int keyColumn : keyColumns) {
              final int cmp = Comparators.<Comparable>naturalNullsFirst()
                                         .compare((Comparable) a[keyColumn], (Comparable) b[keyColumn]);
              if (cmp != 0) {
                return cmp;
              }
            }

            return 0;
          });
      return retVal;
    } else {
      return results;
    }
  }

  @Test
  public void testJoinsWithTwoConditions()
  {
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "SELECT t1.__time, t1.m1\n"
        + "FROM foo t1\n"
        + "JOIN (SELECT m1, MAX(__time) as latest_time FROM foo WHERE m1 IN (1,2) GROUP BY m1) t2\n"
        + "ON t1.m1 = t2.m1 AND t1.__time = t2.latest_time\n",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                        .setDimFilter(
                                            NullHandling.replaceWithDefault()
                                            ? in("m1", ImmutableList.of("1", "2"), null)
                                            : or(
                                                equality("m1", 1.0, ColumnType.FLOAT),
                                                equality("m1", 2.0, ColumnType.FLOAT)
                                            )
                                        )
                                        .setDimensions(new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT))
                                        .setAggregatorSpecs(aggregators(new LongMaxAggregatorFactory("a0", "__time")))
                                        .setContext(context)
                                        .build()
                        ),
                        "j0.",
                        "((\"m1\" == \"j0.d0\") && (\"__time\" == \"j0.a0\"))",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "m1")
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f},
            new Object[]{946771200000L, 2.0f}
        )
    );
  }

  @Test
  public void testJoinsWithThreeConditions()
  {
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "SELECT t1.__time, t1.m1, t1.m2\n"
        + "FROM foo t1\n"
        + "JOIN (SELECT m1, m2, MAX(__time) as latest_time FROM foo WHERE m1 IN (1,2) AND m2 IN (1,2) GROUP by m1,m2) t2\n"
        + "ON t1.m1 = t2.m1 AND t1.m2 = t2.m2 AND t1.__time = t2.latest_time\n",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                        .setDimFilter(
                                            NullHandling.replaceWithDefault()
                                            ? and(
                                                in("m1", ImmutableList.of("1", "2"), null),
                                                in("m2", ImmutableList.of("1", "2"), null)
                                            )
                                            : and(
                                                or(
                                                    equality("m1", 1.0, ColumnType.FLOAT),
                                                    equality("m1", 2.0, ColumnType.FLOAT)
                                                ),
                                                or(
                                                    equality("m2", 1.0, ColumnType.DOUBLE),
                                                    equality("m2", 2.0, ColumnType.DOUBLE)
                                                )
                                            )
                                        )
                                        .setDimensions(
                                            new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT),
                                            new DefaultDimensionSpec("m2", "d1", ColumnType.DOUBLE)
                                        )
                                        .setAggregatorSpecs(aggregators(new LongMaxAggregatorFactory("a0", "__time")))
                                        .setContext(context)
                                        .build()
                        ),
                        "j0.",
                        "((\"m1\" == \"j0.d0\") && (\"m2\" == \"j0.d1\") && (\"__time\" == \"j0.a0\"))",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "m1", "m2")
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, 1.0f, 1.0},
            new Object[]{946771200000L, 2.0f, 2.0}
        )
    );
  }

  @Test
  public void testJoinWithInputRefCondition()
  {
    cannotVectorize();
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);

    Query<?> expectedQuery;

    if (!NullHandling.sqlCompatible()) {
      expectedQuery = Druids.newTimeseriesQueryBuilder()
                            .dataSource(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        GroupByQuery.builder()
                                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                                    .setGranularity(Granularities.ALL)
                                                    .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                                    .setDimensions(
                                                        new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)
                                                    )
                                                    .setPostAggregatorSpecs(
                                                        expressionPostAgg("a0", "1", ColumnType.LONG)
                                                    )
                                                    .build()
                                    ),
                                    "j0.",
                                    "(CAST(floor(100), 'DOUBLE') == \"j0.d0\")",
                                    JoinType.LEFT
                                )
                            )
                            .granularity(Granularities.ALL)
                            .aggregators(aggregators(
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    isNull("j0.a0")
                                )
                            ))
                            .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .context(context)
                            .build();

    } else {
      expectedQuery = Druids.newTimeseriesQueryBuilder()
                            .dataSource(
                                join(
                                    join(
                                        new TableDataSource("foo"),
                                        new QueryDataSource(
                                            Druids.newTimeseriesQueryBuilder()
                                                  .dataSource("foo")
                                                  .aggregators(
                                                      new CountAggregatorFactory("a0"),
                                                      new FilteredAggregatorFactory(
                                                          new CountAggregatorFactory("a1"),
                                                          notNull("m1"),
                                                          "a1"
                                                      )
                                                  )
                                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                                  .context(context)
                                                  .build()
                                        ),
                                        "j0.",
                                        "1",
                                        JoinType.INNER
                                    ),
                                    new QueryDataSource(
                                        GroupByQuery.builder()
                                                    .setInterval(querySegmentSpec(Filtration.eternity()))
                                                    .setGranularity(Granularities.ALL)
                                                    .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                                    .setDimensions(
                                                        new DefaultDimensionSpec("m1", "d0", ColumnType.FLOAT)
                                                    )
                                                    .setPostAggregatorSpecs(
                                                        expressionPostAgg("a0", "1", ColumnType.LONG)
                                                    )
                                                    .build()
                                    ),
                                    "_j0.",
                                    "(CAST(floor(100), 'DOUBLE') == \"_j0.d0\")",
                                    JoinType.LEFT
                                )
                            )
                            .granularity(Granularities.ALL)
                            .aggregators(aggregators(
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    or(
                                        equality("j0.a0", 0L, ColumnType.LONG),
                                        and(
                                            isNull("_j0.a0"),
                                            expressionFilter("(\"j0.a1\" >= \"j0.a0\")")
                                        )

                                    )
                                )
                            ))
                            .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .context(context)
                            .build();

    }

    testQuery(
        "SELECT COUNT(*) FILTER (WHERE FLOOR(100) NOT IN (SELECT m1 FROM foo)) "
        + "FROM foo",
        context,
        ImmutableList.of(expectedQuery),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testJoinsWithUnnestOnLeft()
  {
    // Segment map function of MSQ needs some work
    // To handle these nested cases
    // Remove this when that's handled
    msqIncompatible();
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "with t1 as (\n"
        + "select * from foo, unnest(MV_TO_ARRAY(\"dim3\")) as u(d3)\n"
        + ")\n"
        + "select t1.dim3, t1.d3, t2.dim2 from t1 JOIN numfoo as t2\n"
        + "ON t1.d3 = t2.\"dim2\"",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .dataSource(CalciteTests.DATASOURCE3)
                                .columns("dim2")
                                .legacy(false)
                                .context(context)
                                .build()
                        ),
                        "_j0.",
                        "(\"j0.unnest\" == \"_j0.dim2\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.dim2", "dim3", "j0.unnest")
                .context(context)
                .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"}
        ) : ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"", "", ""}
        )
    );
  }

  @Test
  public void testJoinsWithUnnestOverFilteredDSOnLeft()
  {
    // Segment map function of MSQ needs some work
    // To handle these nested cases
    // Remove this when that's handled
    msqIncompatible();
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "with t1 as (\n"
        + "select * from foo, unnest(MV_TO_ARRAY(\"dim3\")) as u(d3) where dim2='a'\n"
        + ")\n"
        + "select t1.dim3, t1.d3, t2.dim2 from t1 JOIN numfoo as t2\n"
        + "ON t1.d3 = t2.\"dim2\"",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        UnnestDataSource.create(
                            FilteredDataSource.create(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                equality("dim2", "a", ColumnType.STRING)
                            ),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .dataSource(CalciteTests.DATASOURCE3)
                                .columns("dim2")
                                .legacy(false)
                                .context(context)
                                .build()
                        ),
                        "_j0.",
                        "(\"j0.unnest\" == \"_j0.dim2\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.dim2", "dim3", "j0.unnest")
                .context(context)
                .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"}
        ) : ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"", "", ""}
        )
    );
  }

  @Test
  public void testJoinsWithUnnestOverJoin()
  {
    // Segment map function of MSQ needs some work
    // To handle these nested cases
    // Remove this when that's handled
    msqIncompatible();
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "with t1 as (\n"
        + "select * from (SELECT * from foo JOIN (select dim2 as t from foo where dim2 IN ('a','b','ab','abc')) ON dim2=t), "
        + " unnest(MV_TO_ARRAY(\"dim3\")) as u(d3) \n"
        + ")\n"
        + "select t1.dim3, t1.d3, t2.dim2 from t1 JOIN numfoo as t2\n"
        + "ON t1.d3 = t2.\"dim2\"",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        UnnestDataSource.create(
                            join(
                                new TableDataSource(CalciteTests.DATASOURCE1),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .intervals(querySegmentSpec(Filtration.eternity()))
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .filters(new InDimFilter("dim2", ImmutableList.of("a", "b", "ab", "abc"), null))
                                        .legacy(false)
                                        .context(context)
                                        .columns("dim2")
                                        .build()
                                ),
                                "j0.",
                                "(\"dim2\" == \"j0.dim2\")",
                                JoinType.INNER
                            ),
                            expressionVirtualColumn("_j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .dataSource(CalciteTests.DATASOURCE3)
                                .columns("dim2")
                                .legacy(false)
                                .context(context)
                                .build()
                        ),
                        "__j0.",
                        "(\"_j0.unnest\" == \"__j0.dim2\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__j0.dim2", "_j0.unnest", "dim3")
                .context(context)
                .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"}
        ) : ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"", "", ""},
            new Object[]{"", "", ""},
            new Object[]{"", "", ""},
            new Object[]{"", "", ""}
        )
    );
  }

  @Test
  public void testSelfJoinsWithUnnestOnLeftAndRight()
  {
    // Segment map function of MSQ needs some work
    // To handle these nested cases
    // Remove this when that's handled
    msqIncompatible();
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "with t1 as (\n"
        + "select * from foo, unnest(MV_TO_ARRAY(\"dim3\")) as u(d3)\n"
        + ")\n"
        + "select t1.dim3, t1.d3, t2.dim2 from t1 JOIN t1 as t2\n"
        + "ON t1.d3 = t2.d3",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE1),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .dataSource(UnnestDataSource.create(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                                    null
                                ))
                                .columns("dim2", "j0.unnest")
                                .legacy(false)
                                .context(context)
                                .build()
                        ),
                        "_j0.",
                        "(\"j0.unnest\" == \"_j0.j0.unnest\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("_j0.dim2", "dim3", "j0.unnest")
                .context(context)
                .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "b", "a"},
            new Object[]{"[\"a\",\"b\"]", "b", ""},
            new Object[]{"[\"b\",\"c\"]", "b", "a"},
            new Object[]{"[\"b\",\"c\"]", "b", ""},
            new Object[]{"[\"b\",\"c\"]", "c", ""},
            new Object[]{"d", "d", ""}
        ) : ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a"},
            new Object[]{"[\"a\",\"b\"]", "b", "a"},
            new Object[]{"[\"a\",\"b\"]", "b", null},
            new Object[]{"[\"b\",\"c\"]", "b", "a"},
            new Object[]{"[\"b\",\"c\"]", "b", null},
            new Object[]{"[\"b\",\"c\"]", "c", null},
            new Object[]{"d", "d", ""},
            new Object[]{"", "", "a"}
        )
    );
  }

  @Test
  public void testJoinsOverUnnestOverFilterDSOverJoin()
  {
    // Segment map function of MSQ needs some work
    // To handle these nested cases
    // Remove this when that's handled
    msqIncompatible();
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "with t1 as (\n"
        + "select * from (SELECT * from foo JOIN (select dim2 as t from foo where dim2 IN ('a','b','ab','abc')) ON dim2=t),\n"
        + "unnest(MV_TO_ARRAY(\"dim3\")) as u(d3) where m1 IN (1,4) and d3='a'\n"
        + ")\n"
        + "select t1.dim3, t1.d3, t2.dim2, t1.m1 from t1 JOIN numfoo as t2\n"
        + "ON t1.d3 = t2.\"dim2\"",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        UnnestDataSource.create(
                            FilteredDataSource.create(
                                join(
                                    new TableDataSource(CalciteTests.DATASOURCE1),
                                    new QueryDataSource(
                                        newScanQueryBuilder()
                                            .intervals(querySegmentSpec(Filtration.eternity()))
                                            .dataSource(CalciteTests.DATASOURCE1)
                                            .columns("dim2")
                                            .filters(new InDimFilter(
                                                "dim2",
                                                ImmutableList.of("a", "ab", "abc", "b"),
                                                null
                                            ))
                                            .legacy(false)
                                            .context(context)
                                            .build()
                                    ),
                                    "j0.",
                                    "(\"dim2\" == \"j0.dim2\")",
                                    JoinType.INNER
                                ),
                                useDefault ?
                                new InDimFilter("m1", ImmutableList.of("1", "4"), null) :
                                or(
                                    equality("m1", 1.0, ColumnType.FLOAT),
                                    equality("m1", 4.0, ColumnType.FLOAT)
                                )
                            ),
                            expressionVirtualColumn("_j0.unnest", "\"dim3\"", ColumnType.STRING),
                            equality("_j0.unnest", "a", ColumnType.STRING)
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .dataSource(CalciteTests.DATASOURCE3)
                                .columns("dim2")
                                .legacy(false)
                                .context(context)
                                .build()
                        ),
                        "__j0.",
                        "(\"_j0.unnest\" == \"__j0.dim2\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__j0.dim2", "_j0.unnest", "dim3", "m1")
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f},
            new Object[]{"[\"a\",\"b\"]", "a", "a", 1.0f}
        )
    );
  }

  @Test
  public void testLeftJoinsOnTwoWithTables()
  {
    Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    testQuery(
        "with raw1 as (\n"
        + "  select\n"
        + "  dim1,\n"
        + "  count(*)/2 as c\n"
        + "  from foo\n"
        + "  GROUP BY 1\n"
        + "),\n"
        + " raw2 as (\n"
        + "  select \n"
        + "  dim1,\n"
        + "  count(*)/2 as c\n"
        + "  from foo\n"
        + "  GROUP BY 1\n"
        + ")\n"
        + "select\n"
        + "  r1.c-r2.c\n"
        + "from raw1 r1\n"
        + "left join raw2 r2\n"
        + "on r1.dim1 = r2.dim1",
        context,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    join(
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                        .setPostAggregatorSpecs(expressionPostAgg(
                                            "p0",
                                            "(\"a0\" / 2)",
                                            ColumnType.LONG
                                        ))
                                        .setContext(context)
                                        .build()
                        ),
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                        .setPostAggregatorSpecs(expressionPostAgg(
                                            "p0",
                                            "(\"a0\" / 2)",
                                            ColumnType.LONG
                                        ))
                                        .setContext(context)
                                        .build()
                        ),
                        "j0.",
                        "(\"d0\" == \"j0.d0\")",
                        JoinType.LEFT
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("v0")
                .virtualColumns(expressionVirtualColumn("v0", "(\"p0\" - \"j0.p0\")", ColumnType.LONG))
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L},
            new Object[]{0L}
        )
    );
  }
}
