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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.annotations.UsedByJUnitParamsRunner;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.GlobalTableDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.FloatAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.lookup.RegisteredLookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.NumericTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.CannotBuildQueryException;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class CalciteQueryTest extends BaseCalciteQueryTest
{
  private final boolean useDefault = NullHandling.replaceWithDefault();

  @Test
  public void testSelectConstantExpression() throws Exception
  {
    // Test with a Druid-specific function, to make sure they are hooked up correctly even when not selecting
    // from a table.
    testQuery(
        "SELECT REGEXP_EXTRACT('foo', '^(.)')",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"f"}
        )
    );
  }

  @Test
  public void testExpressionContainingNull() throws Exception
  {
    List<String> expectedResult = new ArrayList<>();
    expectedResult.add("Hello");
    expectedResult.add(null);
    testQuery(
        "SELECT ARRAY ['Hello', NULL]",
        ImmutableList.of(),
        ImmutableList.of(new Object[]{expectedResult})
    );
  }

  @Test
  public void testSelectNonNumericNumberLiterals() throws Exception
  {
    // Tests to convert NaN, positive infinity and negative infinity as literals.
    testQuery(
        "SELECT"
        + " CAST(1 / 0.0 AS BIGINT),"
        + " CAST(1 / -0.0 AS BIGINT),"
        + " CAST(-1 / 0.0 AS BIGINT),"
        + " CAST(-1 / -0.0 AS BIGINT),"
        + " CAST(0/ 0.0 AS BIGINT)",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[] {
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                0L
            }
        )
    );
  }

  @Test
  public void testSelectConstantExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "2", ValueType.LONG))
                .columns("dim1", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{2, ""}
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
                                .columns("__time", "cnt", "dim1", "dim2", "dim3", "j0.m1", "m1", "m2", "unique_dim1")
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ValueType.STRING))
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
  public void testJoinOuterGroupByAndSubqueryNoLimit() throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

    testQuery(
        "SELECT dim2, AVG(m2) FROM (SELECT * FROM foo AS t1 INNER JOIN foo AS t2 ON t1.m1 = t2.m1) AS t3 GROUP BY dim2",
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
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ValueType.STRING))
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
                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ValueType.STRING))
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
                                          .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                                          .build()),
                                "j0.",
                                "((timestamp_floor(\"__time\",'PT1H',null,'UTC') == \"j0.d0\") && (\"m1\" == \"j0.a0\"))",
                                JoinType.INNER
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(
                            new DefaultDimensionSpec("__time", "d0", ValueType.LONG),
                            new DefaultDimensionSpec("m1", "d1", ValueType.FLOAT)

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
                                                .setInterval(querySegmentSpec(Intervals.of("1994-04-29/2020-01-11T00:00:00.001Z")))
                                                .setVirtualColumns(
                                                    expressionVirtualColumn(
                                                        "v0",
                                                        "(timestamp_floor(\"__time\",'PT1H',null,'UTC') + 0)",
                                                        ValueType.LONG
                                                    )
                                                )
                                                .setDimFilter(selector("dim3", "b", null))
                                                .setGranularity(Granularities.ALL)
                                                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
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
                            new DefaultDimensionSpec("__time", "d0", ValueType.LONG),
                            new DefaultDimensionSpec("m1", "d1", ValueType.FLOAT)

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
  public void testSelectCountStart() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        "SELECT exp(count(*)) + 10, sum(m2)  FROM druid.foo WHERE  dim2 = 0",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0"),
                                   new DoubleSumAggregatorFactory("a1", "m2")
                               ))
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)")
                               )
                               .context(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        "SELECT exp(count(*)) + 10, sum(m2)  FROM druid.foo WHERE  __time >= TIMESTAMP '2999-01-01 00:00:00'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of(
                                   "2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z"))
                               )
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0"),
                                   new DoubleSumAggregatorFactory("a1", "m2")
                               ))
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)")
                               )
                               .context(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

    testQuery(
        "SELECT COUNT(*) FROM foo WHERE dim1 = 'nonexistent' GROUP BY FLOOR(__time TO DAY)",
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(selector("dim1", "nonexistent", null))
                               .granularity(Granularities.DAY)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0")
                               ))
                               .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                               .build()),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectTrimFamily() throws Exception
  {
    // TRIM has some whacky parsing. Make sure the different forms work.

    testQuery(
        "SELECT\n"
        + "TRIM(BOTH 'x' FROM 'xfoox'),\n"
        + "TRIM(TRAILING 'x' FROM 'xfoox'),\n"
        + "TRIM(' ' FROM ' foo '),\n"
        + "TRIM(TRAILING FROM ' foo '),\n"
        + "TRIM(' foo '),\n"
        + "BTRIM(' foo '),\n"
        + "BTRIM('xfoox', 'x'),\n"
        + "LTRIM(' foo '),\n"
        + "LTRIM('xfoox', 'x'),\n"
        + "RTRIM(' foo '),\n"
        + "RTRIM('xfoox', 'x'),\n"
        + "COUNT(*)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .postAggregators(
                      expressionPostAgg("p0", "'foo'"),
                      expressionPostAgg("p1", "'xfoo'"),
                      expressionPostAgg("p2", "'foo'"),
                      expressionPostAgg("p3", "' foo'"),
                      expressionPostAgg("p4", "'foo'"),
                      expressionPostAgg("p5", "'foo'"),
                      expressionPostAgg("p6", "'foo'"),
                      expressionPostAgg("p7", "'foo '"),
                      expressionPostAgg("p8", "'foox'"),
                      expressionPostAgg("p9", "' foo'"),
                      expressionPostAgg("p10", "'xfoo'")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"foo", "xfoo", "foo", " foo", "foo", "foo", "foo", "foo ", "foox", " foo", "xfoo", 6L}
        )
    );
  }

  @Test
  public void testSelectPadFamily() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "LPAD('foo', 5, 'x'),\n"
        + "LPAD('foo', 2, 'x'),\n"
        + "LPAD('foo', 5),\n"
        + "RPAD('foo', 5, 'x'),\n"
        + "RPAD('foo', 2, 'x'),\n"
        + "RPAD('foo', 5),\n"
        + "COUNT(*)\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .postAggregators(
                      expressionPostAgg("p0", "'xxfoo'"),
                      expressionPostAgg("p1", "'fo'"),
                      expressionPostAgg("p2", "'  foo'"),
                      expressionPostAgg("p3", "'fooxx'"),
                      expressionPostAgg("p4", "'fo'"),
                      expressionPostAgg("p5", "'foo  '")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"xxfoo", "fo", "  foo", "fooxx", "fo", "foo  ", 6L}
        )
    );
  }


  @Test
  public void testExplainSelectConstantExpression() throws Exception
  {
    testQuery(
        "EXPLAIN PLAN FOR SELECT 1 + 1",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"BindableValues(tuples=[[{ 2 }]])\n"}
        )
    );
  }

  @Test
  public void testInformationSchemaSchemata() throws Exception
  {
    testQuery(
        "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"lookup"},
            new Object[]{"druid"},
            new Object[]{"sys"},
            new Object[]{"INFORMATION_SCHEMA"}
        )
    );
  }

  @Test
  public void testInformationSchemaTables() throws Exception
  {
    testQuery(
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_JOINABLE, IS_BROADCAST\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        ImmutableList.of(),
        ImmutableList.<Object[]>builder()
            .add(new Object[]{"druid", CalciteTests.BROADCAST_DATASOURCE, "TABLE", "YES", "YES"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE1, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE2, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE4, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE5, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE3, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.SOME_DATASOURCE, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.SOMEXDATASOURCE, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", "aview", "VIEW", "NO", "NO"})
            .add(new Object[]{"druid", "bview", "VIEW", "NO", "NO"})
            .add(new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"lookup", "lookyloo", "TABLE", "YES", "YES"})
            .add(new Object[]{"sys", "segments", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "server_segments", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "servers", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "supervisors", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "tasks", "SYSTEM_TABLE", "NO", "NO"})
            .build()
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, IS_JOINABLE, IS_BROADCAST\n"
        + "FROM INFORMATION_SCHEMA.TABLES\n"
        + "WHERE TABLE_TYPE IN ('SYSTEM_TABLE', 'TABLE', 'VIEW')",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.<Object[]>builder()
            .add(new Object[]{"druid", CalciteTests.BROADCAST_DATASOURCE, "TABLE", "YES", "YES"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE1, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE2, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE4, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.FORBIDDEN_DATASOURCE, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE5, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.DATASOURCE3, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.SOME_DATASOURCE, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", CalciteTests.SOMEXDATASOURCE, "TABLE", "NO", "NO"})
            .add(new Object[]{"druid", "aview", "VIEW", "NO", "NO"})
            .add(new Object[]{"druid", "bview", "VIEW", "NO", "NO"})
            .add(new Object[]{"INFORMATION_SCHEMA", "COLUMNS", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"INFORMATION_SCHEMA", "SCHEMATA", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"INFORMATION_SCHEMA", "TABLES", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"lookup", "lookyloo", "TABLE", "YES", "YES"})
            .add(new Object[]{"sys", "segments", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "server_segments", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "servers", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "supervisors", "SYSTEM_TABLE", "NO", "NO"})
            .add(new Object[]{"sys", "tasks", "SYSTEM_TABLE", "NO", "NO"})
            .build()
    );
  }

  @Test
  public void testInformationSchemaColumnsOnTable() throws Exception
  {
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", useDefault ? "NO" : "YES"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"dim3", "VARCHAR", "YES"},
            new Object[]{"m1", "FLOAT", useDefault ? "NO" : "YES"},
            new Object[]{"m2", "DOUBLE", useDefault ? "NO" : "YES"},
            new Object[]{"unique_dim1", "OTHER", "YES"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumnsOnForbiddenTable() throws Exception
  {
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'forbiddenDatasource'",
        ImmutableList.of(),
        ImmutableList.of()
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'forbiddenDatasource'",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"__time", "TIMESTAMP", "NO"},
            new Object[]{"cnt", "BIGINT", useDefault ? "NO" : "YES"},
            new Object[]{"dim1", "VARCHAR", "YES"},
            new Object[]{"dim2", "VARCHAR", "YES"},
            new Object[]{"m1", "FLOAT", useDefault ? "NO" : "YES"},
            new Object[]{"m2", "DOUBLE", useDefault ? "NO" : "YES"},
            new Object[]{"unique_dim1", "OTHER", "YES"}
        )
    );
  }

  @Test
  public void testInformationSchemaColumnsOnView() throws Exception
  {
    testQuery(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'aview'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{"dim1_firstchar", "VARCHAR", "YES"}
        )
    );
  }

  @Test
  public void testExplainInformationSchemaColumns() throws Exception
  {
    final String explanation =
        "BindableProject(COLUMN_NAME=[$3], DATA_TYPE=[$7])\n"
        + "  BindableFilter(condition=[AND(=($1, 'druid'), =($2, 'foo'))])\n"
        + "    BindableTableScan(table=[[INFORMATION_SCHEMA, COLUMNS]])\n";

    testQuery(
        "EXPLAIN PLAN FOR\n"
        + "SELECT COLUMN_NAME, DATA_TYPE\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation}
        )
    );
  }

  @Test
  public void testAggregatorsOnInformationSchemaColumns() throws Exception
  {
    // Not including COUNT DISTINCT, since it isn't supported by BindableAggregate, and so it can't work.
    testQuery(
        "SELECT\n"
        + "  COUNT(JDBC_TYPE),\n"
        + "  SUM(JDBC_TYPE),\n"
        + "  AVG(JDBC_TYPE),\n"
        + "  MIN(JDBC_TYPE),\n"
        + "  MAX(JDBC_TYPE)\n"
        + "FROM INFORMATION_SCHEMA.COLUMNS\n"
        + "WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = 'foo'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{8L, 1249L, 156L, -5L, 1111L}
        )
    );
  }

  @Test
  public void testSelectStar() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), 1L, "2", "", "d", 3f, 3.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), 1L, "1", "a", "", 4f, 4.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5f, 5.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-03"), 1L, "abc", NULL_STRING, NULL_STRING, 6f, 6.0, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectStarOnForbiddenTable() throws Exception
  {
    assertQueryIsForbidden(
        "SELECT * FROM druid.forbiddenDatasource",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM druid.forbiddenDatasource",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-01"),
                1L,
                "forbidden",
                "abcd",
                9999.0f,
                NullHandling.defaultDoubleValue(),
                "\"AQAAAQAAAALFBA==\""
            }
        )
    );
  }

  @Test
  public void testUnqualifiedTableName() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testExplainSelectStar() throws Exception
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    testQuery(
        "EXPLAIN PLAN FOR SELECT * FROM druid.foo",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                "DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[],\"resultFormat\":\"compactedList\",\"batchSize\":20480,\"limit\":9223372036854775807,\"order\":\"none\",\"filter\":null,\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\"},\"descending\":false,\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, cnt:LONG, dim1:STRING, dim2:STRING, dim3:STRING, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX}])\n"
            }
        )
    );
  }

  @Test
  public void testSelectStarWithLimit() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo LIMIT 2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1.0f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2.0f, 2.0, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectWithProjection() throws Exception
  {
    testQuery(
        "SELECT SUBSTRING(dim2, 1, 1) FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim2\", 0, 1)", ValueType.STRING)
                )
                .columns("v0")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{NULL_STRING}
        )
    );
  }

  @Test
  public void testSelectWithExpressionFilter() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo WHERE m1 + 1 = 7",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "(\"m1\" + 1)", ValueType.FLOAT)
                )
                .filters(selector("v0", "7", null))
                .columns("dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectStarWithLimitTimeDescending() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo ORDER BY __time DESC LIMIT 2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-03"), 1L, "abc", NULL_STRING, NULL_STRING, 6f, 6d, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5f, 5d, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectStarWithoutLimitTimeAscending() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo ORDER BY __time",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1"))
                .limit(Long.MAX_VALUE)
                .order(ScanQuery.Order.ASCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), 1L, "10.1", NULL_STRING, "[\"b\",\"c\"]", 2f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), 1L, "2", "", "d", 3f, 3.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), 1L, "1", "a", "", 4f, 4.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5f, 5.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-03"), 1L, "abc", NULL_STRING, NULL_STRING, 6f, 6.0, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectSingleColumnTwice() throws Exception
  {
    testQuery(
        "SELECT dim2 x, dim2 y FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a"},
            new Object[]{NULL_STRING, NULL_STRING}
        )
    );
  }

  @Test
  public void testSelectSingleColumnWithLimitDescending() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectStarFromSelectSingleColumnWithLimitDescending() throws Exception
  {
    // After upgrading to Calcite 1.21, Calcite no longer respects the ORDER BY __time DESC
    // in the inner query. This is valid, as the SQL standard considers the subquery results to be an unordered
    // set of rows.
    testQuery(
        "SELECT * FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC) LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .limit(2)
                .order(ScanQuery.Order.NONE)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testSelectLimitWrapping() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectLimitWrappingAgainAkaIDontReallyQuiteUnderstandCalciteQueryPlanning() throws Exception
  {
    // this test is for a specific bug encountered where the 2nd query would not plan with auto limit wrapping, but if
    // *any* column was removed from the select output, e.g. the first query in this test, then it does plan and
    // function correctly. Running the query supplying an explicit limit worked, and turning off auto limit worked.
    // The only difference between an explicit limit and auto limit was that the LogicalSort of the auto limit had an
    // offset of 0 instead of null, so the resolution was to modify the planner to only set offset on the sort if the
    // offset was non-zero. However, why the first query succeeded before this planner change and the latter did not is
    // still a mystery...
    testQuery(
        "SELECT \"__time\", \"count\", \"dimHyperUnique\", \"dimMultivalEnumerated\", \"dimMultivalEnumerated2\","
        + " \"dimMultivalSequentialWithNulls\", \"dimSequential\", \"dimSequentialHalfNull\", \"dimUniform\","
        + " \"dimZipf\", \"metFloatNormal\", \"metFloatZipf\", \"metLongSequential\""
        + " FROM druid.lotsocolumns"
        + " WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '10' YEAR",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE5)
                .intervals(querySegmentSpec(Intervals.of("1990-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                .columns(
                    ImmutableList.<String>builder()
                        .add("__time")
                        .add("count")
                        .add("dimHyperUnique")
                        .add("dimMultivalEnumerated")
                        .add("dimMultivalEnumerated2")
                        .add("dimMultivalSequentialWithNulls")
                        .add("dimSequential")
                        .add("dimSequentialHalfNull")
                        .add("dimUniform")
                        .add("dimZipf")
                        .add("metFloatNormal")
                        .add("metFloatZipf")
                        .add("metLongSequential")
                        .build()
                )
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                1576306800000L,
                1L,
                "0",
                "[\"Baz\",\"Baz\",\"Hello\",\"World\"]",
                useDefault ? "[\"\",\"Apple\",\"Orange\"]" : "[null,\"Apple\",\"Orange\"]",
                "[\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\"]",
                "0",
                "0",
                "74416",
                "27",
                "5000.0",
                "147.0",
                "0"
            },
            new Object[]{
                1576306800000L,
                1L,
                "8",
                "[\"Baz\",\"World\",\"ㅑ ㅓ ㅕ ㅗ ㅛ ㅜ ㅠ ㅡ ㅣ\"]",
                useDefault ? "[\"\",\"Corundum\",\"Xylophone\"]" : "[null,\"Corundum\",\"Xylophone\"]",
                useDefault ? "" : null,
                "8",
                useDefault ? "" : null,
                "50515",
                "9",
                "4999.0",
                "25.0",
                "8"
            }
        )
    );

    testQuery(
        "SELECT \"__time\", \"count\", \"dimHyperUnique\", \"dimMultivalEnumerated\", \"dimMultivalEnumerated2\","
        + " \"dimMultivalSequentialWithNulls\", \"dimSequential\", \"dimSequentialHalfNull\", \"dimUniform\","
        + " \"dimZipf\", \"metFloatNormal\", \"metFloatZipf\", \"metLongSequential\", \"metLongUniform\""
        + " FROM druid.lotsocolumns"
        + " WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '10' YEAR",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE5)
                .intervals(querySegmentSpec(Intervals.of("1990-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                .columns(
                    ImmutableList.<String>builder()
                        .add("__time")
                        .add("count")
                        .add("dimHyperUnique")
                        .add("dimMultivalEnumerated")
                        .add("dimMultivalEnumerated2")
                        .add("dimMultivalSequentialWithNulls")
                        .add("dimSequential")
                        .add("dimSequentialHalfNull")
                        .add("dimUniform")
                        .add("dimZipf")
                        .add("metFloatNormal")
                        .add("metFloatZipf")
                        .add("metLongSequential")
                        .add("metLongUniform")
                        .build()
                )
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                1576306800000L,
                1L,
                "0",
                "[\"Baz\",\"Baz\",\"Hello\",\"World\"]",
                useDefault ? "[\"\",\"Apple\",\"Orange\"]" : "[null,\"Apple\",\"Orange\"]",
                "[\"1\",\"2\",\"3\",\"4\",\"5\",\"6\",\"7\",\"8\"]",
                "0",
                "0",
                "74416",
                "27",
                "5000.0",
                "147.0",
                "0",
                "372"
            },
            new Object[]{
                1576306800000L,
                1L,
                "8",
                "[\"Baz\",\"World\",\"ㅑ ㅓ ㅕ ㅗ ㅛ ㅜ ㅠ ㅡ ㅣ\"]",
                useDefault ? "[\"\",\"Corundum\",\"Xylophone\"]" : "[null,\"Corundum\",\"Xylophone\"]",
                useDefault ? "" : null,
                "8",
                useDefault ? "" : null,
                "50515",
                "9",
                "4999.0",
                "25.0",
                "8",
                "252"
            }
        )
    );
  }

  @Test
  public void testTopNLimitWrapping() throws Exception
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"", 1L},
          new Object[]{"def", 1L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"def", 1L},
          new Object[]{"abc", 1L}
      );
    }
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("dim1", "d0", ValueType.STRING))
                .threshold(2)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)
                    )
                )
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        expected
    );
  }


  @Test
  public void testTopNLimitWrappingOrderByAgg() throws Exception
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 2 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("dim1", "d0", ValueType.STRING))
                .threshold(2)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric("a0")
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(new Object[]{"", 1L}, new Object[]{"1", 1L})
    );
  }

  @Test
  public void testGroupByLimitWrapping() throws Exception
  {
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"def", "abc", 1L},
          new Object[]{"abc", "", 1L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"def", "abc", 1L},
          new Object[]{"abc", null, 1L}
      );
    }
    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo GROUP BY dim1, dim2 ORDER BY dim1 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim1", "d0", ValueType.STRING),
                    new DefaultDimensionSpec("dim2", "d1", ValueType.STRING)
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec("d0", Direction.DESCENDING, StringComparators.LEXICOGRAPHIC)),
                        2
                    )
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testGroupByLimitWrappingOrderByAgg() throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, COUNT(*) FROM druid.foo GROUP BY 1, 2 ORDER BY 3 DESC",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setDimensions(
                    new DefaultDimensionSpec("dim1", "d0", ValueType.STRING),
                    new DefaultDimensionSpec("dim2", "d1", ValueType.STRING)
                )
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec("a0", Direction.DESCENDING, StringComparators.NUMERIC)
                        ),
                        2
                    )
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(new Object[]{"", "a", 1L}, new Object[]{"def", "abc", 1L})
    );
  }

  @Test
  public void testSelectProjectionFromSelectSingleColumnWithInnerLimitDescending() throws Exception
  {
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ValueType.STRING))
                .columns(ImmutableList.of("__time", "v0"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"beep abc"},
            new Object[]{"beep def"}
        )
    );
  }

  @Test
  public void testSelectProjectionFromSelectSingleColumnDescending() throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/7768.

    // After upgrading to Calcite 1.21, Calcite no longer respects the ORDER BY __time DESC
    // in the inner query. This is valid, as the SQL standard considers the subquery results to be an unordered
    // set of rows. This test now validates that the inner ordering is not applied.
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ValueType.STRING))
                .columns(ImmutableList.of("v0"))
                .order(ScanQuery.Order.NONE)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"beep "},
            new Object[]{"beep 10.1"},
            new Object[]{"beep 2"},
            new Object[]{"beep 1"},
            new Object[]{"beep def"},
            new Object[]{"beep abc"}
        )
    );
  }

  @Test
  public void testSelectProjectionFromSelectSingleColumnWithInnerAndOuterLimitDescending() throws Exception
  {
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ValueType.STRING))
                .columns(ImmutableList.of("__time", "v0"))
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"beep abc"},
            new Object[]{"beep def"}
        )
    );
  }

  @Test
  public void testGroupBySingleColumnDescendingNoTopN() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                .setGranularity(Granularities.ALL)
                .setLimitSpec(
                    new DefaultLimitSpec(
                        ImmutableList.of(
                            new OrderByColumnSpec(
                                "d0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.LEXICOGRAPHIC
                            )
                        ),
                        Integer.MAX_VALUE
                    )
                )
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"abc"},
            new Object[]{"2"},
            new Object[]{"10.1"},
            new Object[]{"1"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testEarliestAggregators() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT "
        + "EARLIEST(cnt), EARLIEST(m1), EARLIEST(dim1, 10), "
        + "EARLIEST(cnt + 1), EARLIEST(m1 + 1), EARLIEST(dim1 || CAST(cnt AS VARCHAR), 10) "
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ValueType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ValueType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ValueType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongFirstAggregatorFactory("a0", "cnt"),
                          new FloatFirstAggregatorFactory("a1", "m1"),
                          new StringFirstAggregatorFactory("a2", "dim1", 10),
                          new LongFirstAggregatorFactory("a3", "v0"),
                          new FloatFirstAggregatorFactory("a4", "v1"),
                          new StringFirstAggregatorFactory("a5", "v2", 10)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1.0f, "", 2L, 2.0f, "1"}
        )
    );
  }

  @Test
  public void testLatestAggregators() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT "
        + "LATEST(cnt), LATEST(m1), LATEST(dim1, 10), "
        + "LATEST(cnt + 1), LATEST(m1 + 1), LATEST(dim1 || CAST(cnt AS VARCHAR), 10) "
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ValueType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ValueType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ValueType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "cnt"),
                          new FloatLastAggregatorFactory("a1", "m1"),
                          new StringLastAggregatorFactory("a2", "dim1", 10),
                          new LongLastAggregatorFactory("a3", "v0"),
                          new FloatLastAggregatorFactory("a4", "v1"),
                          new StringLastAggregatorFactory("a5", "v2", 10)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6.0f, "abc", 2L, 7.0f, "abc1"}
        )
    );
  }

  // This test the on-heap version of the AnyAggregator (Double/Float/Long/String)
  @Test
  public void testAnyAggregator() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();

    testQuery(
        "SELECT "
        + "ANY_VALUE(cnt), ANY_VALUE(m1), ANY_VALUE(m2), ANY_VALUE(dim1, 10), "
        + "ANY_VALUE(cnt + 1), ANY_VALUE(m1 + 1), ANY_VALUE(dim1 || CAST(cnt AS VARCHAR), 10) "
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ValueType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ValueType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ValueType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongAnyAggregatorFactory("a0", "cnt"),
                          new FloatAnyAggregatorFactory("a1", "m1"),
                          new DoubleAnyAggregatorFactory("a2", "m2"),
                          new StringAnyAggregatorFactory("a3", "dim1", 10),
                          new LongAnyAggregatorFactory("a4", "v0"),
                          new FloatAnyAggregatorFactory("a5", "v1"),
                          new StringAnyAggregatorFactory("a6", "v2", 10)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.sqlCompatible() ? ImmutableList.of(new Object[]{1L, 1.0f, 1.0, "", 2L, 2.0f, "1"}) : ImmutableList.of(new Object[]{1L, 1.0f, 1.0, "", 2L, 2.0f, "1"})
    );
  }

  // This test the on-heap version of the AnyAggregator (Double/Float/Long) against numeric columns
  // that have null values (when run in SQL compatible null mode)
  @Test
  public void testAnyAggregatorsOnHeapNumericNulls() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();

    testQuery(
        "SELECT ANY_VALUE(l1), ANY_VALUE(d1), ANY_VALUE(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongAnyAggregatorFactory("a0", "l1"),
                          new DoubleAnyAggregatorFactory("a1", "d1"),
                          new FloatAnyAggregatorFactory("a2", "f1")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{7L, 1.0, 1.0f}
        )
    );
  }

  // This test the off-heap (buffer) version of the AnyAggregator (Double/Float/Long) against numeric columns
  // that have null values (when run in SQL compatible null mode)
  @Test
  public void testAnyAggregatorsOffHeapNumericNulls() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();
    testQuery(
        "SELECT ANY_VALUE(l1), ANY_VALUE(d1), ANY_VALUE(f1) FROM druid.numfoo GROUP BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                  .setDataSource(CalciteTests.DATASOURCE3)
                  .setInterval(querySegmentSpec(Filtration.eternity()))
                  .setGranularity(Granularities.ALL)
                  .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "_d0")))
                  .setAggregatorSpecs(
                      aggregators(
                          new LongAnyAggregatorFactory("a0", "l1"),
                          new DoubleAnyAggregatorFactory("a1", "d1"),
                          new FloatAnyAggregatorFactory("a2", "f1")
                      )
                  )
                  .setContext(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),

        NullHandling.sqlCompatible()
        ? ImmutableList.of(
            new Object[]{325323L, 1.7, 0.1f},
            new Object[]{0L, 0.0, 0.0f},
            new Object[]{7L, 1.0, 1.0f},
            new Object[]{null, null, null}
        )
        : ImmutableList.of(
            new Object[]{325323L, 1.7, 0.1f},
            new Object[]{7L, 1.0, 1.0f},
            new Object[]{0L, 0.0, 0.0f}
        )
    );
  }

  // This test the off-heap (buffer) version of the LatestAggregator (Double/Float/Long)
  @Test
  public void testPrimitiveLatestInSubquery() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT SUM(val1), SUM(val2), SUM(val3) FROM (SELECT dim2, LATEST(m1) AS val1, LATEST(cnt) AS val2, LATEST(m2) AS val3 FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(
                                            new FloatLastAggregatorFactory("a0:a", "m1"),
                                            new LongLastAggregatorFactory("a1:a", "cnt"),
                                            new DoubleLastAggregatorFactory("a2:a", "m2"))
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a"),
                                                new FinalizingFieldAccessPostAggregator("a1", "a1:a"),
                                                new FinalizingFieldAccessPostAggregator("a2", "a2:a")

                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new DoubleSumAggregatorFactory("_a0", "a0"),
                            new LongSumAggregatorFactory("_a1", "a1"),
                            new DoubleSumAggregatorFactory("_a2", "a2")
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ? ImmutableList.of(new Object[]{18.0, 4L, 18.0}) : ImmutableList.of(new Object[]{15.0, 3L, 15.0})
    );
  }

  // This test the off-heap (buffer) version of the EarliestAggregator (Double/Float/Long)
  @Test
  public void testPrimitiveEarliestInSubquery() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT SUM(val1), SUM(val2), SUM(val3) FROM (SELECT dim2, EARLIEST(m1) AS val1, EARLIEST(cnt) AS val2, EARLIEST(m2) AS val3 FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(
                                            new FloatFirstAggregatorFactory("a0:a", "m1"),
                                            new LongFirstAggregatorFactory("a1:a", "cnt"),
                                            new DoubleFirstAggregatorFactory("a2:a", "m2"))
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a"),
                                                new FinalizingFieldAccessPostAggregator("a1", "a1:a"),
                                                new FinalizingFieldAccessPostAggregator("a2", "a2:a")

                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new DoubleSumAggregatorFactory("_a0", "a0"),
                            new LongSumAggregatorFactory("_a1", "a1"),
                            new DoubleSumAggregatorFactory("_a2", "a2")
                                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ? ImmutableList.of(new Object[]{11.0, 4L, 11.0}) : ImmutableList.of(new Object[]{8.0, 3L, 8.0})
    );
  }

  // This test the off-heap (buffer) version of the LatestAggregator (String)
  @Test
  public void testStringLatestInSubquery() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, LATEST(dim1, 10) AS val FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new StringLastAggregatorFactory("a0:a", "dim1", 10)))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("_a0", null, "CAST(\"a0\", 'DOUBLE')", ExprMacroTable.nil())))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.sqlCompatible() ? 3 : 1.0}
        )
    );
  }

  // This test the off-heap (buffer) version of the EarliestAggregator (String)
  @Test
  public void testStringEarliestInSubquery() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, EARLIEST(dim1, 10) AS val FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new StringFirstAggregatorFactory("a0:a", "dim1", 10)))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("_a0", null, "CAST(\"a0\", 'DOUBLE')", ExprMacroTable.nil())))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            // default mode subquery results:
            //[, 10.1]
            //[a, ]
            //[abc, def]
            // SQL compatible mode subquery results:
            //[null, 10.1]
            //[, 2]
            //[a, ]
            //[abc, def]
            new Object[]{NullHandling.sqlCompatible() ? 12.1 : 10.1}
        )
    );
  }

  // This test the off-heap (buffer) version of the AnyAggregator (Double/Float/Long)
  @Test
  public void testPrimitiveAnyInSubquery() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();

    testQuery(
        "SELECT SUM(val1), SUM(val2), SUM(val3) FROM (SELECT dim2, ANY_VALUE(m1) AS val1, ANY_VALUE(cnt) AS val2, ANY_VALUE(m2) AS val3 FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(
                                            new FloatAnyAggregatorFactory("a0:a", "m1"),
                                            new LongAnyAggregatorFactory("a1:a", "cnt"),
                                            new DoubleAnyAggregatorFactory("a2:a", "m2"))
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a"),
                                                new FinalizingFieldAccessPostAggregator("a1", "a1:a"),
                                                new FinalizingFieldAccessPostAggregator("a2", "a2:a")

                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new DoubleSumAggregatorFactory("_a0", "a0"),
                            new LongSumAggregatorFactory("_a1", "a1"),
                            new DoubleSumAggregatorFactory("_a2", "a2")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.sqlCompatible() ? ImmutableList.of(new Object[]{11.0, 4L, 11.0}) : ImmutableList.of(new Object[]{8.0, 3L, 8.0})
    );
  }

  // This test the off-heap (buffer) version of the AnyAggregator (String)
  @Test
  public void testStringAnyInSubquery() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();

    testQuery(
        "SELECT SUM(val) FROM (SELECT dim2, ANY_VALUE(dim1, 10) AS val FROM foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                        .setAggregatorSpecs(aggregators(new StringAnyAggregatorFactory("a0:a", "dim1", 10)))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                            )
                                        )
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("_a0", null, "CAST(\"a0\", 'DOUBLE')", ExprMacroTable.nil())))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.sqlCompatible() ? 12.1 : 10.1}
        )
    );
  }

  @Test
  public void testEarliestAggregatorsNumericNulls() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT EARLIEST(l1), EARLIEST(d1), EARLIEST(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongFirstAggregatorFactory("a0", "l1"),
                          new DoubleFirstAggregatorFactory("a1", "d1"),
                          new FloatFirstAggregatorFactory("a2", "f1")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{7L, 1.0, 1.0f}
        )
    );
  }

  @Test
  public void testLatestAggregatorsNumericNull() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT LATEST(l1), LATEST(d1), LATEST(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "l1"),
                          new DoubleLastAggregatorFactory("a1", "d1"),
                          new FloatLastAggregatorFactory("a2", "f1")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultLongValue(), NullHandling.defaultDoubleValue(), NullHandling.defaultFloatValue()}
        )
    );
  }

  @Test
  public void testFirstLatestAggregatorsSkipNulls() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();

    final DimFilter filter;
    if (useDefault) {
      filter = not(selector("dim1", null, null));
    } else {
      filter = and(
          not(selector("dim1", null, null)),
          not(selector("l1", null, null)),
          not(selector("d1", null, null)),
          not(selector("f1", null, null))
      );
    }
    testQuery(
        "SELECT EARLIEST(dim1, 32), LATEST(l1), LATEST(d1), LATEST(f1) "
        + "FROM druid.numfoo "
        + "WHERE dim1 IS NOT NULL AND l1 IS NOT NULL AND d1 IS NOT NULL AND f1 is NOT NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(filter)
                  .aggregators(
                      aggregators(
                          new StringFirstAggregatorFactory("a0", "dim1", 32),
                          new LongLastAggregatorFactory("a1", "l1"),
                          new DoubleLastAggregatorFactory("a2", "d1"),
                          new FloatLastAggregatorFactory("a3", "f1")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // first row of dim1 is empty string, which is null in default mode, last non-null numeric rows are zeros
            new Object[]{useDefault ? "10.1" : "", 0L, 0.0, 0.0f}
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
  public void testAnyAggregatorsDoesNotSkipNulls() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();

    testQuery(
        "SELECT ANY_VALUE(dim1, 32), ANY_VALUE(l2), ANY_VALUE(d2), ANY_VALUE(f2) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new StringAnyAggregatorFactory("a0", "dim1", 32),
                          new LongAnyAggregatorFactory("a1", "l2"),
                          new DoubleAnyAggregatorFactory("a2", "d2"),
                          new FloatAnyAggregatorFactory("a3", "f2")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        // first row has null for l2, d2, f2 and dim1 as empty string (which is null in default mode)
        ImmutableList.of(
            useDefault ? new Object[]{"", 0L, 0.0, 0f} : new Object[]{"", null, null, null}
        )
    );
  }

  @Test
  public void testAnyAggregatorsSkipNullsWithFilter() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();

    final DimFilter filter;
    if (useDefault) {
      filter = not(selector("dim1", null, null));
    } else {
      filter = and(
          not(selector("dim1", null, null)),
          not(selector("l2", null, null)),
          not(selector("d2", null, null)),
          not(selector("f2", null, null))
      );
    }
    testQuery(
        "SELECT ANY_VALUE(dim1, 32), ANY_VALUE(l2), ANY_VALUE(d2), ANY_VALUE(f2) "
        + "FROM druid.numfoo "
        + "WHERE dim1 IS NOT NULL AND l2 IS NOT NULL AND d2 IS NOT NULL AND f2 is NOT NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(filter)
                  .aggregators(
                      aggregators(
                          new StringAnyAggregatorFactory("a0", "dim1", 32),
                          new LongAnyAggregatorFactory("a1", "l2"),
                          new DoubleAnyAggregatorFactory("a2", "d2"),
                          new FloatAnyAggregatorFactory("a3", "f2")
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // first row of dim1 is empty string, which is null in default mode
            new Object[]{"10.1", 325323L, 1.7, 0.1f}
        )
    );
  }

  @Test
  public void testOrderByEarliestFloat() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0f},
          new Object[]{"2", 0.0f},
          new Object[]{"abc", 0.0f},
          new Object[]{"def", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    }
    testQuery(
        "SELECT dim1, EARLIEST(f1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                  .aggregators(
                      aggregators(
                          new FloatFirstAggregatorFactory("a0", "f1")
                      )
                  )
                  .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                  .threshold(10)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByEarliestDouble() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0},
          new Object[]{"2", 0.0},
          new Object[]{"abc", 0.0},
          new Object[]{"def", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    }
    testQuery(
        "SELECT dim1, EARLIEST(d1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new DoubleFirstAggregatorFactory("a0", "d1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByEarliestLong() throws Exception
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0L},
          new Object[]{"2", 0L},
          new Object[]{"abc", 0L},
          new Object[]{"def", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    }
    testQuery(
        "SELECT dim1, EARLIEST(l1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new LongFirstAggregatorFactory("a0", "l1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByLatestFloat() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0f},
          new Object[]{"2", 0.0f},
          new Object[]{"abc", 0.0f},
          new Object[]{"def", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    }

    testQuery(
        "SELECT dim1, LATEST(f1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new FloatLastAggregatorFactory("a0", "f1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByLatestDouble() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0},
          new Object[]{"2", 0.0},
          new Object[]{"abc", 0.0},
          new Object[]{"def", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    }
    testQuery(
        "SELECT dim1, LATEST(d1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new DoubleLastAggregatorFactory("a0", "d1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByLatestLong() throws Exception
  {
    // Cannot vectorize LATEST aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0L},
          new Object[]{"2", 0L},
          new Object[]{"abc", 0L},
          new Object[]{"def", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null},
          new Object[]{"2", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    }
    testQuery(
        "SELECT dim1, LATEST(l1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new LongLastAggregatorFactory("a0", "l1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByAnyFloat() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0f},
          new Object[]{"2", 0.0f},
          new Object[]{"abc", 0.0f},
          new Object[]{"def", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"2", 0.0f},
          new Object[]{"10.1", 0.1f},
          new Object[]{"", 1.0f},
          // Nulls are last because of the null first wrapped Comparator in InvertedTopNMetricSpec which is then
          // reversed by TopNNumericResultBuilder.build()
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null}
      );
    }

    testQuery(
        "SELECT dim1, ANY_VALUE(f1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new FloatAnyAggregatorFactory("a0", "f1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByAnyDouble() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0.0},
          new Object[]{"2", 0.0},
          new Object[]{"abc", 0.0},
          new Object[]{"def", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"2", 0.0},
          new Object[]{"", 1.0},
          new Object[]{"10.1", 1.7},
          // Nulls are last because of the null first wrapped Comparator in InvertedTopNMetricSpec which is then
          // reversed by TopNNumericResultBuilder.build()
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null}
      );
    }
    testQuery(
        "SELECT dim1, ANY_VALUE(d1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new DoubleAnyAggregatorFactory("a0", "d1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testOrderByAnyLong() throws Exception
  {
    // Cannot vectorize ANY aggregator.
    skipVectorize();
    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"1", 0L},
          new Object[]{"2", 0L},
          new Object[]{"abc", 0L},
          new Object[]{"def", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{"2", 0L},
          new Object[]{"", 7L},
          new Object[]{"10.1", 325323L},
          // Nulls are last because of the null first wrapped Comparator in InvertedTopNMetricSpec which is then
          // reversed by TopNNumericResultBuilder.build()
          new Object[]{"1", null},
          new Object[]{"abc", null},
          new Object[]{"def", null}
      );
    }
    testQuery(
        "SELECT dim1, ANY_VALUE(l1) FROM druid.numfoo GROUP BY 1 ORDER BY 2 LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "_d0"))
                .aggregators(
                    aggregators(
                        new LongAnyAggregatorFactory("a0", "l1")
                    )
                )
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("a0")))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testGroupByLong() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByOrdinal() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*) FROM druid.foo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  @Ignore("Disabled since GROUP BY alias can confuse the validator; see DruidConformance::isGroupByAlias")
  public void testGroupByAndOrderByAlias() throws Exception
  {
    testQuery(
        "SELECT cnt AS theCnt, COUNT(*) FROM druid.foo GROUP BY theCnt ORDER BY theCnt ASC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
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
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByExpressionAliasedAsOriginalColumnName() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "FLOOR(__time TO MONTH) AS __time,\n"
        + "COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY FLOOR(__time TO MONTH)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L},
            new Object[]{timestamp("2001-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByAndOrderByOrdinalOfAlias() throws Exception
  {
    testQuery(
        "SELECT cnt as theCnt, COUNT(*) FROM druid.foo GROUP BY 1 ORDER BY 1 ASC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
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
            new Object[]{1L, 6L}
        )
    );
  }

  @Test
  public void testGroupByFloat() throws Exception
  {
    testQuery(
        "SELECT m1, COUNT(*) FROM druid.foo GROUP BY m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("m1", "d0", ValueType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{3.0f, 1L},
            new Object[]{4.0f, 1L},
            new Object[]{5.0f, 1L},
            new Object[]{6.0f, 1L}
        )
    );
  }

  @Test
  public void testGroupByDouble() throws Exception
  {
    testQuery(
        "SELECT m2, COUNT(*) FROM druid.foo GROUP BY m2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("m2", "d0", ValueType.DOUBLE)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0d, 1L},
            new Object[]{2.0d, 1L},
            new Object[]{3.0d, 1L},
            new Object[]{4.0d, 1L},
            new Object[]{5.0d, 1L},
            new Object[]{6.0d, 1L}
        )
    );
  }

  @Test
  public void testFilterOnFloat() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m1 = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(selector("m1", "1.0", null))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testFilterOnDouble() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m2 = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(selector("m2", "1.0", null))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testHavingOnGrandTotal() throws Exception
  {
    testQuery(
        "SELECT SUM(m1) AS m1_sum FROM foo HAVING m1_sum = 21",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(having(selector("a0", "21", null)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{21d}
        )
    );
  }

  @Test
  public void testHavingOnDoubleSum() throws Exception
  {
    testQuery(
        "SELECT dim1, SUM(m1) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING SUM(m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(
                            having(
                                new BoundDimFilter(
                                    "a0",
                                    "1",
                                    null,
                                    true,
                                    false,
                                    false,
                                    null,
                                    StringComparators.NUMERIC
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0d},
            new Object[]{"10.1", 2.0d},
            new Object[]{"2", 3.0d},
            new Object[]{"abc", 6.0d},
            new Object[]{"def", 5.0d}
        )
    );
  }

  @Test
  public void testHavingOnApproximateCountDistinct() throws Exception
  {
    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(
                            aggregators(
                                new CardinalityAggregatorFactory(
                                    "a0",
                                    null,
                                    ImmutableList.of(
                                        new DefaultDimensionSpec("m1", "m1", ValueType.FLOAT)
                                    ),
                                    false,
                                    true
                                )
                            )
                        )
                        .setHavingSpec(
                            having(
                                bound(
                                    "a0",
                                    "1",
                                    null,
                                    true,
                                    false,
                                    null,
                                    StringComparators.NUMERIC
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 2L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  public void testHavingOnExactCountDistinct() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, COUNT(DISTINCT m1) FROM druid.foo GROUP BY dim2 HAVING COUNT(DISTINCT m1) > 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(
                                                dimensions(
                                                    new DefaultDimensionSpec("dim2", "d0", ValueType.STRING),
                                                    new DefaultDimensionSpec("m1", "d1", ValueType.FLOAT)
                                                )
                                            )
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("d0", "_d0", ValueType.STRING)))
                        .setAggregatorSpecs(
                            aggregators(
                                useDefault
                                ? new CountAggregatorFactory("a0")
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    not(selector("d1", null, null))
                                )
                            )
                        )
                        .setHavingSpec(
                            having(
                                bound(
                                    "a0",
                                    "1",
                                    null,
                                    true,
                                    false,
                                    null,
                                    StringComparators.NUMERIC
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 2L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"a", 2L}
        )
    );
  }

  @Test
  public void testHavingOnFloatSum() throws Exception
  {
    testQuery(
        "SELECT dim1, CAST(SUM(m1) AS FLOAT) AS m1_sum FROM druid.foo GROUP BY dim1 HAVING CAST(SUM(m1) AS FLOAT) > 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(
                            having(
                                new BoundDimFilter(
                                    "a0",
                                    "1",
                                    null,
                                    true,
                                    false,
                                    false,
                                    null,
                                    StringComparators.NUMERIC
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"1", 4.0f},
            new Object[]{"10.1", 2.0f},
            new Object[]{"2", 3.0f},
            new Object[]{"abc", 6.0f},
            new Object[]{"def", 5.0f}
        )
    );
  }

  @Test
  public void testColumnComparison() throws Exception
  {
    // Cannot vectorize due to expression filter.
    cannotVectorize();

    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - 1 = dim1 GROUP BY dim1, m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(expressionFilter("((\"m1\" - 1) == CAST(\"dim1\", 'DOUBLE'))"))
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("m1", "d1", ValueType.FLOAT)
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 1.0f, 1L},
            new Object[]{"2", 3.0f, 1L}
        ) :
        ImmutableList.of(
            new Object[]{"2", 3.0f, 1L}
        )
    );
  }

  @Test
  public void testHavingOnRatio() throws Exception
  {
    // Test for https://github.com/apache/druid/issues/4264

    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  COUNT(*) FILTER(WHERE dim2 <> 'a')/COUNT(*) as ratio\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "HAVING COUNT(*) FILTER(WHERE dim2 <> 'a')/COUNT(*) = 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                not(selector("dim2", "a", null))
                            ),
                            new CountAggregatorFactory("a1")
                        ))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "(\"a0\" / \"a1\")")
                        ))
                        .setHavingSpec(having(expressionFilter("((\"a0\" / \"a1\") == 1)")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testGroupByWithSelectProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "substring(\"d0\", 1, -1)")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", NULL_STRING},
            new Object[]{"1", NULL_STRING},
            new Object[]{"10.1", "0.1"},
            new Object[]{"2", NULL_STRING},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"}
        )
    );
  }

  @Test
  public void testGroupByWithSelectAndOrderByProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "ORDER BY CHARACTER_LENGTH(dim1) DESC, dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "substring(\"d0\", 1, -1)"),
                            expressionPostAgg("p1", "strlen(\"d0\")")
                        ))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(
                                new OrderByColumnSpec(
                                    "p1",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.NUMERIC
                                ),
                                new OrderByColumnSpec(
                                    "d0",
                                    OrderByColumnSpec.Direction.ASCENDING,
                                    StringComparators.LEXICOGRAPHIC
                                )
                            ),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "0.1"},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"},
            new Object[]{"1", NULL_STRING},
            new Object[]{"2", NULL_STRING},
            new Object[]{"", NULL_STRING}
        )
    );
  }

  @Test
  public void testTopNWithSelectProjections() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .postAggregators(expressionPostAgg("s0", "substring(\"d0\", 1, -1)"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", NULL_STRING},
            new Object[]{"1", NULL_STRING},
            new Object[]{"10.1", "0.1"},
            new Object[]{"2", NULL_STRING},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"}
        )
    );
  }

  @Test
  public void testTopNWithSelectAndOrderByProjections() throws Exception
  {

    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  SUBSTRING(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "ORDER BY CHARACTER_LENGTH(dim1) DESC\n"
        + "LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .postAggregators(
                    expressionPostAgg("p0", "substring(\"d0\", 1, -1)"),
                    expressionPostAgg("p1", "strlen(\"d0\")")
                )
                .metric(new NumericTopNMetricSpec("p1"))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", "0.1"},
            new Object[]{"abc", "bc"},
            new Object[]{"def", "ef"},
            new Object[]{"1", NULL_STRING},
            new Object[]{"2", NULL_STRING},
            new Object[]{"", NULL_STRING}
        )
    );
  }

  @Test
  public void testUnionAll() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{6L}, new Object[]{6L})
    );
  }

  @Test
  public void testUnionAllWithLimit() throws Exception
  {
    testQuery(
        "SELECT * FROM ("
        + "SELECT COUNT(*) FROM foo UNION ALL SELECT SUM(cnt) FROM foo UNION ALL SELECT COUNT(*) FROM foo"
        + ") LIMIT 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build(),
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L}, new Object[]{6L})
    );
  }

  @Test
  public void testPruneDeadAggregators() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.

    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt)\n"
        + "  WHEN 'foo' THEN SUM(m1)\n"
        + "  WHEN 'baz' THEN SUM(m2)\n"
        + "  END\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{21.0})
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughPostProjection() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.

    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt) / 10\n"
        + "  WHEN 'foo' THEN SUM(m1) / 10\n"
        + "  WHEN 'baz' THEN SUM(m2) / 10\n"
        + "  END\n"
        + "FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                  .postAggregators(ImmutableList.of(expressionPostAgg("p0", "(\"a0\" / 10)")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2.1})
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughHaving() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.

    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN 'bar' THEN SUM(cnt)\n"
        + "  WHEN 'foo' THEN SUM(m1)\n"
        + "  WHEN 'baz' THEN SUM(m2)\n"
        + "  END AS theCase\n"
        + "FROM foo\n"
        + "HAVING theCase = 21",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(new DoubleSumAggregatorFactory("a0", "m1")))
                        .setHavingSpec(having(selector("a0", "21", null)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{21.0})
    );
  }

  @Test
  public void testGroupByCaseWhen() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  CASE EXTRACT(DAY FROM __time)\n"
        + "    WHEN m1 THEN 'match-m1'\n"
        + "    WHEN cnt THEN 'match-cnt'\n"
        + "    WHEN 0 THEN 'zero'"
        + "    END,"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY"
        + "  CASE EXTRACT(DAY FROM __time)\n"
        + "    WHEN m1 THEN 'match-m1'\n"
        + "    WHEN cnt THEN 'match-cnt'\n"
        + "    WHEN 0 THEN 'zero'"
        + "    END",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched("
                                + "(CAST(timestamp_extract(\"__time\",'DAY','UTC'), 'DOUBLE') == \"m1\"),"
                                + "'match-m1',"
                                + "(timestamp_extract(\"__time\",'DAY','UTC') == \"cnt\"),"
                                + "'match-cnt',"
                                + "(timestamp_extract(\"__time\",'DAY','UTC') == 0),"
                                + "'zero',"
                                + DruidExpression.nullLiteral() + ")",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 2L},
            new Object[]{"match-cnt", 1L},
            new Object[]{"match-m1", 3L}
        )
    );
  }

  @Test
  public void testGroupByCaseWhenOfTripleAnd() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  CASE WHEN m1 > 1 AND m1 < 5 AND cnt = 1 THEN 'x' ELSE NULL END,"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(((\"m1\" > 1) && (\"m1\" < 5) && (\"cnt\" == 1)),'x',null)",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 3L},
            new Object[]{"x", 3L}
        )
    );
  }

  @Test
  public void testNullEmptyStringEquality() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE NULLIF(dim2, 'a') IS NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  // Ideally the following filter should be simplified to (dim2 == 'a' || dim2 IS NULL), the
                  // (dim2 != 'a') component is unnecessary.
                  .filters(
                      or(
                          selector("dim2", "a", null),
                          and(
                              selector("dim2", null, null),
                              not(selector("dim2", "a", null))
                          )
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            NullHandling.replaceWithDefault() ?
            // Matches everything but "abc"
            new Object[]{5L} :
            // match only null values
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testNullLongFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 IS NULL",
        useDefault ? ImmutableList.of() : ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(selector("l1", null, null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{0L} : new Object[]{3L}
        )
    );
  }

  @Test
  public void testNullDoubleFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE d1 IS NULL",
        useDefault ? ImmutableList.of() : ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(selector("d1", null, null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{0L} : new Object[]{3L}
        )
    );
  }


  @Test
  public void testNullFloatFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE f1 IS NULL",
        useDefault ? ImmutableList.of() : ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(selector("f1", null, null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{0L} : new Object[]{3L}
        )
    );
  }

  @Test
  public void testNullDoubleTopN() throws Exception
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{1.7, 1L},
          new Object[]{1.0, 1L},
          new Object[]{0.0, 4L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{1.7, 1L},
          new Object[]{1.0, 1L},
          new Object[]{0.0, 1L}
      );
    }
    testQuery(
        "SELECT d1, COUNT(*) FROM druid.numfoo GROUP BY d1 ORDER BY d1 DESC LIMIT 10",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("d1", "_d0", ValueType.DOUBLE))
                .threshold(10)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.NUMERIC)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testNullFloatTopN() throws Exception
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{1.0f, 1L},
          new Object[]{0.1f, 1L},
          new Object[]{0.0f, 4L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{1.0f, 1L},
          new Object[]{0.1f, 1L},
          new Object[]{0.0f, 1L}
      );
    }
    testQuery(
        "SELECT f1, COUNT(*) FROM druid.numfoo GROUP BY f1 ORDER BY f1 DESC LIMIT 10",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("f1", "_d0", ValueType.FLOAT))
                .threshold(10)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.NUMERIC)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testNullLongTopN() throws Exception
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{325323L, 1L},
          new Object[]{7L, 1L},
          new Object[]{0L, 4L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{325323L, 1L},
          new Object[]{7L, 1L},
          new Object[]{0L, 1L}
      );
    }
    testQuery(
        "SELECT l1, COUNT(*) FROM druid.numfoo GROUP BY l1 ORDER BY l1 DESC LIMIT 10",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("l1", "_d0", ValueType.LONG))
                .threshold(10)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .metric(
                    new InvertedTopNMetricSpec(
                        new DimensionTopNMetricSpec(null, StringComparators.NUMERIC)
                    )
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testLongPredicateFilterNulls() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > 3",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(bound("l1", "3", null, true, false, null, StringComparators.NUMERIC))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L})
    );
  }

  @Test
  public void testDoublePredicateFilterNulls() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE d1 > 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(bound("d1", "0", null, true, false, null, StringComparators.NUMERIC))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L})
    );
  }

  @Test
  public void testFloatPredicateFilterNulls() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE f1 > 0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(bound("f1", "0", null, true, false, null, StringComparators.NUMERIC))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L})
    );
  }

  @Test
  public void testEmptyStringEquality() throws Exception
  {
    if (NullHandling.replaceWithDefault()) {
      testQuery(
          "SELECT COUNT(*)\n"
          + "FROM druid.foo\n"
          + "WHERE NULLIF(dim2, 'a') = ''",
          ImmutableList.of(
              Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .filters(in("dim2", ImmutableList.of("", "a"), null))
                    .aggregators(aggregators(new CountAggregatorFactory("a0")))
                    .context(TIMESERIES_CONTEXT_DEFAULT)
                    .build()
          ),
          ImmutableList.of(
              // Matches everything but "abc"
              new Object[]{5L}
          )
      );
    } else {
      testQuery(
          "SELECT COUNT(*)\n"
          + "FROM druid.foo\n"
          + "WHERE NULLIF(dim2, 'a') = ''",
          ImmutableList.of(
              Druids.newTimeseriesQueryBuilder()
                    .dataSource(CalciteTests.DATASOURCE1)
                    .intervals(querySegmentSpec(Filtration.eternity()))
                    .granularity(Granularities.ALL)
                    .filters(selector("dim2", "", null))
                    .aggregators(aggregators(new CountAggregatorFactory("a0")))
                    .context(TIMESERIES_CONTEXT_DEFAULT)
                    .build()
          ),
          ImmutableList.of(
              // match only empty string
              new Object[]{1L}
          )
      );
    }
  }

  @Test
  public void testNullStringEquality() throws Exception
  {
    // In Calcite 1.21, this query is optimized to return 0 without generating a native Druid query, since
    // null is not equal to null or any other value.
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE NULLIF(dim2, 'a') = null",
        ImmutableList.of(),
        ImmutableList.of(new Object[]{0L})
    );
  }

  @Test
  public void testCoalesceColumns() throws Exception
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COALESCE(dim2, dim1), COUNT(*) FROM druid.foo GROUP BY COALESCE(dim2, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",\"dim1\")",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testColumnIsNull() throws Exception
  {
    // Doesn't conform to the SQL standard, but it's how we do it.
    // This example is used in the sql.md doc.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 IS NULL\n",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(selector("dim2", null, null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 3L : 2L}
        )
    );
  }

  @Test
  public void testSelfJoin() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo x, druid.foo y\n",
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
                                  .context(QUERY_CONTEXT_DEFAULT)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{36L}
        )
    );
  }

  @Test
  public void testUnplannableQueries()
  {
    // All of these queries are unplannable because they rely on features Druid doesn't support.
    // This test is here to confirm that we don't fall back to Calcite's interpreter or enumerable implementation.
    // It's also here so when we do support these features, we can have "real" tests for these queries.

    final List<String> queries = ImmutableList.of(
        // SELECT query with order by non-__time.
        "SELECT dim1 FROM druid.foo ORDER BY dim1",

        // DISTINCT with OFFSET.
        "SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5",

        // JOIN condition with not-equals (<>).
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo INNER JOIN lookup.lookyloo l ON foo.dim2 <> l.k",

        // JOIN condition with a function of both sides.
        "SELECT foo.dim1, foo.dim2, l.k, l.v\n"
        + "FROM foo INNER JOIN lookup.lookyloo l ON CHARACTER_LENGTH(foo.dim2 || l.k) > 3\n",

        // Interpreted as a JOIN against VALUES.
        "SELECT COUNT(*) FROM foo WHERE dim1 IN (NULL)"
    );

    for (final String query : queries) {
      assertQueryIsUnplannable(query);
    }
  }

  @Test
  public void testTwoExactCountDistincts() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_HLL,
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
                                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ValueType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                )
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setAggregatorSpecs(
                                    new FilteredAggregatorFactory(
                                        new CountAggregatorFactory("a0"),
                                        not(selector("d0", null, null))
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
                                        .setDimensions(new DefaultDimensionSpec("dim2", "d0", ValueType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                                )
                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setAggregatorSpecs(
                                    new FilteredAggregatorFactory(
                                        new CountAggregatorFactory("a0"),
                                        not(selector("d0", null, null))
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
  public void testUnplannableTwoExactCountDistincts()
  {
    // Requires GROUPING SETS + GROUPING to be translated by AggregateExpandDistinctAggregatesRule.

    assertQueryIsUnplannable(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, COUNT(distinct dim1), COUNT(distinct dim2) FROM druid.foo GROUP BY dim2"
    );
  }

  @Test
  public void testUnplannableExactCountDistinctOnSketch()
  {
    // COUNT DISTINCT on a sketch cannot be exact.

    assertQueryIsUnplannable(
        PLANNER_CONFIG_NO_HLL,
        "SELECT COUNT(distinct unique_dim1) FROM druid.foo"
    );
  }

  @Test
  public void testSelectStarWithDimFilter() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo WHERE dim1 > 'd' OR dim2 = 'a'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    or(
                        bound("dim1", "d", null, true, false, null, StringComparators.LEXICOGRAPHIC),
                        selector("dim2", "a", null)
                    )
                )
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 1L, "", "a", "[\"a\",\"b\"]", 1.0f, 1.0d, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), 1L, "1", "a", "", 4.0f, 4.0d, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc", NULL_STRING, 5.0f, 5.0d, HLLC_STRING}
        )
    );
  }

  @Test
  public void testGroupByNothingWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{0L, null}
        )
    );
  }

  @Test
  public void testGroupByNothingWithImpossibleTimeFilter() throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/7671

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE FLOOR(__time TO DAY) = TIMESTAMP '2000-01-02 01:00:00'\n"
        + "OR FLOOR(__time TO DAY) = TIMESTAMP '2000-01-02 02:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec())
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByOneColumnWithLiterallyFalseFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE 1 = 0 GROUP BY dim1",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothing() throws Exception
  {
    // This query should actually return [0, null] rather than an empty result set, but it doesn't.
    // This test just "documents" the current behavior.

    // Cannot vectorize due to "longMax" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(selector("dim1", "foobar", null))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CountAggregatorFactory("a0"),
                      new LongMaxAggregatorFactory("a1", "cnt")
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByWithGroupByEmpty() throws Exception
  {
    testQuery(
        "SELECT COUNT(*), SUM(cnt) FROM druid.foo GROUP BY ()",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CountAggregatorFactory("a0"),
                      new LongSumAggregatorFactory("a1", "cnt")
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L, 6L})
    );
  }

  @Test
  public void testGroupByWithFilterMatchingNothingWithGroupByLiteral() throws Exception
  {
    // Cannot vectorize due to "longMax" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*), MAX(cnt) FROM druid.foo WHERE dim1 = 'foobar' GROUP BY 'dummy'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(selector("dim1", "foobar", null))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CountAggregatorFactory("a0"),
                      new LongMaxAggregatorFactory("a1", "cnt")
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testCountNonNullColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(cnt) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          useDefault
                          ? new CountAggregatorFactory("a0")
                          : new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a0"),
                              not(selector("cnt", null, null))
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountNullableColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(dim2) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0"),
                          not(selector("dim2", null, null))
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{3L}
        ) :
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testCountNullableExpression() throws Exception
  {
    testQuery(
        "SELECT COUNT(CASE WHEN dim2 = 'abc' THEN 'yes' WHEN dim2 = 'def' THEN 'yes' END) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0"),
                          in("dim2", ImmutableList.of("abc", "def"), null)
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStar() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarOnCommonTableExpression() throws Exception
  {
    testQuery(
        "WITH beep (dim1_firstchar) AS (SELECT SUBSTRING(dim1, 1, 1) FROM foo WHERE dim2 = 'a')\n"
        + "SELECT COUNT(*) FROM beep WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(and(
                      selector("dim2", "a", null),
                      not(selector("dim1", "z", new SubstringDimExtractionFn(0, 1)))
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarOnView() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.aview WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(and(
                      selector("dim2", "a", null),
                      not(selector("dim1", "z", new SubstringDimExtractionFn(0, 1)))
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testExplainCountStarOnView() throws Exception
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String explanation =
        "DruidQueryRel(query=[{"
        + "\"queryType\":\"timeseries\","
        + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
        + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
        + "\"descending\":false,"
        + "\"virtualColumns\":[],"
        + "\"filter\":{\"type\":\"and\",\"fields\":[{\"type\":\"selector\",\"dimension\":\"dim2\",\"value\":\"a\",\"extractionFn\":null},{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":\"z\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}}]},"
        + "\"granularity\":{\"type\":\"all\"},"
        + "\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],"
        + "\"postAggregations\":[],"
        + "\"limit\":2147483647,"
        + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"skipEmptyBuckets\":true,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\"}}]"
        + ", signature=[{a0:LONG}])\n";

    testQuery(
        "EXPLAIN PLAN FOR SELECT COUNT(*) FROM aview WHERE dim1_firstchar <> 'z'",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{explanation}
        )
    );
  }

  @Test
  public void testCountStarWithLikeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim1 like 'a%' OR dim2 like '%xb%' escape 'x'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      or(
                          new LikeDimFilter("dim1", "a%", null, null),
                          new LikeDimFilter("dim2", "%xb%", "x", null)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFilters() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt >= 3 OR cnt = 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      or(
                          bound("cnt", "3", null, false, false, null, StringComparators.NUMERIC),
                          selector("cnt", "1", null)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnFloatLiterals() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt > 1.1 and cnt < 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      bound("cnt", "1.1", "100000001.0", true, true, null, StringComparators.NUMERIC)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      selector("cnt", "1.0", null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      selector("cnt", "100000001.0", null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1.0 or cnt = 100000001.0",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      in("cnt", ImmutableList.of("1.0", "100000001.0"), null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnTwoPoints() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = 1 OR cnt = 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(in("cnt", ImmutableList.of("1", "2"), null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testFilterOnStringAsNumber() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE "
        + "dim1 = 10 OR "
        + "(floor(CAST(dim1 AS float)) = 10.00 and CAST(dim1 AS float) > 9 and CAST(dim1 AS float) <= 10.5)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "floor(CAST(\"dim1\", 'DOUBLE'))",
                                ValueType.DOUBLE
                            )
                        )
                        .setDimFilter(
                            or(
                                bound("dim1", "10", "10", false, false, null, StringComparators.NUMERIC),
                                and(
                                    selector("v0", "10.00", null),
                                    bound("dim1", "9", "10.5", true, false, null, StringComparators.NUMERIC)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testSimpleAggregations() throws Exception
  {
    // Cannot vectorize due to "longMax" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*), COUNT(cnt), COUNT(dim1), AVG(cnt), SUM(cnt), SUM(cnt) + MIN(cnt) + MAX(cnt), COUNT(dim2), COUNT(d1), AVG(d1) FROM druid.numfoo",
        ImmutableList.of(

            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      useDefault
                      ? aggregators(
                          new CountAggregatorFactory("a0"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              not(selector("dim1", null, null))
                          ),
                          new LongSumAggregatorFactory("a2:sum", "cnt"),
                          new CountAggregatorFactory("a2:count"),
                          new LongSumAggregatorFactory("a3", "cnt"),
                          new LongMinAggregatorFactory("a4", "cnt"),
                          new LongMaxAggregatorFactory("a5", "cnt"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a6"),
                              not(selector("dim2", null, null))
                          ),
                          new DoubleSumAggregatorFactory("a7:sum", "d1"),
                          new CountAggregatorFactory("a7:count")
                      )
                      : aggregators(
                          new CountAggregatorFactory("a0"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              not(selector("cnt", null, null))
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a2"),
                              not(selector("dim1", null, null))
                          ),
                          new LongSumAggregatorFactory("a3:sum", "cnt"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a3:count"),
                              not(selector("cnt", null, null))
                          ),
                          new LongSumAggregatorFactory("a4", "cnt"),
                          new LongMinAggregatorFactory("a5", "cnt"),
                          new LongMaxAggregatorFactory("a6", "cnt"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a7"),
                              not(selector("dim2", null, null))
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a8"),
                              not(selector("d1", null, null))
                          ),
                          new DoubleSumAggregatorFactory("a9:sum", "d1"),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a9:count"),
                              not(selector("d1", null, null))
                          )
                      )
                  )
                  .postAggregators(
                      new ArithmeticPostAggregator(
                          useDefault ? "a2" : "a3",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, useDefault ? "a2:sum" : "a3:sum"),
                              new FieldAccessPostAggregator(null, useDefault ? "a2:count" : "a3:count")
                          )
                      ),
                      new ArithmeticPostAggregator(
                          useDefault ? "a7" : "a9",
                          "quotient",
                          ImmutableList.of(
                              new FieldAccessPostAggregator(null, useDefault ? "a7:sum" : "a9:sum"),
                              new FieldAccessPostAggregator(null, useDefault ? "a7:count" : "a9:count")
                          )
                      ),
                      expressionPostAgg(
                          "p0",
                          useDefault ? "((\"a3\" + \"a4\") + \"a5\")" : "((\"a4\" + \"a5\") + \"a6\")"
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 6L, 5L, 1L, 6L, 8L, 3L, 6L, ((1 + 1.7) / 6)}
        ) :
        ImmutableList.of(
            new Object[]{6L, 6L, 6L, 1L, 6L, 8L, 4L, 3L, ((1 + 1.7) / 3)}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationDefault() throws Exception
  {
    // By default this query uses topN.

    testQuery(
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0"))
                .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec("p0")))
                .aggregators(
                    new FloatMinAggregatorFactory("a0", "m1"),
                    new FloatMaxAggregatorFactory("a1", "m1")
                )
                .postAggregators(expressionPostAgg("p0", "(\"a0\" + \"a1\")"))
                .threshold(3)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0f},
            new Object[]{"10.1", 4.0f},
            new Object[]{"2", 6.0f}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNConfig() throws Exception
  {
    // Use PlannerConfig to disable topN, so this query becomes a groupBy.

    // Cannot vectorize due to "floatMin", "floatMax" aggregators.
    cannotVectorize();

    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(
                            new FloatMinAggregatorFactory("a0", "m1"),
                            new FloatMaxAggregatorFactory("a1", "m1")
                        )
                        .setPostAggregatorSpecs(ImmutableList.of(expressionPostAgg("p0", "(\"a0\" + \"a1\")")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "p0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                3
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0f},
            new Object[]{"10.1", 4.0f},
            new Object[]{"2", 6.0f}
        )
    );
  }

  @Test
  public void testGroupByWithSortOnPostAggregationNoTopNContext() throws Exception
  {
    // Use context to disable topN, so this query becomes a groupBy.

    // Cannot vectorize due to "floatMin", "floatMax" aggregators.
    cannotVectorize();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_NO_TOPN,
        "SELECT dim1, MIN(m1) + MAX(m1) AS x FROM druid.foo GROUP BY dim1 ORDER BY x LIMIT 3",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setAggregatorSpecs(
                            new FloatMinAggregatorFactory("a0", "m1"),
                            new FloatMaxAggregatorFactory("a1", "m1")
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                expressionPostAgg("p0", "(\"a0\" + \"a1\")")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "p0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                3
                            )
                        )
                        .setContext(QUERY_CONTEXT_NO_TOPN)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2.0f},
            new Object[]{"10.1", 4.0f},
            new Object[]{"2", 6.0f}
        )
    );
  }

  @Test
  public void testFilteredAggregations() throws Exception
  {
    // Cannot vectorize due to "cardinality", "longMax" aggregators.
    cannotVectorize();

    testQuery(
        "SELECT "
        + "SUM(case dim1 when 'abc' then cnt end), "
        + "SUM(case dim1 when 'abc' then null else cnt end), "
        + "SUM(case substring(dim1, 1, 1) when 'a' then cnt end), "
        + "COUNT(dim2) filter(WHERE dim1 <> '1'), "
        + "COUNT(CASE WHEN dim1 <> '1' THEN 'dummy' END), "
        + "SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END), "
        + "SUM(cnt) filter(WHERE dim2 = 'a'), "
        + "SUM(case when dim1 <> '1' then cnt end) filter(WHERE dim2 = 'a'), "
        + "SUM(CASE WHEN dim1 <> '1' THEN cnt ELSE 0 END), "
        + "MAX(CASE WHEN dim1 <> '1' THEN cnt END), "
        + "COUNT(DISTINCT CASE WHEN dim1 <> '1' THEN m1 END), "
        + "SUM(cnt) filter(WHERE dim2 = 'a' AND dim1 = 'b') "
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          selector("dim1", "abc", null)
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a1", "cnt"),
                          not(selector("dim1", "abc", null))
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a2", "cnt"),
                          selector("dim1", "a", new SubstringDimExtractionFn(0, 1))
                      ),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a3"),
                          and(
                              not(selector("dim2", null, null)),
                              not(selector("dim1", "1", null))
                          )
                      ),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a4"),
                          not(selector("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a5"),
                          not(selector("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a6", "cnt"),
                          selector("dim2", "a", null)
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a7", "cnt"),
                          and(
                              selector("dim2", "a", null),
                              not(selector("dim1", "1", null))
                          )
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a8", "cnt"),
                          not(selector("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new LongMaxAggregatorFactory("a9", "cnt"),
                          not(selector("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new CardinalityAggregatorFactory(
                              "a10",
                              null,
                              dimensions(new DefaultDimensionSpec("m1", "m1", ValueType.FLOAT)),
                              false,
                              true
                          ),
                          not(selector("dim1", "1", null))
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a11", "cnt"),
                          and(selector("dim2", "a", null), selector("dim1", "b", null))
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{1L, 5L, 1L, 2L, 5L, 5L, 2L, 1L, 5L, 1L, 5L, 0L}
        ) :
        ImmutableList.of(
            new Object[]{1L, 5L, 1L, 3L, 5L, 5L, 2L, 1L, 5L, 1L, 5L, null}
        )
    );
  }

  @Test
  public void testCaseFilteredAggregationWithGroupBy() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  cnt,\n"
        + "  SUM(CASE WHEN dim1 <> '1' THEN 1 ELSE 0 END) + SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY cnt",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                not(selector("dim1", "1", null))
                            ),
                            new LongSumAggregatorFactory("a1", "cnt")
                        ))
                        .setPostAggregatorSpecs(ImmutableList.of(expressionPostAgg("p0", "(\"a0\" + \"a1\")")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 11L}
        )
    );
  }

  @Test
  public void testFilteredAggregationWithNotIn() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "COUNT(*) filter(WHERE dim1 NOT IN ('1')),\n"
        + "COUNT(dim2) filter(WHERE dim1 NOT IN ('1'))\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a0"),
                              not(selector("dim1", "1", null))
                          ),
                          new FilteredAggregatorFactory(
                              new CountAggregatorFactory("a1"),
                              and(
                                  not(selector("dim2", null, null)),
                                  not(selector("dim1", "1", null))
                              )
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{5L, 2L}
        ) :
        ImmutableList.of(
            new Object[]{5L, 3L}
        )
    );
  }

  @Test
  public void testExpressionAggregations() throws Exception
  {
    // Cannot vectorize due to "doubleMax" aggregator.
    cannotVectorize();

    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt * 3),\n"
        + "  LN(SUM(cnt) + SUM(m1)),\n"
        + "  MOD(SUM(cnt), 4),\n"
        + "  SUM(CHARACTER_LENGTH(CAST(cnt * 10 AS VARCHAR))),\n"
        + "  MAX(CHARACTER_LENGTH(dim2) + LN(m1))\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new LongSumAggregatorFactory("a0", null, "(\"cnt\" * 3)", macroTable),
                      new LongSumAggregatorFactory("a1", "cnt"),
                      new DoubleSumAggregatorFactory("a2", "m1"),
                      new LongSumAggregatorFactory("a3", null, "strlen(CAST((\"cnt\" * 10), 'STRING'))", macroTable),
                      new DoubleMaxAggregatorFactory("a4", null, "(strlen(\"dim2\") + log(\"m1\"))", macroTable)
                  ))
                  .postAggregators(
                      expressionPostAgg("p0", "log((\"a1\" + \"a2\"))"),
                      expressionPostAgg("p1", "(\"a1\" % 4)")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{18L, 3.295836866004329, 2, 12L, 3f + (Math.log(5.0))}
        )
    );
  }

  @Test
  public void testExpressionFilteringAndGrouping() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  FLOOR(m1 / 2) * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(m1 / 2) * 2 > -1\n"
        + "GROUP BY FLOOR(m1 / 2) * 2\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "(floor((\"m1\" / 2)) * 2)", ValueType.FLOAT)
                        )
                        .setDimFilter(bound("v0", "-1", null, true, false, null, StringComparators.NUMERIC))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
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
            new Object[]{6.0f, 1L},
            new Object[]{4.0f, 2L},
            new Object[]{2.0f, 2L},
            new Object[]{0.0f, 1L}
        )
    );
  }

  @Test
  public void testExpressionFilteringAndGroupingUsingCastToLong() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  CAST(m1 AS BIGINT) / 2 * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE CAST(m1 AS BIGINT) / 2 * 2 > -1\n"
        + "GROUP BY CAST(m1 AS BIGINT) / 2 * 2\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "((CAST(\"m1\", 'LONG') / 2) * 2)", ValueType.LONG)
                        )
                        .setDimFilter(
                            bound("v0", "-1", null, true, false, null, StringComparators.NUMERIC)
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
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
            new Object[]{6L, 1L},
            new Object[]{4L, 2L},
            new Object[]{2L, 2L},
            new Object[]{0L, 1L}
        )
    );
  }

  @Test
  public void testExpressionFilteringAndGroupingOnStringCastToNumber() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  FLOOR(CAST(dim1 AS FLOAT) / 2) * 2,\n"
        + "  COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(CAST(dim1 AS FLOAT) / 2) * 2 > -1\n"
        + "GROUP BY FLOOR(CAST(dim1 AS FLOAT) / 2) * 2\n"
        + "ORDER BY 1 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "(floor((CAST(\"dim1\", 'DOUBLE') / 2)) * 2)",
                                ValueType.FLOAT
                            )
                        )
                        .setDimFilter(
                            bound("v0", "-1", null, true, false, null, StringComparators.NUMERIC)
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{0.0f, 4L}
        ) :
        ImmutableList.of(
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{0.0f, 1L}
        )
    );
  }

  @Test
  public void testInFilter() throws Exception
  {
    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN ('abc', 'def', 'ghi') GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(new InDimFilter("dim1", ImmutableList.of("abc", "def", "ghi"), null))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testInFilterWith23Elements() throws Exception
  {
    // Regression test for https://github.com/apache/druid/issues/4203.

    final List<String> elements = new ArrayList<>();
    elements.add("abc");
    elements.add("def");
    elements.add("ghi");
    for (int i = 0; i < 20; i++) {
      elements.add("dummy" + i);
    }

    final String elementsString = Joiner.on(",").join(elements.stream().map(s -> "'" + s + "'").iterator());

    testQuery(
        "SELECT dim1, COUNT(*) FROM druid.foo WHERE dim1 IN (" + elementsString + ") GROUP BY dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(new InDimFilter("dim1", elements, null))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testCountStarWithDegenerateFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      selector("dim2", "a", null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithNotOfDegenerateFilter() throws Exception
  {
    // This query is evaluated in the planner (no native queries are issued) due to the degenerate filter.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE dim2 = 'a' and not (dim1 > 'a' OR dim1 < 'b')",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOnMetric() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE 2.5 < m1 AND m1 < 3.5",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(bound("m1", "2.5", "3.5", true, true, null, StringComparators.NUMERIC))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyOr() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') OR dim1 = 'ab'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(bound("dim1", "a", "b", false, true, null, StringComparators.LEXICOGRAPHIC))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithBoundFilterSimplifyAnd() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE (dim1 >= 'a' and dim1 < 'b') and dim1 = 'abc'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(selector("dim1", "abc", null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithFilterOnCastedString() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE CAST(dim1 AS bigint) = 2",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(numericSelector("dim1", "2", null))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testRemoveUselessCaseWhen() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "  CASE\n"
        + "    WHEN __time >= TIME_PARSE('2000-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss') AND __time < TIMESTAMP '2001-01-01 00:00:00'\n"
        + "    THEN true\n"
        + "    ELSE false\n"
        + "  END\n"
        + "OR\n"
        + "  __time >= TIMESTAMP '2010-01-01 00:00:00' AND __time < TIMESTAMP '2011-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2001"), Intervals.of("2010/2011")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeMillisecondFilters() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time = TIMESTAMP '2000-01-01 00:00:00.111'\n"
        + "OR (__time >= TIMESTAMP '2000-01-01 00:00:00.888' AND __time < TIMESTAMP '2000-01-02 00:00:00.222')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          Intervals.of("2000-01-01T00:00:00.111/2000-01-01T00:00:00.112"),
                          Intervals.of("2000-01-01T00:00:00.888/2000-01-02T00:00:00.222")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterUsingStringLiterals() throws Exception
  {
    // Strings are implicitly cast to timestamps. Test a few different forms.

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= '2000-01-01 00:00:00' AND __time < '2001-01-01T00:00:00'\n"
        + "OR __time >= '2001-02-01' AND __time < '2001-02-02'\n"
        + "OR __time BETWEEN '2001-03-01' AND '2001-03-02'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          Intervals.of("2000-01-01/2001-01-01"),
                          Intervals.of("2001-02-01/2001-02-02"),
                          Intervals.of("2001-03-01/2001-03-02T00:00:00.001")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterUsingStringLiteralsInvalid()
  {
    // Strings are implicitly cast to timestamps. Test an invalid string.
    // This error message isn't ideal but it is at least better than silently ignoring the problem.
    try {
      testQuery(
          "SELECT COUNT(*) FROM druid.foo\n"
          + "WHERE __time >= 'z2000-01-01 00:00:00' AND __time < '2001-01-01 00:00:00'\n",
          ImmutableList.of(),
          ImmutableList.of()
      );
    }
    catch (Throwable t) {
      Throwable rootException = CalciteTests.getRootCauseFromInvocationTargetExceptionChain(t);
      Assert.assertEquals(IAE.class, rootException.getClass());
      Assert.assertEquals(
          "Illegal TIMESTAMP constant: CAST('z2000-01-01 00:00:00'):TIMESTAMP(3) NOT NULL",
          rootException.getMessage()
      );
    }
  }

  @Test
  public void testCountStarWithSinglePointInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE __time = TIMESTAMP '2000-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2000-01-01T00:00:00.001")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithTwoPointsInTime() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "__time = TIMESTAMP '2000-01-01 00:00:00' OR __time = TIMESTAMP '2000-01-01 00:00:00' + INTERVAL '1' DAY",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          Intervals.of("2000-01-01/2000-01-01T00:00:00.001"),
                          Intervals.of("2000-01-02/2000-01-02T00:00:00.001")
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithComplexDisjointTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 = 'a' and ("
        + "  (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "  OR ("
        + "    (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "    and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "    and dim1 = 'abc'"
        + "  )"
        + ")",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2001"), Intervals.of("2002-05-01/2003-05-01")))
                  .granularity(Granularities.ALL)
                  .filters(
                      and(
                          selector("dim2", "a", null),
                          or(
                              timeBound("2000/2001"),
                              and(
                                  selector("dim1", "abc", null),
                                  timeBound("2002-05-01/2003-05-01")
                              )
                          )
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testCountStarWithNotOfComplexDisjointTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE not (dim2 = 'a' and ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR ("
        + "      (__time >= TIMESTAMP '2002-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00')"
        + "      and (__time >= TIMESTAMP '2002-05-01 00:00:00' AND __time < TIMESTAMP '2003-05-01 00:00:00')"
        + "      and dim1 = 'abc'"
        + "    )"
        + "  )"
        + ")",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      or(
                          not(selector("dim2", "a", null)),
                          and(
                              not(timeBound("2000/2001")),
                              not(and(
                                  selector("dim1", "abc", null),
                                  timeBound("2002-05-01/2003-05-01")
                              ))
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testCountStarWithNotTimeFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim1 <> 'xxx' and not ("
        + "    (__time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2001-01-01 00:00:00')"
        + "    OR (__time >= TIMESTAMP '2003-01-01 00:00:00' AND __time < TIMESTAMP '2004-01-01 00:00:00'))",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(
                      querySegmentSpec(
                          new Interval(DateTimes.MIN, DateTimes.of("2000")),
                          Intervals.of("2001/2003"),
                          new Interval(DateTimes.of("2004"), DateTimes.MAX)
                      )
                  )
                  .filters(not(selector("dim1", "xxx", null)))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeAndDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "and __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-01-01")))
                  .filters(not(selector("dim2", "a", null)))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeOrDimFilter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE dim2 <> 'a' "
        + "or __time BETWEEN TIMESTAMP '2000-01-01 00:00:00' AND TIMESTAMP '2000-12-31 23:59:59.999'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(
                      or(
                          not(selector("dim2", "a", null)),
                          bound(
                              "__time",
                              String.valueOf(timestamp("2000-01-01")),
                              String.valueOf(timestamp("2000-12-31T23:59:59.999")),
                              false,
                              false,
                              null,
                              StringComparators.NUMERIC
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpoch() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM TIMESTAMP '1970-01-01 00:00:00') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM TIMESTAMP '1970-01-02 00:00:00') * 1000",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      bound(
                          "cnt",
                          String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                          String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                          false,
                          true,
                          null,
                          StringComparators.NUMERIC
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingExtractEpochFromDate() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= EXTRACT(EPOCH FROM DATE '1970-01-01') * 1000 "
        + "AND cnt < EXTRACT(EPOCH FROM DATE '1970-01-02') * 1000",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      bound(
                          "cnt",
                          String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                          String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                          false,
                          true,
                          null,
                          StringComparators.NUMERIC
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testCountStarWithTimeFilterOnLongColumnUsingTimestampToMillis() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE "
        + "cnt >= TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-01 00:00:00') "
        + "AND cnt < TIMESTAMP_TO_MILLIS(TIMESTAMP '1970-01-02 00:00:00')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      bound(
                          "cnt",
                          String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                          String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                          false,
                          true,
                          null,
                          StringComparators.NUMERIC
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testSumOfString() throws Exception
  {
    // Cannot vectorize due to expressions in aggregators.
    cannotVectorize();

    testQuery(
        "SELECT SUM(CAST(dim1 AS INTEGER)) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new LongSumAggregatorFactory(
                          "a0",
                          null,
                          "CAST(\"dim1\", 'LONG')",
                          CalciteTests.createExprMacroTable()
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{13L}
        )
    );
  }

  @Test
  public void testSumOfExtractionFn() throws Exception
  {
    // Cannot vectorize due to expressions in aggregators.
    cannotVectorize();

    testQuery(
        "SELECT SUM(CAST(SUBSTRING(dim1, 1, 10) AS INTEGER)) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new LongSumAggregatorFactory(
                          "a0",
                          null,
                          "CAST(substring(\"dim1\", 0, 10), 'LONG')",
                          CalciteTests.createExprMacroTable()
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{13L}
        )
    );
  }

  @Test
  public void testTimeseriesWithTimeFilterOnLongColumnUsingMillisToTimestamp() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  FLOOR(MILLIS_TO_TIMESTAMP(cnt) TO YEAR),\n"
        + "  COUNT(*)\n"
        + "FROM\n"
        + "  druid.foo\n"
        + "WHERE\n"
        + "  MILLIS_TO_TIMESTAMP(cnt) >= TIMESTAMP '1970-01-01 00:00:00'\n"
        + "  AND MILLIS_TO_TIMESTAMP(cnt) < TIMESTAMP '1970-01-02 00:00:00'\n"
        + "GROUP BY\n"
        + "  FLOOR(MILLIS_TO_TIMESTAMP(cnt) TO YEAR)",
        ImmutableList.of(
            new GroupByQuery.Builder()
                .setDataSource(CalciteTests.DATASOURCE1)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setVirtualColumns(
                    expressionVirtualColumn("v0", "timestamp_floor(\"cnt\",'P1Y',null,'UTC')", ValueType.LONG)
                )
                .setDimFilter(
                    bound(
                        "cnt",
                        String.valueOf(DateTimes.of("1970-01-01").getMillis()),
                        String.valueOf(DateTimes.of("1970-01-02").getMillis()),
                        false,
                        true,
                        null,
                        StringComparators.NUMERIC
                    )
                )
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("1970-01-01"), 6L}
        )
    );
  }

  @Test
  public void testSelectDistinctWithCascadeExtractionFilter() throws Exception
  {
    testQuery(
        "SELECT distinct dim1 FROM druid.foo WHERE substring(substring(dim1, 2), 1, 1) = 'e' OR dim2 = 'a'",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            or(
                                selector(
                                    "dim1",
                                    "e",
                                    cascade(
                                        new SubstringDimExtractionFn(1, null),
                                        new SubstringDimExtractionFn(0, 1)
                                    )
                                ),
                                selector("dim2", "a", null)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithStrlenFilter() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT distinct dim1 FROM druid.foo "
        + "WHERE CHARACTER_LENGTH(dim1) = 3 OR CAST(CHARACTER_LENGTH(dim1) AS varchar) = 3",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "strlen(\"dim1\")", ValueType.LONG),
                            // The two layers of CASTs here are unusual, they should really be collapsed into one
                            expressionVirtualColumn(
                                "v1",
                                "CAST(CAST(strlen(\"dim1\"), 'STRING'), 'LONG')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setDimFilter(
                            or(
                                selector("v0", "3", null),
                                selector("v1", "3", null)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithLimit() throws Exception
  {
    // Should use topN even if approximate topNs are off, because this query is exact.

    testQuery(
        "SELECT DISTINCT dim2 FROM druid.foo LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(10)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery2() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 5) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"abc"}
        )
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery3() throws Exception
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectDistinctWithSortAsOuterQuery4() throws Exception
  {
    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 DESC LIMIT 5) LIMIT 10",
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim2", "d0"))
                .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"abc"},
            new Object[]{"a"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{"abc"},
            new Object[]{"a"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testCountDistinct() throws Exception
  {
    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT SUM(cnt), COUNT(distinct dim2), COUNT(distinct unique_dim1) FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          ),
                          new HyperUniquesAggregatorFactory("a2", "unique_dim1", false, true)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 6L}
        )
    );
  }

  @Test
  public void testCountDistinctOfCaseWhen() throws Exception
  {
    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN m1 END),\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN dim1 END),\n"
        + "COUNT(DISTINCT CASE WHEN m1 >= 4 THEN unique_dim1 END)\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a0",
                                  null,
                                  ImmutableList.of(new DefaultDimensionSpec("m1", "m1", ValueType.FLOAT)),
                                  false,
                                  true
                              ),
                              bound("m1", "4", null, false, false, null, StringComparators.NUMERIC)
                          ),
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a1",
                                  null,
                                  ImmutableList.of(new DefaultDimensionSpec("dim1", "dim1", ValueType.STRING)),
                                  false,
                                  true
                              ),
                              bound("m1", "4", null, false, false, null, StringComparators.NUMERIC)
                          ),
                          new FilteredAggregatorFactory(
                              new HyperUniquesAggregatorFactory("a2", "unique_dim1", false, true),
                              bound("m1", "4", null, false, false, null, StringComparators.NUMERIC)
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 3L, 3L}
        )
    );
  }

  @Test
  public void testExactCountDistinct() throws Exception
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT COUNT(distinct dim2) FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("a0"),
                                not(selector("d0", null, null))
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 3L}
        )
    );
  }

  @Test
  public void testApproxCountDistinctWhenHllDisabled() throws Exception
  {
    // When HLL is disabled, APPROX_COUNT_DISTINCT is still approximate.

    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT APPROX_COUNT_DISTINCT(dim2) FROM druid.foo",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testExactCountDistinctWithGroupingAndOtherAggregators() throws Exception
  {
    // When HLL is disabled, do exact count distinct through a nested query.

    testQuery(
        PLANNER_CONFIG_NO_HLL,
        "SELECT dim2, SUM(cnt), COUNT(distinct dim1) FROM druid.foo GROUP BY dim2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimensions(dimensions(
                                                new DefaultDimensionSpec("dim2", "d0"),
                                                new DefaultDimensionSpec("dim1", "d1")
                                            ))
                                            .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("d0", "_d0")))
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new FilteredAggregatorFactory(
                                new CountAggregatorFactory("_a1"),
                                not(selector("d1", null, null))
                            )
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", 3L, 3L},
            new Object[]{"a", 2L, 1L},
            new Object[]{"abc", 1L, 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L, 2L},
            new Object[]{"", 1L, 1L},
            new Object[]{"a", 2L, 2L},
            new Object[]{"abc", 1L, 1L}
        )
    );
  }

  @Test
  public void testApproxCountDistinct() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  APPROX_COUNT_DISTINCT(dim2),\n" // uppercase
        + "  approx_count_distinct(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
        + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
        + "  APPROX_COUNT_DISTINCT(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
        + "  approx_count_distinct(unique_dim1)\n" // on native hyperUnique column
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat(substring(\"dim2\", 0, 1),'x')", ValueType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", "dim2")),
                              false,
                              true
                          ),
                          new FilteredAggregatorFactory(
                              new CardinalityAggregatorFactory(
                                  "a2",
                                  null,
                                  dimensions(new DefaultDimensionSpec("dim2", "dim2")),
                                  false,
                                  true
                              ),
                              not(selector("dim2", "", null))
                          ),
                          new CardinalityAggregatorFactory(
                              "a3",
                              null,
                              dimensions(
                                  new ExtractionDimensionSpec(
                                      "dim2",
                                      "dim2",
                                      ValueType.STRING,
                                      new SubstringDimExtractionFn(0, 1)
                                  )
                              ),
                              false,
                              true
                          ),
                          new CardinalityAggregatorFactory(
                              "a4",
                              null,
                              dimensions(new DefaultDimensionSpec("v0", "v0", ValueType.STRING)),
                              false,
                              true
                          ),
                          new HyperUniquesAggregatorFactory("a5", "unique_dim1", false, true)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 3L, 2L, 2L, 2L, 6L}
        ) :
        ImmutableList.of(
            new Object[]{6L, 3L, 2L, 1L, 1L, 6L}
        )
    );
  }

  @Test
  public void testNestedGroupBy() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "    FLOOR(__time to hour) AS __time,\n"
        + "    dim1,\n"
        + "    COUNT(m2)\n"
        + "FROM (\n"
        + "    SELECT\n"
        + "        MAX(__time) AS __time,\n"
        + "        m2,\n"
        + "        dim1\n"
        + "    FROM druid.foo\n"
        + "    WHERE 1=1\n"
        + "        AND m1 = '5.0'\n"
        + "    GROUP BY m2, dim1\n"
        + ")\n"
        + "GROUP BY FLOOR(__time to hour), dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(
                                            new DefaultDimensionSpec("m2", "d0", ValueType.DOUBLE),
                                            new DefaultDimensionSpec("dim1", "d1")
                                        ))
                                        .setDimFilter(new SelectorDimFilter("m1", "5.0", null))
                                        .setAggregatorSpecs(aggregators(new LongMaxAggregatorFactory("a0", "__time")))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"a0\",'PT1H',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("v0", "_d0", ValueType.LONG),
                            new DefaultDimensionSpec("d1", "_d1", ValueType.STRING)
                        ))
                        .setAggregatorSpecs(
                            aggregators(
                                useDefault
                                ? new CountAggregatorFactory("_a0")
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a0"),
                                    not(selector("d0", null, null))
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{978393600000L, "def", 1L}
        )
    );
  }

  @Test
  public void testDoubleNestedGroupBy() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), COUNT(*) FROM (\n"
        + "  SELECT dim2, SUM(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(
                                            GroupByQuery.builder()
                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                        .setGranularity(Granularities.ALL)
                                                        .setDimensions(dimensions(
                                                            new DefaultDimensionSpec("dim1", "d0"),
                                                            new DefaultDimensionSpec("dim2", "d1")
                                                        ))
                                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                                        .build()
                                        )
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(new DefaultDimensionSpec("d1", "_d0")))
                                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("_a0", "a0")))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("a0", "_a0"),
                            new CountAggregatorFactory("a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(new Object[]{6L, 3L})
        : ImmutableList.of(new Object[]{6L, 4L})
    );
  }

  @Test
  public void testDoubleNestedGroupBy2() throws Exception
  {
    // This test fails when AggregateMergeRule is added to Rules.ABSTRACT_RELATIONAL_RULES. So, we don't add that
    // rule for now. Possible bug in the rule.

    testQuery(
        "SELECT MAX(cnt) FROM (\n"
        + "  SELECT dim2, MAX(t1.cnt) cnt FROM (\n"
        + "    SELECT\n"
        + "      dim1,\n"
        + "      dim2,\n"
        + "      COUNT(*) cnt\n"
        + "    FROM druid.foo\n"
        + "    GROUP BY dim1, dim2\n"
        + "  ) t1\n"
        + "  GROUP BY dim2\n"
        + ") t2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(
                                            GroupByQuery.builder()
                                                        .setDataSource(CalciteTests.DATASOURCE1)
                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                        .setGranularity(Granularities.ALL)
                                                        .setDimensions(
                                                            new DefaultDimensionSpec("dim1", "d0"),
                                                            new DefaultDimensionSpec("dim2", "d1")
                                                        )
                                                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                                        .build()
                                        )
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                        .setAggregatorSpecs(new LongMaxAggregatorFactory("_a0", "a0"))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new LongMaxAggregatorFactory("a0", "_a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }

  @Test
  public void testExactCountDistinctUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
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
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 3L}
        ) :
        ImmutableList.of(
            new Object[]{6L, 4L}
        )
    );
  }

  @Test
  public void testMinMaxAvgDailyCountWithLimit() throws Exception
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
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .aggregators(new CountAggregatorFactory("a0"))
                                      .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                                      .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault
                            ? aggregators(
                              new LongMaxAggregatorFactory("_a0", "a0"),
                              new LongMinAggregatorFactory("_a1", "a0"),
                              new LongSumAggregatorFactory("_a2:sum", "a0"),
                              new CountAggregatorFactory("_a2:count"),
                              new LongMaxAggregatorFactory("_a3", "d0"),
                              new CountAggregatorFactory("_a4")
                            )
                            : aggregators(
                                new LongMaxAggregatorFactory("_a0", "a0"),
                                new LongMinAggregatorFactory("_a1", "a0"),
                                new LongSumAggregatorFactory("_a2:sum", "a0"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a2:count"),
                                    not(selector("a0", null, null))
                                ),
                                new LongMaxAggregatorFactory("_a3", "d0"),
                                new CountAggregatorFactory("_a4")
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "_a2",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "_a2:sum"),
                                        new FieldAccessPostAggregator(null, "_a2:count")
                                    )
                                ),
                                expressionPostAgg("s0", "timestamp_extract(\"_a3\",'EPOCH','UTC')")
                            )
                        )
                        .setLimit(1)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{1L, 1L, 1L, 978480000L, 6L})
    );
  }

  @Test
  public void testAvgDailyCountDistinct() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  AVG(u)\n"
        + "FROM (SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT(cnt) AS u FROM druid.foo GROUP BY 1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                Druids.newTimeseriesQueryBuilder()
                                      .dataSource(CalciteTests.DATASOURCE1)
                                      .intervals(querySegmentSpec(Filtration.eternity()))
                                      .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                                      .aggregators(
                                          new CardinalityAggregatorFactory(
                                              "a0:a",
                                              null,
                                              dimensions(new DefaultDimensionSpec(
                                                  "cnt",
                                                  "cnt",
                                                  ValueType.LONG
                                              )),
                                              false,
                                              true
                                          )
                                      )
                                      .postAggregators(
                                          ImmutableList.of(
                                              new HyperUniqueFinalizingPostAggregator("a0", "a0:a")
                                          )
                                      )
                                      .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                                      .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(
                            useDefault
                            ? aggregators(
                              new LongSumAggregatorFactory("_a0:sum", "a0"),
                              new CountAggregatorFactory("_a0:count")
                            )
                            : aggregators(
                                new LongSumAggregatorFactory("_a0:sum", "a0"),
                                new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("_a0:count"),
                                    not(selector("a0", null, null))
                                )
                            )
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ArithmeticPostAggregator(
                                    "_a0",
                                    "quotient",
                                    ImmutableList.of(
                                        new FieldAccessPostAggregator(null, "_a0:sum"),
                                        new FieldAccessPostAggregator(null, "_a0:count")
                                    )
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{1L})
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testTopNFilterJoin(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

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
                                    ValueType.STRING,
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
  public void testExactCountDistinctOfSemiJoinResult() throws Exception
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
                                                            .setDimFilter(not(selector("dim1", "", null)))
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
                                                        DruidExpression.fromExpression("substring(\"dim2\", 0, 1)"),
                                                        DruidExpression.fromColumn("j0.d0")
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
  public void testMaxSubqueryRows() throws Exception
  {
    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Subquery generated results beyond maximum[2]");

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        ImmutableMap.of(QueryContexts.MAX_SUBQUERY_ROWS_KEY, 2),
        "SELECT COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 <> ''\n"
        + ")\n",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testExplainExactCountDistinctOfSemiJoinResult() throws Exception
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String explanation =
        "DruidOuterQueryRel(query=[{\"queryType\":\"timeseries\",\"dataSource\":{\"type\":\"table\",\"name\":\"__subquery__\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"descending\":false,\"virtualColumns\":[],\"filter\":null,\"granularity\":{\"type\":\"all\"},\"aggregations\":[{\"type\":\"count\",\"name\":\"a0\"}],\"postAggregations\":[],\"limit\":2147483647,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"skipEmptyBuckets\":true,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\"}}], signature=[{a0:LONG}])\n"
        + "  DruidJoinQueryRel(condition=[=(SUBSTRING($3, 1, 1), $8)], joinType=[inner], query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"__join__\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[],\"filter\":null,\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"default\",\"dimension\":\"dim2\",\"outputName\":\"d0\",\"outputType\":\"STRING\"}],\"aggregations\":[],\"postAggregations\":[],\"having\":null,\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\"},\"descending\":false}], signature=[{d0:STRING}])\n"
        + "    DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[],\"resultFormat\":\"compactedList\",\"batchSize\":20480,\"limit\":9223372036854775807,\"order\":\"none\",\"filter\":null,\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\"},\"descending\":false,\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, cnt:LONG, dim1:STRING, dim2:STRING, dim3:STRING, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX}])\n"
        + "    DruidQueryRel(query=[{\"queryType\":\"groupBy\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"virtualColumns\":[],\"filter\":{\"type\":\"not\",\"field\":{\"type\":\"selector\",\"dimension\":\"dim1\",\"value\":null,\"extractionFn\":null}},\"granularity\":{\"type\":\"all\"},\"dimensions\":[{\"type\":\"extraction\",\"dimension\":\"dim1\",\"outputName\":\"d0\",\"outputType\":\"STRING\",\"extractionFn\":{\"type\":\"substring\",\"index\":0,\"length\":1}}],\"aggregations\":[],\"postAggregations\":[],\"having\":null,\"limitSpec\":{\"type\":\"NoopLimitSpec\"},\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\"},\"descending\":false}], signature=[{d0:STRING}])\n";

    testQuery(
        "EXPLAIN PLAN FOR SELECT COUNT(*)\n"
        + "FROM (\n"
        + "  SELECT DISTINCT dim2\n"
        + "  FROM druid.foo\n"
        + "  WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "    SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo WHERE dim1 IS NOT NULL\n"
        + "  )\n"
        + ")",
        ImmutableList.of(),
        ImmutableList.of(new Object[]{explanation})
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWherePushDown() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 <> ''",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setDimFilter(not(selector("dim2", "", null)))
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
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{3L, 2L}
        ) :
        ImmutableList.of(
            new Object[]{5L, 3L}
        )
    );

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)\n"
        + "WHERE dim2 IS NOT NULL",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setDimFilter(not(selector("dim2", null, null)))
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
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{3L, 2L}
        ) :
        ImmutableList.of(
            new Object[]{4L, 3L}
        )
    );
  }

  @Test
  public void testExactCountDistinctUsingSubqueryWithWhereToOuterFilter() throws Exception
  {
    // Cannot vectorize topN operator.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 1)"
        + "WHERE cnt > 0",
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
                                    .context(QUERY_CONTEXT_DEFAULT)
                                    .build()
                            )
                        )
                        .setDimFilter(bound("a0", "0", null, true, false, null, StringComparators.NUMERIC))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new LongSumAggregatorFactory("_a0", "a0"),
                            new CountAggregatorFactory("_a1")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
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
  public void testCompareExactAndApproximateCountDistinctUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  COUNT(*) AS exact_count,\n"
        + "  COUNT(DISTINCT dim1) AS approx_count,\n"
        + "  (CAST(1 AS FLOAT) - COUNT(DISTINCT dim1) / COUNT(*)) * 100 AS error_pct\n"
        + "FROM (SELECT DISTINCT dim1 FROM druid.foo WHERE dim1 <> '')",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Filtration.eternity()))
                                            .setGranularity(Granularities.ALL)
                                            .setDimFilter(not(selector("dim1", "", null)))
                                            .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                                            .setContext(QUERY_CONTEXT_DEFAULT)
                                            .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("a0"),
                            new CardinalityAggregatorFactory(
                                "a1",
                                null,
                                dimensions(new DefaultDimensionSpec("d0", null)),
                                false,
                                true
                            )
                        ))
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                expressionPostAgg("p0", "((1 - (\"a1\" / \"a0\")) * 100)")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{5L, 5L, 0.0f}
        )
    );
  }

  @Test
  public void testHistogramUsingSubquery() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
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
                        .setDimensions(dimensions(new DefaultDimensionSpec("a0", "_d0")))
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("_a0")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"3", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"1", 2L},
            new Object[]{"2", 2L}
        )
    );
  }

  @Test
  public void testHistogramUsingSubqueryWithSort() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  CAST(thecnt AS VARCHAR),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS thecnt FROM druid.foo GROUP BY dim2)\n"
        + "GROUP BY CAST(thecnt AS VARCHAR) ORDER BY CAST(thecnt AS VARCHAR) LIMIT 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
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
                        .setDimensions(dimensions(new DefaultDimensionSpec("a0", "_d0")))
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("_a0")
                        ))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec(
                                    "_d0",
                                    OrderByColumnSpec.Direction.ASCENDING,
                                    StringComparators.LEXICOGRAPHIC
                                )),
                                2
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"1", 1L},
            new Object[]{"2", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"1", 2L},
            new Object[]{"2", 2L}
        )
    );
  }

  @Test
  public void testCountDistinctArithmetic() throws Exception
  {
    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(DISTINCT dim2),\n"
        + "  CAST(COUNT(DISTINCT dim2) AS FLOAT),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2),\n"
        + "  SUM(cnt) / COUNT(DISTINCT dim2) + 3,\n"
        + "  CAST(SUM(cnt) AS FLOAT) / CAST(COUNT(DISTINCT dim2) AS FLOAT) + 3\n"
        + "FROM druid.foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          new CardinalityAggregatorFactory(
                              "a1",
                              null,
                              dimensions(new DefaultDimensionSpec("dim2", null)),
                              false,
                              true
                          )
                      )
                  )
                  .postAggregators(
                      expressionPostAgg("p0", "CAST(\"a1\", 'DOUBLE')"),
                      expressionPostAgg("p1", "(\"a0\" / \"a1\")"),
                      expressionPostAgg("p2", "((\"a0\" / \"a1\") + 3)"),
                      expressionPostAgg("p3", "((CAST(\"a0\", 'DOUBLE') / CAST(\"a1\", 'DOUBLE')) + 3)")
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L, 3L, 3.0f, 2L, 5L, 5.0f}
        )
    );
  }

  @Test
  public void testCountDistinctOfSubstring() throws Exception
  {
    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT SUBSTRING(dim1, 1, 1)) FROM druid.foo WHERE dim1 <> ''",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(not(selector("dim1", "", null)))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(
                                  new ExtractionDimensionSpec(
                                      "dim1",
                                      null,
                                      new SubstringDimExtractionFn(0, 1)
                                  )
                              ),
                              false,
                              true
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{4L}
        )
    );
  }

  @Test
  public void testCountDistinctOfTrim() throws Exception
  {
    // Test a couple different syntax variants of TRIM.

    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(DISTINCT TRIM(BOTH ' ' FROM dim1)) FROM druid.foo WHERE TRIM(dim1) <> ''",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(expressionVirtualColumn("v0", "trim(\"dim1\",' ')", ValueType.STRING))
                  .filters(not(selector("v0", NullHandling.emptyToNullIfNeeded(""), null)))
                  .aggregators(
                      aggregators(
                          new CardinalityAggregatorFactory(
                              "a0",
                              null,
                              dimensions(new DefaultDimensionSpec("v0", "v0", ValueType.STRING)),
                              false,
                              true
                          )
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testSillyQuarters() throws Exception
  {
    // Like FLOOR(__time TO QUARTER) but silly.

    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT CAST((EXTRACT(MONTH FROM __time) - 1 ) / 3 + 1 AS INTEGER) AS quarter, COUNT(*)\n"
        + "FROM foo\n"
        + "GROUP BY CAST((EXTRACT(MONTH FROM __time) - 1 ) / 3 + 1 AS INTEGER)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "(((timestamp_extract(\"__time\",'MONTH','UTC') - 1) / 3) + 1)",
                            ValueType.LONG
                        ))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1, 6L}
        )
    );
  }

  @Test
  public void testRegexpExtract() throws Exception
  {
    // Cannot vectorize due to extractionFn in dimension spec.
    cannotVectorize();

    testQuery(
        "SELECT DISTINCT\n"
        + "  REGEXP_EXTRACT(dim1, '^.'),\n"
        + "  REGEXP_EXTRACT(dim1, '^(.)', 1)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_EXTRACT(dim1, '^(.)', 1) <> 'x'",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            not(selector(
                                "dim1",
                                "x",
                                new RegexDimExtractionFn("^(.)", 1, true, null)
                            ))
                        )
                        .setDimensions(
                            dimensions(
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d0",
                                    new RegexDimExtractionFn("^.", 0, true, null)
                                ),
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d1",
                                    new RegexDimExtractionFn("^(.)", 1, true, null)
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, NULL_STRING},
            new Object[]{"1", "1"},
            new Object[]{"2", "2"},
            new Object[]{"a", "a"},
            new Object[]{"d", "d"}
        )
    );
  }

  @Test
  public void testRegexpExtractFilterViaNotNullCheck() throws Exception
  {
    // Cannot vectorize due to extractionFn in dimension spec.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_EXTRACT(dim1, '^1') IS NOT NULL OR REGEXP_EXTRACT('Z' || dim1, '^Z2') IS NOT NULL",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "regexp_extract(concat('Z',\"dim1\"),'^Z2')", ValueType.STRING)
                  )
                  .filters(
                      or(
                          not(selector("dim1", null, new RegexDimExtractionFn("^1", 0, true, null))),
                          not(selector("v0", null, null))
                      )
                  )
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testRegexpLikeFilter() throws Exception
  {
    // Cannot vectorize due to usage of regex filter.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM foo\n"
        + "WHERE REGEXP_LIKE(dim1, '^1') OR REGEXP_LIKE('Z' || dim1, '^Z2')",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat('Z',\"dim1\")", ValueType.STRING)
                  )
                  .filters(
                      or(
                          new RegexDimFilter("dim1", "^1", null),
                          new RegexDimFilter("v0", "^Z2", null)
                      )
                  )
                  .aggregators(new CountAggregatorFactory("a0"))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testGroupBySortPushDown() throws Exception
  {
    testQuery(
        "SELECT dim2, dim1, SUM(cnt) FROM druid.foo GROUP BY dim2, dim1 ORDER BY dim1 LIMIT 4",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("dim1", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new LongSumAggregatorFactory("a0", "cnt")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                                ),
                                4
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "", 1L},
            new Object[]{"a", "1", 1L},
            new Object[]{NULL_STRING, "10.1", 1L},
            new Object[]{"", "2", 1L}
        )
    );
  }

  @Test
  public void testGroupByLimitPushDownWithHavingOnLong() throws Exception
  {
    testQuery(
        "SELECT dim1, dim2, SUM(cnt) AS thecnt "
        + "FROM druid.foo "
        + "group by dim1, dim2 "
        + "having SUM(cnt) = 1 "
        + "order by dim2 "
        + "limit 4",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new LongSumAggregatorFactory("a0", "cnt")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", OrderByColumnSpec.Direction.ASCENDING)
                                ),
                                4
                            )
                        )
                        .setHavingSpec(having(selector("a0", "1", null)))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", "", 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"abc", "", 1L},
            new Object[]{"", "a", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"10.1", null, 1L},
            new Object[]{"abc", null, 1L},
            new Object[]{"2", "", 1L},
            new Object[]{"", "a", 1L}
        )
    );
  }

  @Test
  public void testGroupByLimitPushdownExtraction() throws Exception
  {
    cannotVectorize();

    testQuery(
        "SELECT dim4, substring(dim5, 1, 1), count(*) FROM druid.numfoo WHERE dim4 = 'a' GROUP BY 1,2 LIMIT 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0"),
                                new ExtractionDimensionSpec("dim5", "_d1", new SubstringDimExtractionFn(0, 1))
                            )
                        )
                        .setVirtualColumns(expressionVirtualColumn("v0", "'a'", ValueType.STRING))
                        .setDimFilter(selector("dim4", "a", null))
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(),
                                2
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "a", 2L},
            new Object[]{"a", "b", 1L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloor() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) = TIMESTAMP '2000-01-01 00:00:00'\n"
        + "OR FLOOR(__time TO MONTH) = TIMESTAMP '2000-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/P2M")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testGroupAndFilterOnTimeFloorWithTimeZone() throws Exception
  {
    testQuery(
        "SELECT TIME_FLOOR(__time, 'P1M', NULL, 'America/Los_Angeles'), COUNT(*)\n"
        + "FROM druid.foo\n"
        + "WHERE\n"
        + "TIME_FLOOR(__time, 'P1M', NULL, 'America/Los_Angeles') = "
        + "  TIME_PARSE('2000-01-01 00:00:00', NULL, 'America/Los_Angeles')\n"
        + "OR TIME_FLOOR(__time, 'P1M', NULL, 'America/Los_Angeles') = "
        + "  TIME_PARSE('2000-02-01 00:00:00', NULL, 'America/Los_Angeles')\n"
        + "GROUP BY 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01T00-08:00/2000-03-01T00-08:00")))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                Calcites.jodaToCalciteTimestamp(
                    new DateTime("2000-01-01", DateTimes.inferTzFromString(LOS_ANGELES)),
                    DateTimeZone.UTC
                ),
                2L
            }
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampWithIntervalArithmetic() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "  __time >= CURRENT_TIMESTAMP + INTERVAL '01:02' HOUR TO MINUTE\n"
        + "  AND __time < TIMESTAMP '2003-02-02 01:00:00' - INTERVAL '1 1' DAY TO HOUR - INTERVAL '1-1' YEAR TO MONTH",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01T01:02/2002")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testSelectCurrentTimeAndDateLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_DATE + INTERVAL '1' DAY",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01T00Z", LOS_ANGELES), day("1999-12-31"), day("2000-01-01")}
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= CURRENT_TIMESTAMP + INTERVAL '1' DAY AND __time < TIMESTAMP '2002-01-01 00:00:00'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02T00Z/2002-01-01T08Z")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampOnView() throws Exception
  {
    testQuery(
        "SELECT * FROM bview",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02/2002")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnCurrentTimestampLosAngelesOnView() throws Exception
  {
    // Tests that query context still applies to view SQL; note the result is different from
    // "testFilterOnCurrentTimestampOnView" above.

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT * FROM bview",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-02T00Z/2002-01-01T08Z")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_LOS_ANGELES)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{5L}
        )
    );
  }

  @Test
  public void testFilterOnNotTimeFloor() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) <> TIMESTAMP '2001-01-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(
                      new Interval(DateTimes.MIN, DateTimes.of("2001-01-01")),
                      new Interval(DateTimes.of("2001-02-01"), DateTimes.MAX)
                  ))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorComparison() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(new Interval(DateTimes.MIN, DateTimes.of("2000-02-01"))))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorComparisonMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE\n"
        + "FLOOR(__time TO MONTH) < TIMESTAMP '2000-02-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(new Interval(DateTimes.MIN, DateTimes.of("2000-03-01"))))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeExtract() throws Exception
  {
    // Cannot vectorize due to expression filter.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(MONTH FROM __time) = 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'YEAR','UTC')", ValueType.LONG),
                      expressionVirtualColumn("v1", "timestamp_extract(\"__time\",'MONTH','UTC')", ValueType.LONG)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(
                      and(
                          selector("v0", "2000", null),
                          selector("v1", "1", null)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testFilterOnTimeExtractWithMultipleDays() throws Exception
  {
    // Cannot vectorize due to expression filters.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(DAY FROM __time) IN (2, 3, 5)",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "timestamp_extract(\"__time\",'YEAR','UTC')",
                          ValueType.LONG
                      ),
                      expressionVirtualColumn(
                          "v1",
                          "timestamp_extract(\"__time\",'DAY','UTC')",
                          ValueType.LONG
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(
                      and(
                          selector("v0", "2000", null),
                          in("v1", ImmutableList.of("2", "3", "5"), null)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2L}
        )
    );
  }

  @Test
  public void testFilterOnTimeExtractWithVariousTimeUnits() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) FROM druid.foo4\n"
        + "WHERE EXTRACT(YEAR FROM __time) = 2000\n"
        + "AND EXTRACT(MICROSECOND FROM __time) = 946723\n"
        + "AND EXTRACT(MILLISECOND FROM __time) = 695\n"
        + "AND EXTRACT(ISODOW FROM __time) = 6\n"
        + "AND EXTRACT(ISOYEAR FROM __time) = 2000\n"
        + "AND EXTRACT(DECADE FROM __time) = 200\n"
        + "AND EXTRACT(CENTURY FROM __time) = 20\n"
        + "AND EXTRACT(MILLENNIUM FROM __time) = 2\n",
        TIMESERIES_CONTEXT_DEFAULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE4)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'YEAR','UTC')", ValueType.LONG),
                      expressionVirtualColumn(
                          "v1",
                          "timestamp_extract(\"__time\",'MICROSECOND','UTC')",
                          ValueType.LONG
                      ),
                      expressionVirtualColumn(
                          "v2",
                          "timestamp_extract(\"__time\",'MILLISECOND','UTC')",
                          ValueType.LONG
                      ),
                      expressionVirtualColumn("v3", "timestamp_extract(\"__time\",'ISODOW','UTC')", ValueType.LONG),
                      expressionVirtualColumn("v4", "timestamp_extract(\"__time\",'ISOYEAR','UTC')", ValueType.LONG),
                      expressionVirtualColumn("v5", "timestamp_extract(\"__time\",'DECADE','UTC')", ValueType.LONG),
                      expressionVirtualColumn("v6", "timestamp_extract(\"__time\",'CENTURY','UTC')", ValueType.LONG),
                      expressionVirtualColumn("v7", "timestamp_extract(\"__time\",'MILLENNIUM','UTC')", ValueType.LONG)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(
                      and(
                          selector("v0", "2000", null),
                          selector("v1", "946723", null),
                          selector("v2", "695", null),
                          selector("v3", "6", null),
                          selector("v4", "2000", null),
                          selector("v5", "200", null),
                          selector("v6", "20", null),
                          selector("v7", "2", null)
                      )
                  )
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testFilterOnTimeFloorMisaligned() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo "
        + "WHERE floor(__time TO month) = TIMESTAMP '2000-01-01 00:00:01'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec())
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testGroupByFloor() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT floor(CAST(dim1 AS float)), COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "floor(CAST(\"dim1\", 'DOUBLE'))", ValueType.FLOAT)
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.FLOAT)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultFloatValue(), 3L},
            new Object[]{1.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{10.0f, 1L}
        )
    );
  }

  @Test
  public void testGroupByFloorWithOrderBy() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT floor(CAST(dim1 AS float)) AS fl, COUNT(*) FROM druid.foo GROUP BY floor(CAST(dim1 AS float)) ORDER BY fl DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "floor(CAST(\"dim1\", 'DOUBLE'))",
                                ValueType.FLOAT
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec(
                                    "v0",
                                    "d0",
                                    ValueType.FLOAT
                                )
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.DESCENDING,
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
            new Object[]{10.0f, 1L},
            new Object[]{2.0f, 1L},
            new Object[]{1.0f, 1L},
            new Object[]{NullHandling.defaultFloatValue(), 3L}
        )
    );
  }

  @Test
  public void testGroupByFloorTimeAndOneOtherDimensionWithOrderBy() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT floor(__time TO year), dim2, COUNT(*)"
        + " FROM druid.foo"
        + " GROUP BY floor(__time TO year), dim2"
        + " ORDER BY floor(__time TO year), dim2, COUNT(*) DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1Y',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.LONG),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d0",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "d1",
                                        OrderByColumnSpec.Direction.ASCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    ),
                                    new OrderByColumnSpec(
                                        "a0",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{timestamp("2000"), "", 2L},
            new Object[]{timestamp("2000"), "a", 1L},
            new Object[]{timestamp("2001"), "", 1L},
            new Object[]{timestamp("2001"), "a", 1L},
            new Object[]{timestamp("2001"), "abc", 1L}
        ) :
        ImmutableList.of(
            new Object[]{timestamp("2000"), null, 1L},
            new Object[]{timestamp("2000"), "", 1L},
            new Object[]{timestamp("2000"), "a", 1L},
            new Object[]{timestamp("2001"), null, 1L},
            new Object[]{timestamp("2001"), "a", 1L},
            new Object[]{timestamp("2001"), "abc", 1L}
        )
    );
  }

  @Test
  public void testGroupByStringLength() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT CHARACTER_LENGTH(dim1), COUNT(*) FROM druid.foo GROUP BY CHARACTER_LENGTH(dim1)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "strlen(\"dim1\")", ValueType.LONG))
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{0, 1L},
            new Object[]{1, 2L},
            new Object[]{3, 2L},
            new Object[]{4, 1L}
        )
    );
  }

  @Test
  public void testFilterAndGroupByLookup() throws Exception
  {
    // Cannot vectorize due to extraction dimension specs.
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
        null,
        "lookyloo",
        false,
        null,
        null,
        true
    );

    testQuery(
        "SELECT LOOKUP(dim1, 'lookyloo'), COUNT(*) FROM foo\n"
        + "WHERE LOOKUP(dim1, 'lookyloo') <> 'xxx'\n"
        + "GROUP BY LOOKUP(dim1, 'lookyloo')",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(
                            not(selector(
                                "dim1",
                                "xxx",
                                extractionFn
                            ))
                        )
                        .setDimensions(
                            dimensions(
                                new ExtractionDimensionSpec(
                                    "dim1",
                                    "d0",
                                    ValueType.STRING,
                                    extractionFn
                                )
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(
                                new CountAggregatorFactory("a0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NULL_STRING, 5L},
            new Object[]{"xabc", 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testFilterAndGroupByLookupUsingJoinOperatorWithValueFilterPushdownMatchesNothig(Map<String, Object> queryContext) throws Exception
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
  public void testFilterAndGroupByLookupUsingJoinOperatorWithNotFilter(Map<String, Object> queryContext) throws Exception
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
  public void testFilterAndGroupByLookupUsingPostAggregationJoinOperator(Map<String, Object> queryContext) throws Exception
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
  public void testInnerJoinTableLookupLookupWithFilterWithOuterLimitWithAllColumns(Map<String, Object> queryContext) throws Exception
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
  public void testInnerJoinTableLookupLookupWithFilterWithoutLimitWithAllColumns(Map<String, Object> queryContext) throws Exception
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
                                                                                        ),
                                                                                        new LookupDataSource("lookyloo"),
                                                                                        "__j0.",
                                                                                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("__j0.k")),
                                                                                        JoinType.INNER
                                                                                    ),
                                                                                    new LookupDataSource("lookyloo"),
                                                                                    "___j0.",
                                                                                    equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("___j0.k")),
                                                                                    JoinType.INNER
                                                                                ),
                                                                                new LookupDataSource("lookyloo"),
                                                                                "____j0.",
                                                                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("____j0.k")),
                                                                                JoinType.INNER
                                                                            ),
                                                                            new LookupDataSource("lookyloo"),
                                                                            "_____j0.",
                                                                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_____j0.k")),
                                                                            JoinType.INNER
                                                                        ),
                                                                        new LookupDataSource("lookyloo"),
                                                                        "______j0.",
                                                                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("______j0.k")),
                                                                        JoinType.INNER
                                                                    ),
                                                                    new LookupDataSource("lookyloo"),
                                                                    "_______j0.",
                                                                    equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_______j0.k")),
                                                                    JoinType.INNER
                                                                ),
                                                                new LookupDataSource("lookyloo"),
                                                                "________j0.",
                                                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("________j0.k")),
                                                                JoinType.INNER
                                                            ),
                                                            new LookupDataSource("lookyloo"),
                                                            "_________j0.",
                                                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_________j0.k")),
                                                            JoinType.INNER
                                                        ),
                                                        new LookupDataSource("lookyloo"),
                                                        "__________j0.",
                                                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("__________j0.k")),
                                                        JoinType.INNER
                                                    ),
                                                    new LookupDataSource("lookyloo"),
                                                    "___________j0.",
                                                    equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("___________j0.k")),
                                                    JoinType.INNER
                                                ),
                                                new LookupDataSource("lookyloo"),
                                                "____________j0.",
                                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("____________j0.k")),
                                                JoinType.INNER
                                            ),
                                            new LookupDataSource("lookyloo"),
                                            "_____________j0.",
                                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_____________j0.k")),
                                            JoinType.INNER
                                        ),
                                        new LookupDataSource("lookyloo"),
                                        "______________j0.",
                                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("______________j0.k")),
                                        JoinType.INNER
                                    ),
                                    new LookupDataSource("lookyloo"),
                                    "_______________j0.",
                                    equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_______________j0.k")),
                                    JoinType.INNER
                                ),
                                new LookupDataSource("lookyloo"),
                                "________________j0.",
                                equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("________________j0.k")),
                                JoinType.INNER
                            ),
                            new LookupDataSource("lookyloo"),
                            "_________________j0.",
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("_________________j0.k")),
                            JoinType.INNER
                        ),
                        new LookupDataSource("lookyloo"),
                        "__________________j0.",
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("__________________j0.k")),
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
                                .setAggregatorSpecs(new StringLastAggregatorFactory("a0", "v", 10))
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
                .virtualColumns(expressionVirtualColumn("v0", "substring(\"j0.v\", 0, 3)", ValueType.STRING))
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
                                              ValueType.FLOAT
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInnerJoinTwoLookupsToTableUsingNumericColumnInReverse(Map<String, Object> queryContext) throws Exception
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
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
                                ImmutableList.of(new OrderByColumnSpec("d1", Direction.ASCENDING)),
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
                                ImmutableList.of(new OrderByColumnSpec("d1", Direction.ASCENDING)),
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
                                            expressionVirtualColumn("v0", "null", ValueType.STRING)
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
                    expressionVirtualColumn("v0", "null", ValueType.STRING)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
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
                                          expressionVirtualColumn("v0", "CAST(\"k\", 'LONG')", ValueType.LONG)
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
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
                                    expressionVirtualColumn("v0", "CAST(\"k\", 'DOUBLE')", ValueType.FLOAT)
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
                                    expressionVirtualColumn("v0", "CAST(\"k\", 'DOUBLE')", ValueType.FLOAT)
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
                                    expressionVirtualColumn("v0", "substring(\"k\", 0, 2)", ValueType.STRING)
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
  public void testCountDistinctOfLookup() throws Exception
  {
    // Cannot vectorize due to "cardinality" aggregator.
    cannotVectorize();

    final RegisteredLookupExtractionFn extractionFn = new RegisteredLookupExtractionFn(
        null,
        "lookyloo",
        false,
        null,
        null,
        true
    );

    testQuery(
        "SELECT COUNT(DISTINCT LOOKUP(dim1, 'lookyloo')) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          ImmutableList.of(new ExtractionDimensionSpec("dim1", null, extractionFn)),
                          false,
                          true
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
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
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? 2L : 1L}
        )
    );
  }

  @Test
  public void testSelectStarFromLookup() throws Exception
  {
    testQuery(
        "SELECT * FROM lookup.lookyloo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(new LookupDataSource("lookyloo"))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("k", "v")
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a", "xa"},
            new Object[]{"abc", "xabc"},
            new Object[]{"nosuchkey", "mysteryvalue"},
            new Object[]{"6", "x6"}
        )
    );
  }

  @Test
  public void testGroupByExpressionFromLookup() throws Exception
  {
    // Cannot vectorize direct queries on lookup tables.
    cannotVectorize();

    testQuery(
        "SELECT SUBSTRING(v, 1, 1), COUNT(*) FROM lookup.lookyloo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new LookupDataSource("lookyloo"))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new ExtractionDimensionSpec(
                                    "v",
                                    "d0",
                                    new SubstringDimExtractionFn(0, 1)
                                )
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"m", 1L},
            new Object[]{"x", 3L}
        )
    );
  }

  @Test
  public void testTimeseries() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, timestamp("2000-01-01")},
            new Object[]{3L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testFilteredTimeAggregators() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:00'\n"
        + "                    AND __time <  TIMESTAMP '2000-02-01 00:00:00'),\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2000-01-01 00:00:01'\n"
        + "                    AND __time <  TIMESTAMP '2000-02-01 00:00:00'),\n"
        + "  SUM(cnt) FILTER(WHERE __time >= TIMESTAMP '2001-01-01 00:00:00'\n"
        + "                    AND __time <  TIMESTAMP '2001-02-01 00:00:00')\n"
        + "FROM foo\n"
        + "WHERE\n"
        + "  __time >= TIMESTAMP '2000-01-01 00:00:00'\n"
        + "  AND __time < TIMESTAMP '2001-02-01 00:00:00'",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2001-02-01")))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a0", "cnt"),
                          bound(
                              "__time",
                              null,
                              String.valueOf(timestamp("2000-02-01")),
                              false,
                              true,
                              null,
                              StringComparators.NUMERIC
                          )
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a1", "cnt"),
                          bound(
                              "__time",
                              String.valueOf(timestamp("2000-01-01T00:00:01")),
                              String.valueOf(timestamp("2000-02-01")),
                              false,
                              true,
                              null,
                              StringComparators.NUMERIC
                          )
                      ),
                      new FilteredAggregatorFactory(
                          new LongSumAggregatorFactory("a2", "cnt"),
                          bound(
                              "__time",
                              String.valueOf(timestamp("2001-01-01")),
                              String.valueOf(timestamp("2001-02-01")),
                              false,
                              true,
                              null,
                              StringComparators.NUMERIC
                          )
                      )
                  ))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, 2L, 3L}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesViaQueryContext() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT FLOOR(__time TO MONTH) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_LOS_ANGELES, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, timestamp("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2001-01-01", LOS_ANGELES)}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesViaPlannerConfig() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_LOS_ANGELES,
        QUERY_CONTEXT_DEFAULT,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT\n"
        + "    FLOOR(__time TO MONTH) AS gran,\n"
        + "    cnt\n"
        + "  FROM druid.foo\n"
        + "  WHERE __time >= TIME_PARSE('1999-12-01 00:00:00') AND __time < TIME_PARSE('2002-01-01 00:00:00')\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("1999-12-01T00-08:00/2002-01-01T00-08:00")))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, timestamp("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2001-01-01", LOS_ANGELES)}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloor() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, timestamp("2000-01-01")},
            new Object[]{3L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithTimeShift() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(TIME_SHIFT(__time, 'P1D', -1), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(timestamp_shift(\"__time\",'P1D',-1,'UTC'),'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
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
            new Object[]{1L, timestamp("1999-12-01")},
            new Object[]{2L, timestamp("2000-01-01")},
            new Object[]{1L, timestamp("2000-12-01")},
            new Object[]{2L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithTimestampAdd() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor((\"__time\" + -86400000),'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
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
            new Object[]{1L, timestamp("1999-12-01")},
            new Object[]{2L, timestamp("2000-01-01")},
            new Object[]{1L, timestamp("2000-12-01")},
            new Object[]{2L, timestamp("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingTimeFloorWithOrigin() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M', TIMESTAMP '1970-01-01 01:02:03') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(
                      new PeriodGranularity(
                          Period.months(1),
                          DateTimes.of("1970-01-01T01:02:03"),
                          DateTimeZone.UTC
                      )
                  )
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01T01:02:03")},
            new Object[]{2L, timestamp("2000-01-01T01:02:03")},
            new Object[]{1L, timestamp("2000-12-01T01:02:03")},
            new Object[]{2L, timestamp("2001-01-01T01:02:03")}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesUsingTimeFloorConnectionUtc() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M', CAST(NULL AS TIMESTAMP), 'America/Los_Angeles') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01T08")},
            new Object[]{2L, timestamp("2000-01-01T08")},
            new Object[]{1L, timestamp("2000-12-01T08")},
            new Object[]{2L, timestamp("2001-01-01T08")}
        )
    );
  }

  @Test
  public void testTimeseriesLosAngelesUsingTimeFloorConnectionLosAngeles() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT TIME_FLOOR(__time, 'P1M') AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(1), null, DateTimes.inferTzFromString(LOS_ANGELES)))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_LOS_ANGELES, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, timestamp("1999-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2000-01-01", LOS_ANGELES)},
            new Object[]{1L, timestamp("2000-12-01", LOS_ANGELES)},
            new Object[]{2L, timestamp("2001-01-01", LOS_ANGELES)}
        )
    );
  }

  @Test
  public void testTimeseriesDontSkipEmptyBuckets() throws Exception
  {
    // Tests that query context parameters are passed through to the underlying query engine.
    Long defaultVal = NullHandling.replaceWithDefault() ? 0L : null;
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT floor(__time TO HOUR) AS gran, cnt FROM druid.foo\n"
        + "  WHERE __time >= TIMESTAMP '2000-01-01 00:00:00' AND __time < TIMESTAMP '2000-01-02 00:00:00'\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000/2000-01-02")))
                  .granularity(new PeriodGranularity(Period.hours(1), null, DateTimeZone.UTC))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS, "d0"))
                  .build()
        ),
        ImmutableList.<Object[]>builder()
            .add(new Object[]{1L, timestamp("2000-01-01")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T01")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T02")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T03")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T04")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T05")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T06")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T07")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T08")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T09")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T10")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T11")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T12")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T13")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T14")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T15")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T16")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T17")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T18")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T19")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T20")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T21")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T22")})
            .add(new Object[]{defaultVal, timestamp("2000-01-01T23")})
            .build()
    );
  }

  @Test
  public void testTimeseriesUsingCastAsDate() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), dt FROM (\n"
        + "  SELECT CAST(__time AS DATE) AS dt,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY dt\n"
        + "ORDER BY dt",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, day("2000-01-01")},
            new Object[]{1L, day("2000-01-02")},
            new Object[]{1L, day("2000-01-03")},
            new Object[]{1L, day("2001-01-01")},
            new Object[]{1L, day("2001-01-02")},
            new Object[]{1L, day("2001-01-03")}
        )
    );
  }

  @Test
  public void testTimeseriesUsingFloorPlusCastAsDate() throws Exception
  {
    testQuery(
        "SELECT SUM(cnt), dt FROM (\n"
        + "  SELECT CAST(FLOOR(__time TO QUARTER) AS DATE) AS dt,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY dt\n"
        + "ORDER BY dt",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(new PeriodGranularity(Period.months(3), null, DateTimeZone.UTC))
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, day("2000-01-01")},
            new Object[]{3L, day("2001-01-01")}
        )
    );
  }

  @Test
  public void testTimeseriesDescending() throws Exception
  {
    // Cannot vectorize due to descending order.
    cannotVectorize();

    testQuery(
        "SELECT gran, SUM(cnt) FROM (\n"
        + "  SELECT floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran DESC",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .descending(true)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-01"), 3L},
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByExtractYear() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  EXTRACT(YEAR FROM __time) AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM __time)\n"
        + "ORDER BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_extract(\"__time\",'YEAR','UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
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
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
    );
  }

  @Test
  public void testGroupByFormatYearAndMonth() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "  TIME_FORMAt(__time, 'yyyy MM') AS \"year\",\n"
        + "  SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY TIME_FORMAt(__time, 'yyyy MM')\n"
        + "ORDER BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_format(\"__time\",'yyyy MM','UTC')",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.STRING)))
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
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"2000 01", 3L},
            new Object[]{"2001 01", 3L}
        )
    );
  }

  @Test
  public void testGroupByExtractFloorTime() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_extract(timestamp_floor(\"__time\",'P1Y',null,'UTC'),'YEAR','UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{2000L, 3L},
            new Object[]{2001L, 3L}
        )
    );
  }

  @Test
  public void testGroupByExtractFloorTimeLosAngeles() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_LOS_ANGELES,
        "SELECT\n"
        + "EXTRACT(YEAR FROM FLOOR(__time TO YEAR)) AS \"year\", SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY EXTRACT(YEAR FROM FLOOR(__time TO YEAR))",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_extract(timestamp_floor(\"__time\",'P1Y',null,'America/Los_Angeles'),'YEAR','America/Los_Angeles')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_LOS_ANGELES)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1999L, 1L},
            new Object[]{2000L, 3L},
            new Object[]{2001L, 2L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimitNoTopN() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_NO_TOPN,
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .limit(1)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithLimit() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "LIMIT 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .limit(1)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testTimeseriesWithOrderByAndLimit() throws Exception
  {
    testQuery(
        "SELECT gran, SUM(cnt)\n"
        + "FROM (\n"
        + "  SELECT floor(__time TO month) AS gran, cnt\n"
        + "  FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran\n"
        + "LIMIT 1",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .limit(1)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupByTimeAndOtherDimension() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY dim2, gran\n"
        + "ORDER BY dim2, gran",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d0", OrderByColumnSpec.Direction.ASCENDING),
                                    new OrderByColumnSpec(
                                        "d1",
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
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, timestamp("2000-01-01"), 1L},
            new Object[]{null, timestamp("2001-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupingSets() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, gran), (dim2), (gran), () )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupingSetsWithNumericDimension() throws Exception
  {
    testQuery(
        "SELECT cnt, COUNT(*)\n"
        + "FROM foo\n"
        + "GROUP BY GROUPING SETS ( (cnt), () )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6L},
            new Object[]{null, 6L}
        )
    );
  }

  @Test
  public void testGroupByRollup() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY ROLLUP (dim2, gran)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupByRollupDifferentOrder() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    // Like "testGroupByRollup", but the ROLLUP exprs are in the reverse order.
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY ROLLUP (gran, dim2)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.LONG),
                                new DefaultDimensionSpec("v1", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupByCube() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY CUBE (dim2, gran)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d1"),
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupingSetsWithDummyDimension() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (dim2, 'dummy', gran), (dim2), (gran), ('dummy') )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v2",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v2", "d2", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0", "d2"),
                                ImmutableList.of("d0"),
                                ImmutableList.of(),
                                ImmutableList.of("d2")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, null, 6L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupingSetsNoSuperset() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    // Note: the grouping sets are reordered in the output of this query, but this is allowed.
    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", null, 3L},
            new Object[]{"a", null, 2L},
            new Object[]{"abc", null, 1L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupingSetsWithOrderByDimension() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )\n"
        + "ORDER BY gran, dim2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "d1",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    ),
                                    new OrderByColumnSpec(
                                        "d0",
                                        Direction.DESCENDING,
                                        StringComparators.LEXICOGRAPHIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", null, 1L},
            new Object[]{"a", null, 2L},
            new Object[]{"", null, 3L},
            new Object[]{NULL_STRING, null, 6L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L}
        )
    );
  }

  @Test
  public void testGroupingSetsWithOrderByAggregator() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )\n"
        + "ORDER BY SUM(cnt)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a0",
                                        Direction.ASCENDING,
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
            new Object[]{"abc", null, 1L},
            new Object[]{"a", null, 2L},
            new Object[]{"", null, 3L},
            new Object[]{NULL_STRING, timestamp("2000-01-01"), 3L},
            new Object[]{NULL_STRING, timestamp("2001-01-01"), 3L},
            new Object[]{NULL_STRING, null, 6L}
        )
    );
  }

  @Test
  public void testGroupingSetsWithOrderByAggregatorWithLimit() throws Exception
  {
    // Cannot vectorize due to virtual columns.
    cannotVectorize();

    testQuery(
        "SELECT dim2, gran, SUM(cnt)\n"
        + "FROM (SELECT FLOOR(__time TO MONTH) AS gran, COALESCE(dim2, '') dim2, cnt FROM druid.foo) AS x\n"
        + "GROUP BY GROUPING SETS ( (), (dim2), (gran) )\n"
        + "ORDER BY SUM(cnt)\n"
        + "LIMIT 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setSubtotalsSpec(
                            ImmutableList.of(
                                ImmutableList.of("d0"),
                                ImmutableList.of("d1"),
                                ImmutableList.of()
                            )
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a0",
                                        Direction.ASCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                1
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", null, 1L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testUsingSubqueryAsPartOfAndFilter(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

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
                                              .context(TIMESERIES_CONTEXT_DEFAULT)
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
                                                .setVirtualColumns(expressionVirtualColumn("v0", "1", ValueType.LONG))
                                                .setDimFilter(new LikeDimFilter("dim1", "%bc", null, null))
                                                .setDimensions(
                                                    dimensions(
                                                        new DefaultDimensionSpec("dim1", "d0"),
                                                        new DefaultDimensionSpec("v0", "d1", ValueType.LONG)
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
  public void testTimeExtractWithTooFewArguments() throws Exception
  {
    // Regression test for https://github.com/apache/druid/pull/7710.
    expectedException.expect(ValidationException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(CalciteContextException.class));
    expectedException.expectCause(
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString(
                "Invalid number of arguments to function 'TIME_EXTRACT'. Was expecting 2 arguments"
            )
        )
    );
    testQuery("SELECT TIME_EXTRACT(__time) FROM druid.foo", ImmutableList.of(), ImmutableList.of());
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
                                        .intervals(querySegmentSpec(Intervals.of("2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                                        .columns("dim1")
                                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .context(queryContext)
                                        .build()
                                ),
                                new QueryDataSource(
                                    newScanQueryBuilder()
                                        .dataSource(CalciteTests.DATASOURCE1)
                                        .intervals(querySegmentSpec(Intervals.of("2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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
                        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ValueType.STRING))
                        .build()
                )
                .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                .setGranularity(Granularities.ALL)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .build()
        ),
        ImmutableList.of(new Object[] {1L})
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
                        .intervals(querySegmentSpec(Intervals.of("2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(CalciteTests.DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of("2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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
        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ValueType.STRING))
        .build();

    QueryLifecycleFactory qlf = CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate);
    QueryLifecycle ql = qlf.factorize();
    Sequence seq = ql.runSimple(
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        null
    );
    List<Object> results = seq.toList();
    Assert.assertEquals(
        ImmutableList.of(ResultRow.of("def")),
        results
    );
  }

  @Test
  public void testUsingSubqueryAsFilterOnTwoColumns() throws Exception
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
                                        .setDimFilter(selector("dim2", "abc", null))
                                        .setDimensions(dimensions(
                                            new DefaultDimensionSpec("dim1", "d0"),
                                            new DefaultDimensionSpec("dim2", "d1")
                                        ))
                                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(expressionPostAgg("p0", "'abc'"))
                                        )
                                        .setHavingSpec(having(selector("a0", "1", null)))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        ),
                        "j0.",
                        StringUtils.format(
                            "(%s && %s)",
                            equalsCondition(DruidExpression.fromColumn("dim1"), DruidExpression.fromColumn("j0.d0")),
                            equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.p0"))
                        ),
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "'abc'", ValueType.STRING))
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
  public void testUsingSubqueryAsFilterWithInnerSort() throws Exception
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
                        equalsCondition(DruidExpression.fromColumn("dim2"), DruidExpression.fromColumn("j0.d0")),
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
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ValueType.LONG)
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
                    expressionVirtualColumn("v0", "timestamp_extract(\"__time\",'MONTH','UTC')", ValueType.LONG)
                )
                .setDimFilter(not(selector("dim1", "", null)))
                .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.LONG)))
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setAggregatorSpecs(
                    aggregators(
                        new CardinalityAggregatorFactory(
                            "a0",
                            null,
                            ImmutableList.of(
                                new DefaultDimensionSpec("dim1", "dim1", ValueType.STRING)
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

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testInAggregationSubquery(Map<String, Object> queryContext) throws Exception
  {
    // Cannot vectorize JOIN operator.
    cannotVectorize();

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
                                          .context(TIMESERIES_CONTEXT_DEFAULT)
                                          .build()
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
                        .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ValueType.LONG)))
                        .setContext(queryContext)
                        .build()
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
                                                      .context(TIMESERIES_CONTEXT_DEFAULT)
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
                                          .context(TIMESERIES_CONTEXT_DEFAULT)
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
                        .setDimensions(dimensions(new DefaultDimensionSpec("__time", "d0", ValueType.LONG)))
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
  public void testUsingSubqueryWithLimit() throws Exception
  {
    // Cannot vectorize scan query.
    cannotVectorize();

    testQuery(
        "SELECT COUNT(*) AS cnt FROM ( SELECT * FROM druid.foo LIMIT 10 ) tmpA",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                                .limit(10)
                                .context(QUERY_CONTEXT_DEFAULT)
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
  public void testUsingSubqueryWithoutLimit() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) AS cnt FROM ( SELECT * FROM druid.foo ) tmpA",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testUnicodeFilterAndGroupBy() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  dim2,\n"
        + "  COUNT(*)\n"
        + "FROM foo2\n"
        + "WHERE\n"
        + "  dim1 LIKE U&'\u05D3\\05E8%'\n" // First char is actually in the string; second is a SQL U& escape
        + "  OR dim1 = 'друид'\n"
        + "GROUP BY dim1, dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE2)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(or(
                            new LikeDimFilter("dim1", "דר%", null, null),
                            new SelectorDimFilter("dim1", "друид", null)
                        ))
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("dim1", "d0"),
                            new DefaultDimensionSpec("dim2", "d1")
                        ))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"друид", "ru", 1L},
            new Object[]{"דרואיד", "he", 1L}
        )
    );
  }

  @Test
  public void testProjectAfterSort() throws Exception
  {
    testQuery(
        "select dim1 from (select dim1, dim2, count(*) cnt from druid.foo group by dim1, dim2 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testProjectAfterSort2() throws Exception
  {
    testQuery(
        "select s / cnt, dim1, dim2, s from (select dim1, dim2, count(*) cnt, sum(m2) s from druid.foo group by dim1, dim2 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0"),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(
                            aggregators(new CountAggregatorFactory("a0"), new DoubleSumAggregatorFactory("a1", "m2"))
                        )
                        .setPostAggregatorSpecs(Collections.singletonList(expressionPostAgg(
                            "p0",
                            "(\"a1\" / \"a0\")"
                        )))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, "", "a", 1.0},
            new Object[]{4.0, "1", "a", 4.0},
            new Object[]{2.0, "10.1", NullHandling.defaultStringValue(), 2.0},
            new Object[]{3.0, "2", "", 3.0},
            new Object[]{6.0, "abc", NullHandling.defaultStringValue(), 6.0},
            new Object[]{5.0, "def", "abc", 5.0}
        )
    );
  }

  @Test
  @Ignore("In Calcite 1.17, this test worked, but after upgrading to Calcite 1.21, this query fails with:"
          + " org.apache.calcite.sql.validate.SqlValidatorException: Column 'dim1' is ambiguous")
  public void testProjectAfterSort3() throws Exception
  {
    testQuery(
        "select dim1 from (select dim1, dim1, count(*) cnt from druid.foo group by dim1, dim1 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                Collections.singletonList(
                                    new OrderByColumnSpec("a0", Direction.ASCENDING, StringComparators.NUMERIC)
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }


  @Test
  public void testProjectAfterSort3WithoutAmbiguity() throws Exception
  {
    // This query is equivalent to the one in testProjectAfterSort3 but renames the second grouping column
    // to avoid the ambiguous name exception. The inner sort is also optimized out in Calcite 1.21.
    testQuery(
        "select copydim1 from (select dim1, dim1 AS copydim1, count(*) cnt from druid.foo group by dim1, dim1 order by cnt)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "d0")
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSortProjectAfterNestedGroupBy() throws Exception
  {
    testQuery(
        "SELECT "
        + "  cnt "
        + "FROM ("
        + "  SELECT "
        + "    __time, "
        + "    dim1, "
        + "    COUNT(m2) AS cnt "
        + "  FROM ("
        + "    SELECT "
        + "        __time, "
        + "        m2, "
        + "        dim1 "
        + "    FROM druid.foo "
        + "    GROUP BY __time, m2, dim1 "
        + "  ) "
        + "  GROUP BY __time, dim1 "
        + "  ORDER BY cnt"
        + ")",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE1)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(dimensions(
                                            new DefaultDimensionSpec("__time", "d0", ValueType.LONG),
                                            new DefaultDimensionSpec("m2", "d1", ValueType.DOUBLE),
                                            new DefaultDimensionSpec("dim1", "d2")
                                        ))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(
                            new DefaultDimensionSpec("d0", "_d0", ValueType.LONG),
                            new DefaultDimensionSpec("d2", "_d1", ValueType.STRING)
                        ))
                        .setAggregatorSpecs(
                            aggregators(
                                useDefault
                                ? new CountAggregatorFactory("a0")
                                : new FilteredAggregatorFactory(
                                    new CountAggregatorFactory("a0"),
                                    not(selector("d1", null, null))
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L},
            new Object[]{1L}
        )
    );
  }

  @Test
  public void testPostAggWithTimeseries() throws Exception
  {
    // Cannot vectorize due to descending order.
    cannotVectorize();

    testQuery(
        "SELECT "
        + "  FLOOR(__time TO YEAR), "
        + "  SUM(m1), "
        + "  SUM(m1) + SUM(m2) "
        + "FROM "
        + "  druid.foo "
        + "WHERE "
        + "  dim2 = 'a' "
        + "GROUP BY FLOOR(__time TO YEAR) "
        + "ORDER BY FLOOR(__time TO YEAR) desc",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .filters(selector("dim2", "a", null))
                  .granularity(Granularities.YEAR)
                  .aggregators(
                      aggregators(
                          new DoubleSumAggregatorFactory("a0", "m1"),
                          new DoubleSumAggregatorFactory("a1", "m2")
                      )
                  )
                  .postAggregators(
                      expressionPostAgg("p0", "(\"a0\" + \"a1\")")
                  )
                  .descending(true)
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{978307200000L, 4.0, 8.0},
            new Object[]{946684800000L, 1.0, 2.0}
        )
    );
  }

  @Test
  public void testPostAggWithTopN() throws Exception
  {
    testQuery(
        "SELECT "
        + "  AVG(m2), "
        + "  SUM(m1) + SUM(m2) "
        + "FROM "
        + "  druid.foo "
        + "WHERE "
        + "  dim2 = 'a' "
        + "GROUP BY m1 "
        + "ORDER BY m1 "
        + "LIMIT 5",
        Collections.singletonList(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("m1", "d0", ValueType.FLOAT))
                .filters("dim2", "a")
                .aggregators(
                    useDefault
                    ? aggregators(
                      new DoubleSumAggregatorFactory("a0:sum", "m2"),
                      new CountAggregatorFactory("a0:count"),
                      new DoubleSumAggregatorFactory("a1", "m1"),
                      new DoubleSumAggregatorFactory("a2", "m2")
                    )
                    : aggregators(
                      new DoubleSumAggregatorFactory("a0:sum", "m2"),
                      new FilteredAggregatorFactory(
                          new CountAggregatorFactory("a0:count"),
                          not(selector("m2", null, null))
                      ),
                      new DoubleSumAggregatorFactory("a1", "m1"),
                      new DoubleSumAggregatorFactory("a2", "m2")
                    )
                )
                .postAggregators(
                    new ArithmeticPostAggregator(
                        "a0",
                        "quotient",
                        ImmutableList.of(
                            new FieldAccessPostAggregator(null, "a0:sum"),
                            new FieldAccessPostAggregator(null, "a0:count")
                        )
                    ),
                    expressionPostAgg("p0", "(\"a1\" + \"a2\")")
                )
                .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
                .threshold(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{1.0, 2.0},
            new Object[]{4.0, 8.0}
        )
    );
  }

  @Test
  public void testConcat() throws Exception
  {
    testQuery(
        "SELECT CONCAT(dim1, '-', dim1, '_', dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",'-',\"dim1\",'_',\"dim1\")",
                    ValueType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"-_"},
            new Object[]{"10.1-10.1_10.1"},
            new Object[]{"2-2_2"},
            new Object[]{"1-1_1"},
            new Object[]{"def-def_def"},
            new Object[]{"abc-abc_abc"}
        )
    );

    testQuery(
        "SELECT CONCAt(dim1, CONCAt(dim2,'x'), m2, 9999, dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",concat(\"dim2\",'x'),\"m2\",9999,\"dim1\")",
                    ValueType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"ax1.09999"},
            new Object[]{NullHandling.sqlCompatible() ? null : "10.1x2.0999910.1"}, // dim2 is null
            new Object[]{"2x3.099992"},
            new Object[]{"1ax4.099991"},
            new Object[]{"defabcx5.09999def"},
            new Object[]{NullHandling.sqlCompatible() ? null : "abcx6.09999abc"} // dim2 is null
        )
    );
  }

  @Test
  public void testTextcat() throws Exception
  {
    testQuery(
        "SELECT textcat(dim1, dim1) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim1\",\"dim1\")", ValueType.STRING))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"10.110.1"},
            new Object[]{"22"},
            new Object[]{"11"},
            new Object[]{"defdef"},
            new Object[]{"abcabc"}
        )
    );

    testQuery(
        "SELECT textcat(dim1, CAST(m2 as VARCHAR)) as dimX FROM foo",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "concat(\"dim1\",CAST(\"m2\", 'STRING'))",
                    ValueType.STRING
                ))
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"1.0"},
            new Object[]{"10.12.0"},
            new Object[]{"23.0"},
            new Object[]{"14.0"},
            new Object[]{"def5.0"},
            new Object[]{"abc6.0"}
        )
    );
  }

  @Test
  public void testRequireTimeConditionPositive() throws Exception
  {
    // simple timeseries
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT __time as t, floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "WHERE t >= '2000-01-01' and t < '2002-01-01'"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Intervals.of("2000-01-01/2002-01-01")))
                  .granularity(Granularities.MONTH)
                  .aggregators(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                  .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_DEFAULT, "d0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L, timestamp("2000-01-01")},
            new Object[]{3L, timestamp("2001-01-01")}
        )
    );

    // nested groupby only requires time condition for inner most query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo WHERE __time >= '2000-01-01' GROUP BY dim2)",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                GroupByQuery.builder()
                                            .setDataSource(CalciteTests.DATASOURCE1)
                                            .setInterval(querySegmentSpec(Intervals.utc(
                                                DateTimes.of("2000-01-01").getMillis(),
                                                JodaUtils.MAX_INSTANT
                                            )))
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
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{6L, 3L}
        ) :
        ImmutableList.of(
            new Object[]{6L, 4L}
        )
    );

    // Cannot vectorize next test due to "cardinality" aggregator.
    cannotVectorize();

    // semi-join requires time condition on both left and right query
    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE __time >= '2000-01-01' AND SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo\n"
        + "  WHERE dim1 <> '' AND __time >= '2000-01-01'\n"
        + ")",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new QueryDataSource(
                              GroupByQuery.builder()
                                          .setDataSource(CalciteTests.DATASOURCE1)
                                          .setInterval(
                                              querySegmentSpec(
                                                  Intervals.utc(
                                                      DateTimes.of("2000-01-01").getMillis(),
                                                      JodaUtils.MAX_INSTANT
                                                  )
                                              )
                                          )
                                          .setDimFilter(
                                              not(selector("dim1", NullHandling.sqlCompatible() ? "" : null, null))
                                          )
                                          .setGranularity(Granularities.ALL)
                                          .setDimensions(
                                              new ExtractionDimensionSpec(
                                                  "dim1",
                                                  "d0",
                                                  ValueType.STRING,
                                                  new SubstringDimExtractionFn(0, 1)
                                              )
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
                  .intervals(
                      querySegmentSpec(
                          Intervals.utc(
                              DateTimes.of("2000-01-01").getMillis(),
                              JodaUtils.MAX_INSTANT
                          )
                      )
                  )
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testRequireTimeConditionSimpleQueryNegative() throws Exception
  {
    expectedException.expect(CannotBuildQueryException.class);
    expectedException.expectMessage("__time column");

    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT SUM(cnt), gran FROM (\n"
        + "  SELECT __time as t, floor(__time TO month) AS gran,\n"
        + "  cnt FROM druid.foo\n"
        + ") AS x\n"
        + "GROUP BY gran\n"
        + "ORDER BY gran",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testRequireTimeConditionSubQueryNegative() throws Exception
  {
    expectedException.expect(CannotBuildQueryException.class);
    expectedException.expectMessage("__time column");

    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT\n"
        + "  SUM(cnt),\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2)",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testRequireTimeConditionSemiJoinNegative() throws Exception
  {
    expectedException.expect(CannotBuildQueryException.class);
    expectedException.expectMessage("__time column");

    testQuery(
        PLANNER_CONFIG_REQUIRE_TIME_CONDITION,
        "SELECT COUNT(*) FROM druid.foo\n"
        + "WHERE SUBSTRING(dim2, 1, 1) IN (\n"
        + "  SELECT SUBSTRING(dim1, 1, 1) FROM druid.foo\n"
        + "  WHERE dim1 <> '' AND __time >= '2000-01-01'\n"
        + ")",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterFloatDimension() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM numfoo WHERE f1 = 0.1 LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(selector("f1", "0.1", null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testFilterDoubleDimension() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM numfoo WHERE d1 = 1.7 LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(selector("d1", "1.7", null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1"}
        )
    );
  }

  @Test
  public void testFilterLongDimension() throws Exception
  {
    testQuery(
        "SELECT dim1 FROM numfoo WHERE l1 = 7 LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(selector("l1", "7", null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{""}
        )
    );
  }

  @Test
  public void testTrigonometricFunction() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        "SELECT exp(count(*)) + 10, sin(pi / 6), cos(pi / 6), tan(pi / 6), cot(pi / 6)," +
        "asin(exp(count(*)) / 2), acos(exp(count(*)) / 2), atan(exp(count(*)) / 2), atan2(exp(count(*)), 1) " +
        "FROM druid.foo WHERE  dim2 = 0",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(bound("dim2", "0", "0", false, false, null, StringComparators.NUMERIC))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0")
                               ))
                               // after upgrading to Calcite 1.21, expressions like sin(pi/6) that only reference
                               // literals are optimized into literals
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)"),
                                   expressionPostAgg("p1", "0.49999999999999994"),
                                   expressionPostAgg("p2", "0.8660254037844387"),
                                   expressionPostAgg("p3", "0.5773502691896257"),
                                   expressionPostAgg("p4", "1.7320508075688776"),
                                   expressionPostAgg("p5", "asin((exp(\"a0\") / 2))"),
                                   expressionPostAgg("p6", "acos((exp(\"a0\") / 2))"),
                                   expressionPostAgg("p7", "atan((exp(\"a0\") / 2))"),
                                   expressionPostAgg("p8", "atan2(exp(\"a0\"),1)")
                               )
                               .context(QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS)
                               .build()),
        ImmutableList.of(
            new Object[]{
                11.0,
                Math.sin(Math.PI / 6),
                Math.cos(Math.PI / 6),
                Math.tan(Math.PI / 6),
                Math.cos(Math.PI / 6) / Math.sin(Math.PI / 6),
                Math.asin(0.5),
                Math.acos(0.5),
                Math.atan(0.5),
                Math.atan2(1, 1)
            }
        )
    );
  }

  @Test
  public void testRadiansAndDegrees() throws Exception
  {
    testQuery(
        "SELECT RADIANS(m1 * 15)/DEGREES(m2) FROM numfoo WHERE dim1 = '1'",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "(toRadians((\"m1\" * 15)) / toDegrees(\"m2\"))", ValueType.DOUBLE)
                )
                .columns("v0")
                .filters(selector("dim1", "1", null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{Math.toRadians(60) / Math.toDegrees(4)}
        )
    );
  }

  @Test
  public void testTimestampDiff() throws Exception
  {
    testQuery(
        "SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '1999-01-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(DAY, __time, DATE '2001-01-01'), \n"
        + "TIMESTAMPDIFF(HOUR, TIMESTAMP '1999-12-31 01:00:00', __time), \n"
        + "TIMESTAMPDIFF(MINUTE, TIMESTAMP '1999-12-31 23:58:03', __time), \n"
        + "TIMESTAMPDIFF(SECOND, TIMESTAMP '1999-12-31 23:59:03', __time), \n"
        + "TIMESTAMPDIFF(MONTH, TIMESTAMP '1999-11-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(YEAR, TIMESTAMP '1996-11-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(QUARTER, TIMESTAMP '1996-10-01 00:00:00', __time), \n"
        + "TIMESTAMPDIFF(WEEK, TIMESTAMP '1998-10-01 00:00:00', __time) \n"
        + "FROM druid.foo\n"
        + "LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "div((\"__time\" - 915148800000),86400000)", ValueType.LONG),
                    expressionVirtualColumn("v1", "div((978307200000 - \"__time\"),86400000)", ValueType.LONG),
                    expressionVirtualColumn("v2", "div((\"__time\" - 946602000000),3600000)", ValueType.LONG),
                    expressionVirtualColumn("v3", "div((\"__time\" - 946684683000),60000)", ValueType.LONG),
                    expressionVirtualColumn("v4", "div((\"__time\" - 946684743000),1000)", ValueType.LONG),
                    expressionVirtualColumn("v5", "subtract_months(\"__time\",941414400000,'UTC')", ValueType.LONG),
                    expressionVirtualColumn(
                        "v6",
                        "div(subtract_months(\"__time\",846806400000,'UTC'),12)",
                        ValueType.LONG
                    ),
                    expressionVirtualColumn(
                        "v7",
                        "div(subtract_months(\"__time\",844128000000,'UTC'),3)",
                        ValueType.LONG
                    ),
                    expressionVirtualColumn("v8", "div(div((\"__time\" - 907200000000),1000),604800)", ValueType.LONG)
                )
                .columns("v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8")
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()

        ),
        ImmutableList.of(
            new Object[]{365, 366, 23, 1, 57, 2, 3, 13, 65},
            new Object[]{366, 365, 47, 1441, 86457, 2, 3, 13, 65}
        )
    );
  }

  @Test
  public void testTimestampCeil() throws Exception
  {
    testQuery(
        "SELECT CEIL(TIMESTAMP '2000-01-01 00:00:00' TO DAY), \n"
        + "CEIL(TIMESTAMP '2000-01-01 01:00:00' TO DAY) \n"
        + "FROM druid.foo\n"
        + "LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "946684800000", ValueType.LONG),
                    expressionVirtualColumn("v1", "946771200000", ValueType.LONG)
                )
                .columns("v0", "v1")
                .limit(1)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()

        ),
        ImmutableList.of(
            new Object[]{
                Calcites.jodaToCalciteTimestamp(
                    DateTimes.of("2000-01-01"),
                    DateTimeZone.UTC
                ),
                Calcites.jodaToCalciteTimestamp(
                    DateTimes.of("2000-01-02"),
                    DateTimeZone.UTC
                )
            }
        )
    );
  }

  @Test
  public void testNvlColumns() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT NVL(dim2, dim1), COUNT(*) FROM druid.foo GROUP BY NVL(dim2, dim1)\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",\"dim1\")",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"10.1", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 2L}
        )
    );
  }

  @Test
  public void testGroupByWithLiteralInSubqueryGrouping() throws Exception
  {
    testQuery(
        "SELECT \n"
        + "   t1, t2\n"
        + "  FROM\n"
        + "   ( SELECT\n"
        + "     'dummy' as t1,\n"
        + "     CASE\n"
        + "       WHEN \n"
        + "         dim4 = 'b'\n"
        + "       THEN dim4\n"
        + "       ELSE NULL\n"
        + "     END AS t2\n"
        + "     FROM\n"
        + "       numfoo\n"
        + "     GROUP BY\n"
        + "       dim4\n"
        + "   )\n"
        + " GROUP BY\n"
        + "   t1,t2\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(CalciteTests.DATASOURCE3)
                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(new DefaultDimensionSpec("dim4", "_d0", ValueType.STRING))
                                        .setContext(QUERY_CONTEXT_DEFAULT)
                                        .build()
                        )
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "\'dummy\'",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "case_searched((\"_d0\" == 'b'),\"_d0\",null)",
                                ValueType.STRING
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.STRING),
                                new DefaultDimensionSpec("v1", "d1", ValueType.STRING)
                            )
                        )
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"dummy", NULL_STRING},
            new Object[]{"dummy", "b"}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringGroupBy() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    List<Object[]> expected;
    if (NullHandling.replaceWithDefault()) {
      expected = ImmutableList.of(
          new Object[]{"bfoo", 2L},
          new Object[]{"foo", 2L},
          new Object[]{"", 1L},
          new Object[]{"afoo", 1L},
          new Object[]{"cfoo", 1L},
          new Object[]{"dfoo", 1L}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, 2L},
          new Object[]{"bfoo", 2L},
          new Object[]{"afoo", 1L},
          new Object[]{"cfoo", 1L},
          new Object[]{"dfoo", 1L},
          new Object[]{"foo", 1L}
      );
    }
    testQuery(
        "SELECT concat(dim3, 'foo'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ValueType.STRING))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        expected
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringGroupByWithFilter() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT concat(dim3, 'foo'), SUM(cnt) FROM druid.numfoo where concat(dim3, 'foo') = 'bfoo' GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ValueType.STRING))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setDimFilter(selector("v0", "bfoo", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"bfoo", 2L},
            new Object[]{"afoo", 1L},
            new Object[]{"cfoo", 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringScan() throws Exception
  {
    final String nullVal = NullHandling.replaceWithDefault() ? "[\"foo\"]" : "[null]";
    testQuery(
        "SELECT concat(dim3, 'foo') FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ValueType.STRING))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"afoo\",\"bfoo\"]"},
            new Object[]{"[\"bfoo\",\"cfoo\"]"},
            new Object[]{"[\"dfoo\"]"},
            new Object[]{"[\"foo\"]"},
            new Object[]{nullVal},
            new Object[]{nullVal}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringSelfConcatScan() throws Exception
  {
    final String nullVal = NullHandling.replaceWithDefault() ? "[\"-lol-\"]" : "[null]";
    testQuery(
        "SELECT concat(dim3, '-lol-', dim3) FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'-lol-',\"dim3\")", ValueType.STRING))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a-lol-a\",\"b-lol-b\"]"},
            new Object[]{"[\"b-lol-b\",\"c-lol-c\"]"},
            new Object[]{"[\"d-lol-d\"]"},
            new Object[]{"[\"-lol-\"]"},
            new Object[]{nullVal},
            new Object[]{nullVal}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringScanWithFilter() throws Exception
  {
    testQuery(
        "SELECT concat(dim3, 'foo') FROM druid.numfoo where concat(dim3, 'foo') = 'bfoo'",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ValueType.STRING))
                .filters(selector("v0", "bfoo", null))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"afoo\",\"bfoo\"]"},
            new Object[]{"[\"bfoo\",\"cfoo\"]"}
        )
    );
  }

  @Test
  public void testSelectConstantArrayExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT ARRAY[1,2] as arr, dim1 FROM foo LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "array(1,2)", ValueType.STRING))
                .columns("dim1", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[1,2]", ""}
        )
    );
  }

  @Test
  public void testSelectNonConstantArrayExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT ARRAY[CONCAT(dim1, 'word'),'up'] as arr, dim1 FROM foo LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "array(concat(\"dim1\",'word'),'up')", ValueType.STRING))
                .columns("dim1", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"word\",\"up\"]", ""},
            new Object[]{"[\"10.1word\",\"up\"]", "10.1"},
            new Object[]{"[\"2word\",\"up\"]", "2"},
            new Object[]{"[\"1word\",\"up\"]", "1"},
            new Object[]{"[\"defword\",\"up\"]", "def"}
        )
    );
  }

  @Test
  public void testSelectNonConstantArrayExpressionFromTableFailForMultival() throws Exception
  {
    // without expression output type inference to prevent this, the automatic translation will try to turn this into
    //
    //    `map((dim3) -> array(concat(dim3,'word'),'up'), dim3)`
    //
    // This error message will get better in the future. The error without translation would be:
    //
    //    org.apache.druid.java.util.common.RE: Unhandled array constructor element type [STRING_ARRAY]

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Unhandled map function output type [STRING_ARRAY]");
    testQuery(
        "SELECT ARRAY[CONCAT(dim3, 'word'),'up'] as arr, dim1 FROM foo LIMIT 5",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testMultiValueStringOverlapFilter() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_OVERLAP(dim3, ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(new InDimFilter("dim3", ImmutableList.of("a", "b"), null))
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]"},
            new Object[]{"[\"b\",\"c\"]"}
        )
    );
  }

  @Test
  public void testMultiValueStringOverlapFilterNonLiteral() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_OVERLAP(dim3, ARRAY[dim2]) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(expressionFilter("array_overlap(\"dim3\",array(\"dim2\"))"))
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]"},
            new Object[]{useDefault ? "" : null}
        )
    );
  }

  @Test
  public void testMultiValueStringContainsFilter() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_CONTAINS(dim3, ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    new AndDimFilter(
                        new SelectorDimFilter("dim3", "a", null),
                        new SelectorDimFilter("dim3", "b", null)
                    )
                )
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]"}
        )
    );
  }

  @Test
  public void testMultiValueStringContainsArrayOfOneElement() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_CONTAINS(dim3, ARRAY['a']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(new SelectorDimFilter("dim3", "a", null))
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]"}
        )
    );
  }

  @Test
  public void testMultiValueStringContainsArrayOfNonLiteral() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_CONTAINS(dim3, ARRAY[dim2]) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(expressionFilter("array_contains(\"dim3\",array(\"dim2\"))"))
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]"},
            new Object[]{useDefault ? "" : null}
        )
    );
  }

  @Test
  public void testMultiValueStringSlice() throws Exception
  {
    testQuery(
        "SELECT MV_SLICE(dim3, 1) FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "array_slice(\"dim3\",1)", ValueType.STRING))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"b\"]"},
            new Object[]{"[\"c\"]"},
            new Object[]{"[]"},
            new Object[]{"[]"},
            new Object[]{"[]"},
            new Object[]{"[]"}
        )
    );
  }

  @Test
  public void testMultiValueStringLength() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT dim1, MV_LENGTH(dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1, 2 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "array_length(\"dim3\")", ValueType.LONG))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "_d0", ValueType.STRING),
                                new DefaultDimensionSpec("v0", "_d1", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "_d1",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", 2, 1L},
            new Object[]{"10.1", 2, 1L},
            new Object[]{"1", 1, 1L},
            new Object[]{"2", 1, 1L},
            new Object[]{"abc", 1, 1L},
            new Object[]{"def", 1, 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringAppend() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (NullHandling.replaceWithDefault()) {
      results = ImmutableList.of(
          new Object[]{"foo", 6L},
          new Object[]{"", 3L},
          new Object[]{"b", 2L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L},
          new Object[]{"d", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"foo", 6L},
          new Object[]{null, 2L},
          new Object[]{"b", 2L},
          new Object[]{"", 1L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L},
          new Object[]{"d", 1L}
      );
    }
    testQuery(
        "SELECT MV_APPEND(dim3, 'foo'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_append(\"dim3\",'foo')",
                            ValueType.STRING
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testMultiValueStringPrepend() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (NullHandling.replaceWithDefault()) {
      results = ImmutableList.of(
          new Object[]{"foo", 6L},
          new Object[]{"", 3L},
          new Object[]{"b", 2L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L},
          new Object[]{"d", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"foo", 6L},
          new Object[]{null, 2L},
          new Object[]{"b", 2L},
          new Object[]{"", 1L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L},
          new Object[]{"d", 1L}
      );
    }
    testQuery(
        "SELECT MV_PREPEND('foo', dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_prepend('foo',\"dim3\")",
                            ValueType.STRING
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testMultiValueStringPrependAppend() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (NullHandling.replaceWithDefault()) {
      results = ImmutableList.of(
          new Object[]{"foo,null", "null,foo", 3L},
          new Object[]{"foo,a,b", "a,b,foo", 1L},
          new Object[]{"foo,b,c", "b,c,foo", 1L},
          new Object[]{"foo,d", "d,foo", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"foo,null", "null,foo", 2L},
          new Object[]{"foo,", ",foo", 1L},
          new Object[]{"foo,a,b", "a,b,foo", 1L},
          new Object[]{"foo,b,c", "b,c,foo", 1L},
          new Object[]{"foo,d", "d,foo", 1L}
      );
    }
    testQuery(
        "SELECT MV_TO_STRING(MV_PREPEND('foo', dim3), ','), MV_TO_STRING(MV_APPEND(dim3, 'foo'), ','), SUM(cnt) FROM druid.numfoo GROUP BY 1,2 ORDER BY 3 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "array_to_string(array_prepend('foo',\"dim3\"),',')",
                                ValueType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "array_to_string(array_append(\"dim3\",'foo'),',')",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING),
                                new DefaultDimensionSpec("v1", "_d1", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testMultiValueStringConcat() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (NullHandling.replaceWithDefault()) {
      results = ImmutableList.of(
          new Object[]{"", 6L},
          new Object[]{"b", 4L},
          new Object[]{"a", 2L},
          new Object[]{"c", 2L},
          new Object[]{"d", 2L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null, 4L},
          new Object[]{"b", 4L},
          new Object[]{"", 2L},
          new Object[]{"a", 2L},
          new Object[]{"c", 2L},
          new Object[]{"d", 2L}
      );
    }
    testQuery(
        "SELECT MV_CONCAT(dim3, dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_concat(\"dim3\",\"dim3\")",
                            ValueType.STRING
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testMultiValueStringOffset() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_OFFSET(dim3, 1), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "array_offset(\"dim3\",1)", ValueType.STRING))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"b", 1L},
            new Object[]{"c", 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringOrdinal() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_ORDINAL(dim3, 2), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "array_ordinal(\"dim3\",2)", ValueType.STRING))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 4L},
            new Object[]{"b", 1L},
            new Object[]{"c", 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringOffsetOf() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_OFFSET_OF(dim3, 'b'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_offset_of(\"dim3\",'b')",
                            ValueType.LONG
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? -1 : null, 4L},
            new Object[]{0, 1L},
            new Object[]{1, 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringOrdinalOf() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_ORDINAL_OF(dim3, 'b'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_ordinal_of(\"dim3\",'b')",
                            ValueType.LONG
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.replaceWithDefault() ? -1 : null, 4L},
            new Object[]{1, 1L},
            new Object[]{2, 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringToString() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (NullHandling.replaceWithDefault()) {
      results = ImmutableList.of(
          new Object[]{"", 3L},
          new Object[]{"a,b", 1L},
          new Object[]{"b,c", 1L},
          new Object[]{"d", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null, 2L},
          new Object[]{"", 1L},
          new Object[]{"a,b", 1L},
          new Object[]{"b,c", 1L},
          new Object[]{"d", 1L}
      );
    }
    testQuery(
        "SELECT MV_TO_STRING(dim3, ','), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_to_string(\"dim3\",',')",
                            ValueType.STRING
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testMultiValueStringToStringToMultiValueString() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (NullHandling.replaceWithDefault()) {
      results = ImmutableList.of(
          new Object[]{"d", 7L},
          new Object[]{"", 3L},
          new Object[]{"b", 2L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"d", 5L},
          new Object[]{null, 2L},
          new Object[]{"b", 2L},
          new Object[]{"", 1L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L}
      );
    }
    testQuery(
        "SELECT STRING_TO_MV(CONCAT(MV_TO_STRING(dim3, ','), ',d'), ','), SUM(cnt) FROM druid.numfoo WHERE MV_LENGTH(dim3) > 0 GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "array_length(\"dim3\")", ValueType.LONG),
                            expressionVirtualColumn(
                                "v1",
                                "string_to_array(concat(array_to_string(\"dim3\",','),',d'),',')",
                                ValueType.STRING
                            )
                        )
                        .setDimFilter(bound("v0", "0", null, true, false, null, StringComparators.NUMERIC))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "_d0", ValueType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testLeftRightStringOperators() throws Exception
  {
    testQuery(
        "SELECT\n"
        + "  dim1,"
        + "  LEFT(dim1, 2),\n"
        + "  RIGHT(dim1, 2)\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim1", "d0")))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "left(\"d0\",2)"),
                            expressionPostAgg("p1", "right(\"d0\",2)")
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "", ""},
            new Object[]{"1", "1", "1"},
            new Object[]{"10.1", "10", ".1"},
            new Object[]{"2", "2", "2"},
            new Object[]{"abc", "ab", "bc"},
            new Object[]{"def", "de", "ef"}
        )
    );
  }

  @Test
  public void testQueryContextOuterLimit() throws Exception
  {
    Map<String, Object> outerLimitContext = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    outerLimitContext.put(PlannerContext.CTX_SQL_OUTER_LIMIT, 4);

    TopNQueryBuilder baseBuilder = new TopNQueryBuilder()
        .dataSource(CalciteTests.DATASOURCE1)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .granularity(Granularities.ALL)
        .dimension(new DefaultDimensionSpec("dim1", "d0"))
        .metric(
            new InvertedTopNMetricSpec(
                new DimensionTopNMetricSpec(
                    null,
                    StringComparators.LEXICOGRAPHIC
                )
            )
        )
        .context(outerLimitContext);

    List<Object[]> results1;
    if (NullHandling.replaceWithDefault()) {
      results1 = ImmutableList.of(
          new Object[]{""},
          new Object[]{"def"},
          new Object[]{"abc"},
          new Object[]{"2"}
      );
    } else {
      results1 = ImmutableList.of(
          new Object[]{"def"},
          new Object[]{"abc"},
          new Object[]{"2"},
          new Object[]{"10.1"}
      );
    }

    // no existing limit
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        outerLimitContext,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            baseBuilder.threshold(4).build()
        ),
        results1
    );

    // existing limit greater than context limit, override existing limit
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        outerLimitContext,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC LIMIT 9",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            baseBuilder.threshold(4).build()
        ),
        results1
    );


    List<Object[]> results2;
    if (NullHandling.replaceWithDefault()) {
      results2 = ImmutableList.of(
          new Object[]{""},
          new Object[]{"def"}
      );
    } else {
      results2 = ImmutableList.of(
          new Object[]{"def"},
          new Object[]{"abc"}
      );
    }

    // existing limit less than context limit, keep existing limit
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        outerLimitContext,
        "SELECT dim1 FROM druid.foo GROUP BY dim1 ORDER BY dim1 DESC LIMIT 2",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            baseBuilder.threshold(2).build()

        ),
        results2
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
                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ValueType.STRING))
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
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ValueType.STRING))
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
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ValueType.STRING))
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
  public void testInnerJoinOnTwoInlineDataSourcesWithOuterWhere(Map<String, Object> queryContext) throws Exception
  {
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
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .filters(new SelectorDimFilter("dim1", "10.1", null))
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ValueType.STRING))
                .columns("__time", "_v0")
                .filters(new NotDimFilter(new SelectorDimFilter("v0", null, null)))
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
                                .virtualColumns(expressionVirtualColumn("v0", "\'10.1\'", ValueType.STRING))
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
                .virtualColumns(expressionVirtualColumn("_v0", "\'10.1\'", ValueType.STRING))
                .columns("__time", "_v0")
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
  @Test(expected = RelOptPlanner.CannotPlanException.class)
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testJoinOnConstantShouldFail(Map<String, Object> queryContext) throws Exception
  {
    cannotVectorize();

    final String query = "SELECT t1.dim1 from foo as t1 LEFT JOIN foo as t2 on t1.dim1 = '10.1'";

    testQuery(
        query,
        queryContext,
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testRepeatedIdenticalVirtualExpressionGrouping() throws Exception
  {
    cannotVectorize();

    final String query = "SELECT \n"
                         + "\tCASE dim1 WHEN NULL THEN FALSE ELSE TRUE END AS col_a,\n"
                         + "\tCASE dim2 WHEN NULL THEN FALSE ELSE TRUE END AS col_b\n"
                         + "FROM foo\n"
                         + "GROUP BY 1, 2";

    testQuery(
        query,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "1", ValueType.LONG))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.LONG),
                                new DefaultDimensionSpec("v0", "d1", ValueType.LONG)
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{true, true}
        )
    );
  }

  @Test
  public void testValidationErrorNullLiteralIllegal() throws Exception
  {
    expectedException.expectMessage("Illegal use of 'NULL'");

    testQuery(
        "SELECT REGEXP_LIKE('x', NULL)",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testValidationErrorNonLiteralIllegal() throws Exception
  {
    expectedException.expectMessage("Argument to function 'REGEXP_LIKE' must be a literal");

    testQuery(
        "SELECT REGEXP_LIKE('x', dim1) FROM foo",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testValidationErrorWrongTypeLiteral() throws Exception
  {
    expectedException.expectMessage("Cannot apply 'REGEXP_LIKE' to arguments");

    testQuery(
        "SELECT REGEXP_LIKE('x', 1) FROM foo",
        ImmutableList.of(),
        ImmutableList.of()
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
                .dimension(new DefaultDimensionSpec("j0.dim4", "_d0", ValueType.STRING))
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

  /**
   * This is a provider of query contexts that should be used by join tests.
   * It tests various configs that can be passed to join queries. All the configs provided by this provider should
   * have the join query engine return the same results.
   */
  public static class QueryContextForJoinProvider
  {
    @UsedByJUnitParamsRunner
    public static Object[] provideQueryContexts()
    {
      return new Object[] {
          // default behavior
          QUERY_CONTEXT_DEFAULT,
          // filter value re-writes enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .build(),
          // rewrite values enabled but filter re-writes disabled.
          // This should be drive the same behavior as the previous config
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .build(),
          // filter re-writes disabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .build(),
      };
    }
  }
}
