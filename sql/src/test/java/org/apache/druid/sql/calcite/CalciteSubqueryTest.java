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
import com.google.inject.Injector;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Order;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
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
import org.apache.druid.query.aggregation.firstlast.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Calcite tests which involve subqueries and materializing the intermediate results on {@link org.apache.druid.server.ClientQuerySegmentWalker}
 * The tests are run with two different codepaths:
 * 1. Where the memory limit is not set. The intermediate results are materialized as inline rows
 * 2. Where the memory limit is set. The intermediate results are materialized as frames
 */
@SqlTestFrameworkConfig.ComponentSupplier(CalciteSubqueryTest.SubqueryComponentSupplier.class)
public class CalciteSubqueryTest extends BaseCalciteQueryTest
{
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testExactCountDistinctUsingSubqueryWithWhereToOuterFilter(String testName, Map<String, Object> queryContext)
  {
    if (!queryContext.containsKey(QueryContexts.MAX_SUBQUERY_BYTES_KEY)) {
      cannotVectorize();
    }
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSubqueryOnDataSourceWithMissingColumnsInSegments(String testName, Map<String, Object> queryContext)
  {
    if (!queryContext.containsKey(QueryContexts.MAX_SUBQUERY_BYTES_KEY)) {
      cannotVectorize();
    }
    testQuery(
        "SELECT\n"
        + "  __time,\n"
        + "  col1,\n"
        + "  col2,\n"
        + "  col3,\n"
        + "  COUNT(*)\n"
        + "FROM (SELECT * FROM dsMissingCol LIMIT 10)\n"
        + "GROUP BY 1, 2, 3, 4",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            new QueryDataSource(
                                newScanQueryBuilder()
                                    .dataSource("dsMissingCol")
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .columns("__time", "col1", "col2", "col3")
                                    .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
                                    .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                    .limit(10)
                                    .build()
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(
                            new DefaultDimensionSpec("__time", "d0", ColumnType.LONG),
                            new DefaultDimensionSpec("col1", "d1", ColumnType.STRING),
                            new DefaultDimensionSpec("col2", "d2", ColumnType.STRING),
                            new DefaultDimensionSpec("col3", "d3", ColumnType.STRING)
                        )
                        .setAggregatorSpecs(aggregators(
                            new CountAggregatorFactory("a0")
                        ))
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, "abc", NullHandling.defaultStringValue(), "def", 1L},
            new Object[]{946684800000L, "foo", "bar", NullHandling.defaultStringValue(), 1L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testExactCountDistinctOfSemiJoinResult(String testName, Map<String, Object> queryContext)
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testTwoExactCountDistincts(String testName, Map<String, Object> queryContext)
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
                .columnTypes(ColumnType.LONG, ColumnType.LONG)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{NullHandling.sqlCompatible() ? 6L : 5L, NullHandling.sqlCompatible() ? 3L : 2L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testViewAndJoin(String testName, Map<String, Object> queryContext)
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
                                                       .columnTypes(ColumnType.STRING)
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
                                                   .columnTypes(ColumnType.STRING)
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testGroupByWithPostAggregatorReferencingTimeFloorColumnOnTimeseries(String testName, Map<String, Object> queryContext)
  {
    if (!queryContext.containsKey(QueryContexts.MAX_SUBQUERY_BYTES_KEY)) {
      cannotVectorize();
    }
    cannotVectorizeUnlessFallback();
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testUsingSubqueryAsFilterWithInnerSort(String testName, Map<String, Object> queryContext)
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
                .columnTypes(ColumnType.STRING, ColumnType.STRING)
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testUsingSubqueryAsFilterOnTwoColumns(String testName, Map<String, Object> queryContext)
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
                .columnTypes(ColumnType.LONG, ColumnType.LONG, ColumnType.STRING, ColumnType.STRING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2001-01-02"), 1L, "def", "abc"}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testMinMaxAvgDailyCountWithLimit(String testName, Map<String, Object> queryContext)
  {
    if (!queryContext.containsKey(QueryContexts.MAX_SUBQUERY_BYTES_KEY)) {
      cannotVectorize();
    }
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testEmptyGroupWithOffsetDoesntInfiniteLoop(String testName, Map<String, Object> queryContext)
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
                  .columnTypes(ColumnType.LONG, ColumnType.LONG)
                  .limit(10)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of()
    );
  }


  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testMaxSubqueryRows(String testName, Map<String, Object> queryContext)
  {
    if ("without memory limit".equals(testName)) {
      testMaxSubqueryRowsWithoutMemoryLimit(testName, queryContext);
    } else {
      testMaxSubQueryRowsWithLimit(testName, queryContext);
    }
  }

  private void testMaxSubqueryRowsWithoutMemoryLimit(String testName, Map<String, Object> queryContext)
  {
    Map<String, Object> modifiedQueryContext = new HashMap<>(queryContext);
    modifiedQueryContext.put(QueryContexts.MAX_SUBQUERY_ROWS_KEY, 1);

    testQueryThrows(
        "SELECT\n"
            + "  SUM(cnt),\n"
            + "  COUNT(*)\n"
            + "FROM (SELECT dim2, SUM(cnt) AS cnt FROM druid.foo GROUP BY dim2 LIMIT 2) \n"
            + "WHERE cnt > 0",
        modifiedQueryContext,
        ResourceLimitExceededException.class,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString(
                "Cannot issue the query, subqueries generated results beyond maximum[1] rows. Try setting the "
                    + "'maxSubqueryBytes' in the query context to 'auto' for enabling byte based limit, which chooses an optimal "
                    + "limit based on memory size and result's heap usage or manually configure the values of either 'maxSubqueryBytes' "
                    + "or 'maxSubqueryRows' in the query context. Manually alter the value carefully as it can cause the broker to "
                    + "go out of memory."
            )
        )
    );
  }

  private void testMaxSubQueryRowsWithLimit(String testName, Map<String, Object> queryContext)
  {
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testZeroMaxNumericInFilter(String testName, Map<String, Object> queryContext)
  {
    Throwable exception = assertThrows(UOE.class, () -> {

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
    });
    assertTrue(exception.getMessage().contains("[maxNumericInFilters] must be greater than 0"));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testUseTimeFloorInsteadOfGranularityOnJoinResult(String testName, Map<String, Object> queryContext)
  {
    if (!queryContext.containsKey(QueryContexts.MAX_SUBQUERY_BYTES_KEY)) {
      cannotVectorize();
    }
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
                                        .columnTypes(ColumnType.LONG, ColumnType.STRING)
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
                                                            .columnTypes(ColumnType.STRING)
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testJoinWithTimeDimension(String testName, Map<String, Object> queryContext)
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
                                             .columnTypes(ColumnType.LONG)
                                             .context(queryContext)
                                             .build()),
                                   "j0.",
                                   "(\"__time\" == \"j0.__time\")",
                                   JoinType.INNER,
                                   null,
                                   ExprMacroTable.nil(),
                                   CalciteTests.createJoinableFactoryWrapper(),
                                   JoinAlgorithm.BROADCAST
                               ))
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(new CountAggregatorFactory("a0")))
                               .context(queryContext)
                               .build()),
        ImmutableList.of(new Object[]{6L})
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testUsingSubqueryWithLimit(String testName, Map<String, Object> queryContext)
  {
    if (!queryContext.containsKey(QueryContexts.MAX_SUBQUERY_BYTES_KEY)) {
      cannotVectorize();
    }
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
                                .columnTypes(ColumnType.LONG)
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSelfJoin(String testName, Map<String, Object> queryContext)
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
                                  .columns("__time", "dim1", "dim2", "dim3", "cnt", "m1", "m2", "unique_dim1")
                                  .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.LONG, ColumnType.FLOAT, ColumnType.DOUBLE, ColumnType.ofComplex("hyperUnique"))
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testJoinWithSubqueries(String testName, Map<String, Object> queryContext)
  {
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
                                    .columnTypes(ColumnType.STRING)
                                    .eternityInterval()
                                    .build()
                            ),
                            "j0.",
                            "(\"dim1\" == \"j0.dim1\")",
                            JoinType.LEFT,
                            null,
                            ExprMacroTable.nil(),
                            CalciteTests.createJoinableFactoryWrapper(),
                            JoinAlgorithm.BROADCAST
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
                                                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                                                .eternityInterval()
                                                .build()
                                        ),
                                        "j0.",
                                        "(\"dim1\" == \"j0.dim1\")",
                                        JoinType.LEFT,
                                        null,
                                        ExprMacroTable.nil(),
                                        CalciteTests.createJoinableFactoryWrapper(),
                                        JoinAlgorithm.BROADCAST
                                    )
                                )
                                .columns("dim1", "j0.dim2")
                                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                                .eternityInterval()
                                .build()
                        ),
                        "_j0.",
                        "(\"dim1\" == \"_j0.dim1\")",
                        JoinType.FULL,
                        null,
                        ExprMacroTable.nil(),
                        CalciteTests.createJoinableFactoryWrapper(),
                        JoinAlgorithm.BROADCAST
                    )
                )
                .columns("dim1", "_j0.j0.dim2")
                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                .eternityInterval()
                .build()
        ),
        results
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSingleValueFloatAgg(String testName, Map<String, Object> queryContext)
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT count(*) FROM foo where m1 <= (select min(m1) + 4 from foo)")
        .expectedQuery(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(new FloatMinAggregatorFactory("a0", "m1"))
                                .postAggregators(
                                    expressionPostAgg("p0", "(\"a0\" + 4)", ColumnType.FLOAT)
                                )

                                .build()
                        ),
                        "j0.",
                        "1",
                        NullHandling.replaceWithDefault() ? JoinType.LEFT : JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .filters(expressionFilter("(\"m1\" <= \"j0.p0\")"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {5L}
            )
        )
        .run();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSingleValueDoubleAgg(String testName, Map<String, Object> queryContext)
  {
    cannotVectorize();
    testBuilder()
        .sql("SELECT count(*) FROM foo where m1 >= (select max(m1) - 3.5 from foo)")
        .expectedQuery(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.DATASOURCE1),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(new FloatMaxAggregatorFactory("a0", "m1"))
                                .postAggregators(
                                    expressionPostAgg(
                                        "p0",
                                        "(\"a0\" - 3.5)",
                                        ColumnType.DOUBLE
                                    )
                                )
                                .build()
                        ),
                        "j0.",
                        "1",
                        NullHandling.replaceWithDefault() ? JoinType.LEFT : JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .filters(expressionFilter("(\"m1\" >= \"j0.p0\")"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {4L}
            )
        )
        .run();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSingleValueLongAgg(String testName, Map<String, Object> queryContext)
  {
    cannotVectorize();
    testBuilder()
        .sql(
            "SELECT count(*) FROM wikipedia where __time >= (select max(__time) - INTERVAL '10' MINUTE from wikipedia)"
        )
        .expectedQuery(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.WIKIPEDIA),
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.WIKIPEDIA)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .granularity(Granularities.ALL)
                                .aggregators(
                                    new LongMaxAggregatorFactory(
                                        "a0",
                                        "__time"
                                    )
                                )
                                .postAggregators(
                                    expressionPostAgg(
                                        "p0", "(\"a0\" - 600000)",
                                        ColumnType.LONG
                                    )
                                )
                                .build()
                        ),
                        "j0.",
                        "1",
                        NullHandling.replaceWithDefault() ? JoinType.LEFT : JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .filters(expressionFilter("(\"__time\" >= \"j0.p0\")"))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {220L}
            )
        )
        .run();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSingleValueStringAgg(String testName, Map<String, Object> queryContext)
  {
    testBuilder()
        .sql(
            "SELECT count(*) FROM wikipedia where channel = (select channel from wikipedia order by __time desc LIMIT 1 OFFSET 6)"
        )
        .expectedQuery(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(
                    join(
                        new TableDataSource(CalciteTests.WIKIPEDIA),
                        new QueryDataSource(
                            Druids.newScanQueryBuilder()
                                .dataSource(CalciteTests.WIKIPEDIA)
                                .intervals(querySegmentSpec(Filtration.eternity()))
                                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                .offset(6L)
                                .limit(1L)
                                .order(Order.DESCENDING)
                                .columns("channel", "__time")
                                .columnTypes(ColumnType.STRING, ColumnType.STRING)
                                .context(QUERY_CONTEXT_DEFAULT)
                                .build()
                        ),
                        "j0.",
                        "(\"channel\" == \"j0.channel\")",
                        JoinType.INNER
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        )
        .expectedResults(
            ImmutableList.of(
                new Object[] {1256L}
            )
        )
        .run();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSingleValueStringMultipleRowsAgg(String testName, Map<String, Object> queryContext)
  {
    cannotVectorize();
    testQueryThrows(
        "SELECT  count(*) FROM wikipedia where channel = (select channel from wikipedia order by __time desc LIMIT 2 OFFSET 6)",
        RuntimeException.class,
        "java.util.concurrent.ExecutionException: org.apache.druid.error.DruidException: Subquery expression returned more than one row"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSingleValueEmptyInnerAgg(String testName, Map<String, Object> queryContext)
  {
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
                                                      .filters(in("channel", Arrays.asList("abc", "xyz")))
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

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testGroupBySubqueryWithEarliestAggregator(String testName, Map<String, Object> queryContext)
  {
    cannotVectorize();

    // Note: EARLIEST aggregator is used because the intermediate type "serializablePair" is different from the finalized type
    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{"1", "", "a", "1"},
          new Object[]{"10.1", "b", "", "10.1"},
          new Object[]{"10.1", "c", "", "10.1"},
          new Object[]{"2", "d", "", "2"},
          new Object[]{"abc", "", "", "abc"},
          new Object[]{"def", "", "abc", "def"}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{"", "a", "a", ""},
          new Object[]{"", "b", "a", ""},
          new Object[]{"1", "", "a", "1"},
          new Object[]{"10.1", "b", null, "10.1"},
          new Object[]{"10.1", "c", null, "10.1"},
          new Object[]{"2", "d", "", "2"},
          new Object[]{"abc", null, null, "abc"},
          new Object[]{"def", null, "abc", "def"}
      );
    }

    testQuery(
        "SELECT a.dim1, a.dim3, a.e_dim2, b.dim1 "
        + "FROM ("
        + " SELECT dim1, dim3, EARLIEST(dim2) AS e_dim2 "
        + " FROM foo GROUP BY 1, 2 LIMIT 100"
        + ") a "
        + "INNER JOIN foo b ON a.dim1 = b.dim1",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    JoinDataSource.create(
                        new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource("foo")
                                        .setInterval(querySegmentSpec(Intervals.ETERNITY))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING),
                                            new DefaultDimensionSpec("dim3", "d1", ColumnType.STRING)
                                        )
                                        .addAggregator(new StringFirstAggregatorFactory("a0", "dim2", "__time", 1024))
                                        .setLimitSpec(new DefaultLimitSpec(Collections.emptyList(), 100))
                                        .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource("foo")
                                .intervals(querySegmentSpec(Intervals.ETERNITY))
                                .columns("dim1")
                                .columnTypes(ColumnType.STRING)
                                .build()
                        ),
                        "j0.",
                        "(\"d0\" == \"j0.dim1\")",
                        JoinType.INNER,
                        null,
                        TestExprMacroTable.INSTANCE,
                        null,
                        JoinAlgorithm.BROADCAST
                    )
                )
                .intervals(querySegmentSpec(Intervals.ETERNITY))
                .columns("d0", "d1", "a0", "j0.dim1")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expectedResults
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testTopNSubqueryWithEarliestAggregator(String testName, Map<String, Object> queryContext)
  {
    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{"1", "a", "1"},
          new Object[]{"10.1", "", "10.1"},
          new Object[]{"2", "", "2"},
          new Object[]{"abc", "", "abc"},
          new Object[]{"def", "abc", "def"}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{"", "a", ""},
          new Object[]{"1", "a", "1"},
          new Object[]{"10.1", null, "10.1"},
          new Object[]{"2", "", "2"},
          new Object[]{"abc", null, "abc"},
          new Object[]{"def", "abc", "def"}
      );
    }

    testQuery(
        "SELECT a.dim1, a.e_dim2, b.dim1 "
        + "FROM ("
        + " SELECT dim1, EARLIEST(dim2) AS e_dim2 "
        + " FROM foo "
        + " GROUP BY 1 "
        + " LIMIT 100"
        + ") a "
        + "INNER JOIN foo b ON a.dim1 = b.dim1",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    JoinDataSource.create(
                        new QueryDataSource(
                            new TopNQueryBuilder()
                                .dataSource("foo")
                                .dimension(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                                .threshold(100)
                                .intervals(querySegmentSpec(Intervals.ETERNITY))
                                .granularity(Granularities.ALL)
                                .aggregators(
                                    new StringFirstAggregatorFactory("a0", "dim2", "__time", 1024)
                                )
                                .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource("foo")
                                .intervals(querySegmentSpec(Intervals.ETERNITY))
                                .columns("dim1")
                                .columnTypes(ColumnType.STRING)
                                .build()
                        ),
                        "j0.",
                        "(\"d0\" == \"j0.dim1\")",
                        JoinType.INNER,
                        null,
                        TestExprMacroTable.INSTANCE,
                        null,
                        JoinAlgorithm.BROADCAST
                    )
                )
                .intervals(querySegmentSpec(Intervals.ETERNITY))
                .columns("d0", "a0", "j0.dim1")
                .columnTypes(ColumnType.STRING, ColumnType.STRING, ColumnType.STRING)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expectedResults
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testTimeseriesSubqueryWithEarliestAggregator(String testName, Map<String, Object> queryContext)
  {
    testQuery(
        "SELECT a.__time, a.e_dim2, b.__time "
        + "FROM ("
        + " SELECT TIME_FLOOR(\"__time\", 'PT24H') as __time, EARLIEST(dim2) AS e_dim2 "
        + " FROM foo "
        + " GROUP BY 1 "
        + ") a "
        + "INNER JOIN foo b ON a.__time = b.__time",
        queryContext,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    JoinDataSource.create(
                        new QueryDataSource(
                            Druids.newTimeseriesQueryBuilder()
                                  .dataSource("foo")
                                  .intervals(querySegmentSpec(Intervals.ETERNITY))
                                  .granularity(new PeriodGranularity(
                                      new Period("PT24H"),
                                      null,
                                      DateTimeZone.UTC
                                  ))
                                  .aggregators(new StringFirstAggregatorFactory("a0", "dim2", "__time", 1024))
                                  .build()
                        ),
                        new QueryDataSource(
                            newScanQueryBuilder()
                                .dataSource("foo")
                                .intervals(querySegmentSpec(Intervals.ETERNITY))
                                .columns("__time")
                                .columnTypes(ColumnType.LONG)
                                .build()
                        ),
                        "j0.",
                        "(\"d0\" == \"j0.__time\")",
                        JoinType.INNER,
                        null,
                        TestExprMacroTable.INSTANCE,
                        null,
                        JoinAlgorithm.BROADCAST
                    )
                )
                .intervals(querySegmentSpec(Intervals.ETERNITY))
                .columns("d0", "a0", "j0.__time")
                .columnTypes(ColumnType.LONG, ColumnType.STRING, ColumnType.LONG)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, "a", 946684800000L},
            new Object[]{946771200000L, NullHandling.defaultStringValue(), 946771200000L},
            new Object[]{946857600000L, "", 946857600000L},
            new Object[]{978307200000L, "a", 978307200000L},
            new Object[]{978393600000L, "abc", 978393600000L},
            new Object[]{978480000000L, NullHandling.defaultStringValue(), 978480000000L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testScalarInArrayToUseHavingFilter(String testName, Map<String, Object> queryContext)
  {
    cannotVectorizeUnlessFallback();
    DimFilter filter = NullHandling.replaceWithDefault()
                       ? new InDimFilter("v0", new HashSet<>(Arrays.asList("1", "17")))
                       : new TypedInFilter("v0", ColumnType.LONG, null, ImmutableList.of(1, 17), null);
    testQuery(
        "select countryName from "
        + "(select countryName, length(countryName) as cname from wikipedia group by countryName) "
        + "where SCALAR_IN_ARRAY(cname, ARRAY[17, 1])",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                .setDataSource(new TableDataSource(CalciteTests.WIKIPEDIA))
                .setInterval(querySegmentSpec(Intervals.ETERNITY))
                .setVirtualColumns(expressionVirtualColumn("v0", "strlen(\"countryName\")", ColumnType.LONG))
                .setDimFilter(filter)
                .setGranularity(Granularities.ALL)
                .setDimensions(new DefaultDimensionSpec("countryName", "d0", ColumnType.STRING))
                .setLimitSpec(NoopLimitSpec.instance())
                .setContext(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"Republic of Korea"}
        )
    );
  }

  public static class SubqueryComponentSupplier extends SqlTestFramework.StandardComponentSupplier
  {

    private final TempDirProducer tmpDirProducer;

    public SubqueryComponentSupplier(TempDirProducer tempDirProducer)
    {
      super(tempDirProducer);
      this.tmpDirProducer = tempDirProducer;
    }

    @Override
    public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
        QueryRunnerFactoryConglomerate conglomerate,
        JoinableFactoryWrapper joinableFactory,
        Injector injector
    )
    {
      SpecificSegmentsQuerySegmentWalker walker =
          super.createQuerySegmentWalker(conglomerate, joinableFactory, injector);

      final String datasource1 = "dsMissingCol";
      final File tmpFolder = tempDirProducer.newTempFolder();

      final List<ImmutableMap<String, Object>> rawRows1 = ImmutableList.of(
          ImmutableMap.<String, Object>builder()
                      .put("t", "2000-01-01")
                      .put("col1", "foo")
                      .put("col2", "bar")
                      .build()
      );
      final List<InputRow> rows1 =
          rawRows1
              .stream()
              .map(mapInputRow -> MapInputRowParser.parse(
                  new InputRowSchema(
                      new TimestampSpec("t", "iso", null),
                      new DimensionsSpec(
                          DimensionsSpec.getDefaultSchemas(ImmutableList.of("col1", "col2"))
                      ),
                      null
                  ),
                  mapInputRow
              ))
              .collect(Collectors.toList());
      final QueryableIndex queryableIndex1 = IndexBuilder
          .create()
          .tmpDir(new File(tmpFolder, datasource1))
          .segmentWriteOutMediumFactory(OnHeapMemorySegmentWriteOutMediumFactory.instance())
          .schema(new IncrementalIndexSchema.Builder()
                      .withRollup(false)
                      .withDimensionsSpec(
                          new DimensionsSpec(
                              ImmutableList.of(
                                  new StringDimensionSchema("col1"),
                                  new StringDimensionSchema("col2")
                              )
                          )
                      )
                      .build()
          )
          .rows(rows1)
          .buildMMappedIndex();

      final List<ImmutableMap<String, Object>> rawRows2 = ImmutableList.of(
          ImmutableMap.<String, Object>builder()
                      .put("t", "2000-01-01")
                      .put("col1", "abc")
                      .put("col3", "def")
                      .build()
      );
      final List<InputRow> rows2 =
          rawRows2
              .stream()
              .map(mapInputRow -> MapInputRowParser.parse(
                  new InputRowSchema(
                      new TimestampSpec("t", "iso", null),
                      new DimensionsSpec(
                          DimensionsSpec.getDefaultSchemas(ImmutableList.of("col1", "col3"))
                      ),
                      null
                  ),
                  mapInputRow
              ))
              .collect(Collectors.toList());
      final QueryableIndex queryableIndex2 = IndexBuilder
          .create()
          .tmpDir(new File(tmpFolder, datasource1))
          .segmentWriteOutMediumFactory(OnHeapMemorySegmentWriteOutMediumFactory.instance())
          .schema(new IncrementalIndexSchema.Builder()
                      .withRollup(false)
                      .withDimensionsSpec(
                          new DimensionsSpec(
                              ImmutableList.of(
                                  new StringDimensionSchema("col1"),
                                  new StringDimensionSchema("col3")
                              )
                          )
                      )
                      .build()
          )
          .rows(rows2)
          .buildMMappedIndex();

      walker.add(
          DataSegment.builder()
              .dataSource(datasource1)
              .interval(Intervals.ETERNITY)
              .version("1")
              .shardSpec(new LinearShardSpec(0))
              .size(0)
              .build(),
          queryableIndex1
      );

      walker.add(
          DataSegment.builder()
                     .dataSource(datasource1)
                     .interval(Intervals.ETERNITY)
                     .version("1")
                     .shardSpec(new LinearShardSpec(1))
                     .size(0)
                     .build(),
          queryableIndex2
      );

      return walker;
    }
  }
}
