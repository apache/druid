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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.InvertedTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalciteSelectQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testSelectConstantExpression()
  {
    // Test with a Druid-specific function, to make sure they are hooked up correctly even when not selecting
    // from a table.
    testQuery(
        "SELECT REGEXP_EXTRACT('foo', '^(.)')",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{0L}),
                        RowSignature.builder().add("ZERO", ColumnType.LONG).build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    new ExpressionVirtualColumn(
                        "v0",
                        "'f'",
                        ColumnType.STRING,
                        ExprMacroTable.nil()
                    )
                )
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"f"}
        )
    );
  }

  @Test
  public void testExpressionContainingNull()
  {
    testQuery(
        "SELECT ARRAY ['Hello', NULL]",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{0L}),
                        RowSignature.builder().add("ZERO", ColumnType.LONG).build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    new ExpressionVirtualColumn(
                        "v0",
                        "array('Hello',null)",
                        ColumnType.STRING_ARRAY,
                        ExprMacroTable.nil()
                    )
                )
                .columns("v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{"[\"Hello\",null]"})
    );
  }

  @Test
  public void testValuesContainingNull()
  {
    testQuery(
        "SELECT * FROM (VALUES (NULL, 'United States'))",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{null, "United States"}),
                        RowSignature
                            .builder()
                            .add("EXPR$0", null)
                            .add("EXPR$1", ColumnType.STRING)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("EXPR$0", "EXPR$1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{null, "United States"})
    );
  }

  @Test
  public void testMultipleValuesContainingNull()
  {
    testQuery(
        "SELECT * FROM (VALUES (NULL, 'United States'), ('Delhi', 'India'))",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{null, "United States"}, new Object[]{"Delhi", "India"}),
                        RowSignature
                            .builder()
                            .add("EXPR$0", ColumnType.STRING)
                            .add("EXPR$1", ColumnType.STRING)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("EXPR$0", "EXPR$1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{NULL_STRING, "United States"}, new Object[]{"Delhi", "India"})
    );
  }

  @Test
  public void testMultipleValuesContainingNullAndIntegerValues()
  {
    testQuery(
        "SELECT * FROM (VALUES (NULL, 'United States'), (50, 'India'))",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(new Object[]{null, "United States"}, new Object[]{50L, "India"}),
                        RowSignature
                            .builder()
                            .add("EXPR$0", ColumnType.LONG)
                            .add("EXPR$1", ColumnType.STRING)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("EXPR$0", "EXPR$1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{null, "United States"}, new Object[]{50, "India"})
    );
  }

  @Test
  public void testSelectNonNumericNumberLiterals()
  {
    // Tests to convert NaN, positive infinity and negative infinity as literals.
    testQuery(
        "SELECT"
            + " CAST(1 / 0.0 AS BIGINT),"
            + " CAST(1 / -0.0 AS BIGINT),"
            + " CAST(-1 / 0.0 AS BIGINT),"
            + " CAST(-1 / -0.0 AS BIGINT),"
            + " CAST(0/ 0.0 AS BIGINT)",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(
                            new Object[]{Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, 0L}
                        ),
                        RowSignature.builder()
                            .add("EXPR$0", ColumnType.LONG)
                            .add("EXPR$1", ColumnType.LONG)
                            .add("EXPR$2", ColumnType.LONG)
                            .add("EXPR$3", ColumnType.LONG)
                            .add("EXPR$4", ColumnType.LONG)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MIN_VALUE,
                Long.MIN_VALUE,
                0L
            }
        )
    );
  }

  // Test that the integers are getting correctly casted after being passed through a function when not selecting from
  // a table
  @Test
  public void testDruidLogicalValuesRule()
  {
    testQuery(
        "SELECT FLOOR(123), CEIL(123), CAST(123.0 AS INTEGER)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(InlineDataSource.fromIterable(
                    ImmutableList.of(new Object[]{123L, 123L, 123L}),
                    RowSignature.builder()
                                .add("EXPR$0", ColumnType.LONG)
                                .add("EXPR$1", ColumnType.LONG)
                                .add("EXPR$2", ColumnType.LONG)
                                .build()
                ))
                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.ETERNITY)))
                .columns(ImmutableList.of("EXPR$0", "EXPR$1", "EXPR$2"))
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                123,
                123,
                123
            }
        )
    );
  }

  @Test
  public void testSelectConstantExpressionFromTable()
  {
    testQuery(
        "SELECT 1 + 1, dim1 FROM foo LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "2", ColumnType.LONG))
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
  public void testSelectConstantExpressionEquivalentToNaN()
  {
    expectedException.expectMessage(
        "'(log10(0) - log10(0))' evaluates to 'NaN' that is not supported in SQL. You can either cast the expression as BIGINT ('CAST((log10(0) - log10(0)) as BIGINT)') or VARCHAR ('CAST((log10(0) - log10(0)) as VARCHAR)') or change the expression itself");
    testQuery(
        "SELECT log10(0) - log10(0), dim1 FROM foo LIMIT 1",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectConstantExpressionEquivalentToInfinity()
  {
    expectedException.expectMessage(
        "'log10(0)' evaluates to '-Infinity' that is not supported in SQL. You can either cast the expression as BIGINT ('CAST(log10(0) as BIGINT)') or VARCHAR ('CAST(log10(0) as VARCHAR)') or change the expression itself");
    testQuery(
        "SELECT log10(0), dim1 FROM foo LIMIT 1",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectTrimFamily()
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
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"foo", "xfoo", "foo", " foo", "foo", "foo", "foo", "foo ", "foox", " foo", "xfoo", 6L}
        )
    );
  }

  @Test
  public void testSelectPadFamily()
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
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"xxfoo", "fo", "  foo", "fooxx", "fo", "foo  ", 6L}
        )
    );
  }

  @Test
  public void testBitwiseExpressions()
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{0L, 7L, 7L, -8L, 28L, 1L, 4607182418800017408L, 3.5E-323},
          new Object[]{325323L, 325323L, 0L, -325324L, 1301292L, 81330L, 4610334938539176755L, 1.60731E-318},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, null, null, -8L, 28L, 1L, 4607182418800017408L, 3.5E-323},
          new Object[]{325323L, 325323L, 0L, -325324L, 1301292L, 81330L, 4610334938539176755L, 1.60731E-318},
          new Object[]{0L, 0L, 0L, -1L, 0L, 0L, 0L, 0.0},
          new Object[]{null, null, null, null, null, null, null, null},
          new Object[]{null, null, null, null, null, null, null, null},
          new Object[]{null, null, null, null, null, null, null, null}
      );
    }
    testQuery(
        "SELECT\n"
            + "BITWISE_AND(l1, l2),\n"
            + "BITWISE_OR(l1, l2),\n"
            + "BITWISE_XOR(l1, l2),\n"
            + "BITWISE_COMPLEMENT(l1),\n"
            + "BITWISE_SHIFT_LEFT(l1, 2),\n"
            + "BITWISE_SHIFT_RIGHT(l1, 2),\n"
            + "BITWISE_CONVERT_DOUBLE_TO_LONG_BITS(d1),\n"
            + "BITWISE_CONVERT_LONG_BITS_TO_DOUBLE(l1)\n"
            + "FROM numfoo",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7")
                .virtualColumns(
                    expressionVirtualColumn("v0", "bitwiseAnd(\"l1\",\"l2\")", ColumnType.LONG),
                    expressionVirtualColumn("v1", "bitwiseOr(\"l1\",\"l2\")", ColumnType.LONG),
                    expressionVirtualColumn("v2", "bitwiseXor(\"l1\",\"l2\")", ColumnType.LONG),
                    expressionVirtualColumn("v3", "bitwiseComplement(\"l1\")", ColumnType.LONG),
                    expressionVirtualColumn("v4", "bitwiseShiftLeft(\"l1\",2)", ColumnType.LONG),
                    expressionVirtualColumn("v5", "bitwiseShiftRight(\"l1\",2)", ColumnType.LONG),
                    expressionVirtualColumn("v6", "bitwiseConvertDoubleToLongBits(\"d1\")", ColumnType.LONG),
                    expressionVirtualColumn("v7", "bitwiseConvertLongBitsToDouble(\"l1\")", ColumnType.DOUBLE)
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testSafeDivideExpressions()
  {
    List<Object[]> expected;
    if (useDefault) {
      expected = ImmutableList.of(
          new Object[]{0.0F, 0L, 0.0, 7.0F},
          new Object[]{1.0F, 1L, 1.0, 3253230.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F}
      );
    } else {
      expected = ImmutableList.of(
          new Object[]{null, null, null, 7.0F},
          new Object[]{1.0F, 1L, 1.0, 3253230.0F},
          new Object[]{0.0F, 0L, 0.0, 0.0F},
          new Object[]{null, null, null, null},
          new Object[]{null, null, null, null},
          new Object[]{null, null, null, null}
      );
    }
    testQuery(
        "SELECT\n"
            + "SAFE_DIVIDE(f1, f2),\n"
            + "SAFE_DIVIDE(l1, l2),\n"
            + "SAFE_DIVIDE(d2, d1),\n"
            + "SAFE_DIVIDE(l1, f1)\n"
            + "FROM numfoo",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("v0", "v1", "v2", "v3")
                .virtualColumns(
                    expressionVirtualColumn("v0", "safe_divide(\"f1\",\"f2\")", ColumnType.FLOAT),
                    expressionVirtualColumn("v1", "safe_divide(\"l1\",\"l2\")", ColumnType.LONG),
                    expressionVirtualColumn("v2", "safe_divide(\"d2\",\"d1\")", ColumnType.DOUBLE),
                    expressionVirtualColumn("v3", "safe_divide(\"l1\",\"f1\")", ColumnType.FLOAT)
                )
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        expected
    );
  }

  @Test
  public void testExplainSelectConstantExpression()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();
    final String query = "EXPLAIN PLAN FOR SELECT 1 + 1";
    final String explanation = "[{"
                               + "\"query\":{\"queryType\":\"scan\","
                               + "\"dataSource\":{\"type\":\"inline\",\"columnNames\":[\"EXPR$0\"],\"columnTypes\":[\"LONG\"],\"rows\":[[2]]},"
                               + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                               + "\"resultFormat\":\"compactedList\","
                               + "\"columns\":[\"EXPR$0\"],"
                               + "\"legacy\":false,"
                               + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                               + "\"granularity\":{\"type\":\"all\"}},"
                               + "\"signature\":[{\"name\":\"EXPR$0\",\"type\":\"LONG\"}]"
                               + "}]";
    final String legacyExplanation = "DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"inline\",\"columnNames\":[\"EXPR$0\"],\"columnTypes\":[\"LONG\"],\"rows\":[[2]]},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"EXPR$0\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"granularity\":{\"type\":\"all\"}}], signature=[{EXPR$0:LONG}])\n";
    final String resources = "[]";

    testQuery(
        PlannerConfig.builder().useNativeQueryExplain(false).build(),
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                legacyExplanation,
                resources
            }
        )
    );
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                explanation,
                resources
            }
        )
    );
  }

  @Test
  public void testSelectStarWithDimFilter()
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
            new Object[]{timestamp("2000-01-01"), "", "a", "[\"a\",\"b\"]", 1L, 1.0f, 1.0d, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), "1", "a", "", 1L, 4.0f, 4.0d, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), "def", "abc", NULL_STRING, 1L, 5.0f, 5.0d, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectDistinctWithCascadeExtractionFilter()
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
  public void testSelectDistinctWithStrlenFilter()
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
                    expressionVirtualColumn("v0", "strlen(\"dim1\")", ColumnType.LONG),
                    // The two layers of CASTs here are unusual, they should really be collapsed into one
                    expressionVirtualColumn(
                        "v1",
                        "CAST(CAST(strlen(\"dim1\"), 'STRING'), 'LONG')",
                        ColumnType.LONG
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
  public void testSelectDistinctWithLimit()
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
  public void testSelectDistinctWithSortAsOuterQuery()
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
  public void testSelectDistinctWithSortAsOuterQuery2()
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
  public void testSelectDistinctWithSortAsOuterQuery3()
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
  public void testSelectNonAggregatingWithLimitLiterallyZero()
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT dim2 FROM druid.foo ORDER BY dim2 LIMIT 0",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(),
                        RowSignature.builder().add("dim2", ColumnType.STRING).build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectNonAggregatingWithLimitReducedToZero()
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(),
                        RowSignature.builder().add("dim2", ColumnType.STRING).build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectAggregatingWithLimitReducedToZero()
  {
    // Query reduces to LIMIT 0.

    testQuery(
        "SELECT * FROM (SELECT DISTINCT dim2 FROM druid.foo ORDER BY dim2 LIMIT 2 OFFSET 5) OFFSET 2",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(),
                        RowSignature.builder().add("dim2", ColumnType.STRING).build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim2")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectCurrentTimeAndDateLosAngeles()
  {
    final Map<String, Object> context = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    context.put(PlannerContext.CTX_SQL_CURRENT_TIMESTAMP, "2000-01-01T00:00:00.123Z");
    context.put(PlannerContext.CTX_SQL_TIME_ZONE, LOS_ANGELES);

    DateTimeZone timeZone = DateTimes.inferTzFromString(LOS_ANGELES);
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        context,
        "SELECT "
        + "CURRENT_TIMESTAMP, "
        + "CURRENT_TIMESTAMP(0), "
        + "CURRENT_TIMESTAMP(1), "
        + "CURRENT_TIMESTAMP(2), "
        + "CURRENT_TIMESTAMP(3), "
        + "CURRENT_DATE, "
        + "CURRENT_DATE + INTERVAL '1' DAY",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                .dataSource(
                    InlineDataSource.fromIterable(
                        ImmutableList.of(
                            new Object[]{
                                // milliseconds of timestamps as if they were in UTC. This looks strange
                                // but intentional because they are what Calcite gives us.
                                // See DruidLogicalValuesRule.getValueFromLiteral()
                                // and Calcites.calciteDateTimeLiteralToJoda.
                                new DateTime("2000-01-01T00:00:00.123Z", timeZone)
                                    .withZone(DateTimeZone.UTC).getMillis(),
                                new DateTime("2000-01-01T00:00:00.000Z", timeZone)
                                    .withZone(DateTimeZone.UTC).getMillis(),
                                new DateTime("2000-01-01T00:00:00.100Z", timeZone)
                                    .withZone(DateTimeZone.UTC).getMillis(),
                                new DateTime("2000-01-01T00:00:00.120Z", timeZone)
                                    .withZone(DateTimeZone.UTC).getMillis(),
                                new DateTime("2000-01-01T00:00:00.123Z", timeZone)
                                    .withZone(DateTimeZone.UTC).getMillis(),
                                new DateTime("1999-12-31", timeZone).withZone(DateTimeZone.UTC).getMillis(),
                                new DateTime("2000-01-01", timeZone).withZone(DateTimeZone.UTC).getMillis()
                            }
                        ),
                        RowSignature.builder()
                            .add("CURRENT_TIMESTAMP", ColumnType.LONG)
                            .add("EXPR$1", ColumnType.LONG)
                            .add("EXPR$2", ColumnType.LONG)
                            .add("EXPR$3", ColumnType.LONG)
                            .add("EXPR$4", ColumnType.LONG)
                            .add("CURRENT_DATE", ColumnType.LONG)
                            .add("EXPR$6", ColumnType.LONG)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("CURRENT_DATE", "CURRENT_TIMESTAMP", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4", "EXPR$6")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .context(context)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-01T00:00:00.123Z", LOS_ANGELES),
                timestamp("2000-01-01T00:00:00.000Z", LOS_ANGELES),
                timestamp("2000-01-01T00:00:00.100Z", LOS_ANGELES),
                timestamp("2000-01-01T00:00:00.120Z", LOS_ANGELES),
                timestamp("2000-01-01T00:00:00.123Z", LOS_ANGELES),
                day("1999-12-31"),
                day("2000-01-01")
            }
        )
    );
  }

  @Test
  public void testSelectCurrentTimePrecisionTooHigh()
  {
    testQueryThrows(
        "SELECT CURRENT_TIMESTAMP(4)",
        expectedException -> {
          expectedException.expect(SqlPlanningException.class);
          expectedException.expectMessage(
              "Argument to function 'CURRENT_TIMESTAMP' must be a valid precision between '0' and '3'"
          );
        }
    );
  }

  @Test
  public void testSelectCountStar()
  {
    // timeseries with all granularity have a single group, so should return default results for given aggregators
    // which for count is 0 and sum is null in sql compatible mode or 0.0 in default mode.
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
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
            .context(QUERY_CONTEXT_DEFAULT)
            .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
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
            .context(QUERY_CONTEXT_DEFAULT)
            .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

    // this behavior was not always correct, so make sure legacy behavior can be retained by skipping empty buckets
    // explicitly in the context which causes these timeseries queries to return no results
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        TIMESERIES_CONTEXT_BY_GRAN,
        "SELECT COUNT(*) FROM foo WHERE dim1 = 'nonexistent'",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
            .dataSource(CalciteTests.DATASOURCE1)
            .intervals(querySegmentSpec(Filtration.eternity()))
            .filters(selector("dim1", "nonexistent", null))
            .granularity(Granularities.ALL)
            .aggregators(aggregators(
                new CountAggregatorFactory("a0")
            ))
            .context(TIMESERIES_CONTEXT_BY_GRAN)
            .build()),
        ImmutableList.of()
    );

    // timeseries with a granularity is grouping by the time expression, so matching nothing returns no results
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
            .context(getTimeseriesContextWithFloorTime(TIMESERIES_CONTEXT_BY_GRAN, "d0"))
            .build()),
        ImmutableList.of()
    );
  }

  @Test
  public void testSelectStarFromLookup()
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
  public void testSelectStar()
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
            new Object[]{timestamp("2000-01-01"), "", "a", "[\"a\",\"b\"]", 1L, 1f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), "10.1", NULL_STRING, "[\"b\",\"c\"]", 1L, 2f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), "2", "", "d", 1L, 3f, 3.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), "1", "a", "", 1L, 4f, 4.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), "def", "abc", NULL_STRING, 1L, 5f, 5.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-03"), "abc", NULL_STRING, NULL_STRING, 1L, 6f, 6.0, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectStarOnForbiddenTable()
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
                "forbidden",
                "abcd",
                1L,
                9999.0f,
                NullHandling.defaultDoubleValue(),
                "\"AQAAAQAAAALFBA==\""
            },
            new Object[]{
                timestamp("2000-01-02"),
                "forbidden",
                "a",
                1L,
                1234.0f,
                NullHandling.defaultDoubleValue(),
                "\"AQAAAQAAAALFBA==\""
            }
        )
    );
  }

  @Test
  public void testSelectStarOnForbiddenView()
  {
    assertQueryIsForbidden(
        "SELECT * FROM view.forbiddenView",
        CalciteTests.REGULAR_USER_AUTH_RESULT
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM view.forbiddenView",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim1\", 0, 1)", ColumnType.STRING),
                    expressionVirtualColumn("v1", "'a'", ColumnType.STRING)
                )
                .filters(selector("dim2", "a", null))
                .columns("__time", "v0", "v1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-01"),
                NullHandling.defaultStringValue(),
                "a"
            },
            new Object[]{
                timestamp("2001-01-01"),
                "1",
                "a"
            }
        )
    );
  }

  @Test
  public void testSelectStarOnRestrictedView()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM view.restrictedView",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .filters(selector("dim2", "a", null))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "dim1", "dim2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-02"),
                "forbidden",
                "a",
                1234.0f
            }
        )
    );

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        "SELECT * FROM view.restrictedView",
        CalciteTests.SUPER_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.FORBIDDEN_DATASOURCE)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(selector("dim2", "a", null))
                .columns("__time", "dim1", "dim2", "m1")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{
                timestamp("2000-01-02"),
                "forbidden",
                "a",
                1234.0f
            }
        )
    );
  }

  @Test
  public void testUnqualifiedTableName()
  {
    testQuery(
        "SELECT COUNT(*) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        )
    );
  }

  @Test
  public void testExplainSelectStar()
  {
    // Skip vectorization since otherwise the "context" will change for each subtest.
    skipVectorize();

    final String query = "EXPLAIN PLAN FOR SELECT * FROM druid.foo";
    final String legacyExplanation = "DruidQueryRel(query=[{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},\"resultFormat\":\"compactedList\",\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],\"legacy\":false,\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},\"granularity\":{\"type\":\"all\"}}], signature=[{__time:LONG, dim1:STRING, dim2:STRING, dim3:STRING, cnt:LONG, m1:FLOAT, m2:DOUBLE, unique_dim1:COMPLEX<hyperUnique>}])\n";
    final String explanation = "[{"
                               + "\"query\":{\"queryType\":\"scan\","
                               + "\"dataSource\":{\"type\":\"table\",\"name\":\"foo\"},"
                               + "\"intervals\":{\"type\":\"intervals\",\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                               + "\"resultFormat\":\"compactedList\","
                               + "\"columns\":[\"__time\",\"cnt\",\"dim1\",\"dim2\",\"dim3\",\"m1\",\"m2\",\"unique_dim1\"],"
                               + "\"legacy\":false,"
                               + "\"context\":{\"defaultTimeout\":300000,\"maxScatterGatherBytes\":9223372036854775807,\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\",\"vectorize\":\"false\",\"vectorizeVirtualColumns\":\"false\"},"
                               + "\"granularity\":{\"type\":\"all\"}},"
                               + "\"signature\":[{\"name\":\"__time\",\"type\":\"LONG\"},{\"name\":\"dim1\",\"type\":\"STRING\"},{\"name\":\"dim2\",\"type\":\"STRING\"},{\"name\":\"dim3\",\"type\":\"STRING\"},{\"name\":\"cnt\",\"type\":\"LONG\"},{\"name\":\"m1\",\"type\":\"FLOAT\"},{\"name\":\"m2\",\"type\":\"DOUBLE\"},{\"name\":\"unique_dim1\",\"type\":\"COMPLEX<hyperUnique>\"}]"
                               + "}]";
    final String resources = "[{\"name\":\"foo\",\"type\":\"DATASOURCE\"}]";

    testQuery(
        PlannerConfig.builder().useNativeQueryExplain(false).build(),
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                legacyExplanation,
                resources
            }
        )
    );
    testQuery(
        PLANNER_CONFIG_NATIVE_QUERY_EXPLAIN,
        query,
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{
                explanation,
                resources
            }
        )
    );
  }

  @Test
  public void testSelectStarWithLimit()
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
            new Object[]{timestamp("2000-01-01"), "", "a", "[\"a\",\"b\"]", 1L, 1.0f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), "10.1", NULL_STRING, "[\"b\",\"c\"]", 1L, 2.0f, 2.0, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectStarWithLimitAndOffset()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT_NO_COMPLEX_SERDE,
        QUERY_CONTEXT_DEFAULT,
        "SELECT * FROM druid.foo LIMIT 2 OFFSET 1",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("__time", "cnt", "dim1", "dim2", "dim3", "m1", "m2", "unique_dim1")
                .offset(1)
                .limit(2)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{timestamp("2000-01-02"), "10.1", NULL_STRING, "[\"b\",\"c\"]", 1L, 2.0f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), "2", "", "d", 1L, 3f, 3.0, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectWithProjection()
  {
    testQuery(
        "SELECT SUBSTRING(dim2, 1, 1) FROM druid.foo LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim2\", 0, 1)", ColumnType.STRING)
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
  public void testSelectWithExpressionFilter()
  {
    testQuery(
        "SELECT dim1 FROM druid.foo WHERE m1 + 1 = 7",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "(\"m1\" + 1)", ColumnType.FLOAT)
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
  public void testSelectStarWithLimitTimeDescending()
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
            new Object[]{timestamp("2001-01-03"), "abc", NULL_STRING, NULL_STRING, 1L, 6f, 6d, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), "def", "abc", NULL_STRING, 1L, 5f, 5d, HLLC_STRING}
        )
    );
  }

  @Test
  public void testSelectStarWithoutLimitTimeAscending()
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
            new Object[]{timestamp("2000-01-01"), "", "a", "[\"a\",\"b\"]", 1L, 1f, 1.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-02"), "10.1", NULL_STRING, "[\"b\",\"c\"]", 1L, 2f, 2.0, HLLC_STRING},
            new Object[]{timestamp("2000-01-03"), "2", "", "d", 1L, 3f, 3.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-01"), "1", "a", "", 1L, 4f, 4.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-02"), "def", "abc", NULL_STRING, 1L, 5f, 5.0, HLLC_STRING},
            new Object[]{timestamp("2001-01-03"), "abc", NULL_STRING, NULL_STRING, 1L, 6f, 6.0, HLLC_STRING}
        )
    );
  }


  @Test
  public void testSelectSingleColumnTwice()
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
  public void testSelectSingleColumnWithLimitDescending()
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
  public void testSelectStarFromSelectSingleColumnWithLimitDescending()
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
  public void testSelectLimitWrapping()
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
  public void testSelectLimitWrappingOnTopOfOffset()
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC OFFSET 1",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .offset(1)
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"1"}
        )
    );
  }

  @Test
  public void testSelectLimitWrappingOnTopOfOffsetAndLowLimit()
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 1 OFFSET 1",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .offset(1)
                .limit(1)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectLimitWrappingOnTopOfOffsetAndHighLimit()
  {
    testQuery(
        "SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 10 OFFSET 1",
        OUTER_LIMIT_CONTEXT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("__time", "dim1"))
                .offset(1)
                .limit(2)
                .order(ScanQuery.Order.DESCENDING)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(OUTER_LIMIT_CONTEXT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"def"},
            new Object[]{"1"}
        )
    );
  }

  @Test
  public void testSelectLimitWrappingAgainAkaIDontReallyQuiteUnderstandCalciteQueryPlanning()
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
  public void testSelectProjectionFromSelectSingleColumnWithInnerLimitDescending()
  {
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 2)",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ColumnType.STRING))
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
  public void testSelectProjectionFromSelectSingleColumnDescending()
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
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ColumnType.STRING))
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
  public void testSelectProjectionFromSelectSingleColumnWithInnerAndOuterLimitDescending()
  {
    testQuery(
        "SELECT 'beep ' || dim1 FROM (SELECT dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) LIMIT 2",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "concat('beep ',\"dim1\")", ColumnType.STRING))
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
  public void testOrderThenLimitThenFilter()
  {
    testQuery(
        "SELECT dim1 FROM "
            + "(SELECT __time, dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) "
            + "WHERE dim1 IN ('abc', 'def')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    new QueryDataSource(
                        newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .columns(ImmutableList.of("__time", "dim1"))
                            .limit(4)
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .context(QUERY_CONTEXT_DEFAULT)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .filters(in("dim1", Arrays.asList("abc", "def"), null))
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
}
