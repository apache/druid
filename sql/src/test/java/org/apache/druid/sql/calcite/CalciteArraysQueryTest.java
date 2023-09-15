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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FilteredDataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for array functions and array types
 */
public class CalciteArraysQueryTest extends BaseCalciteQueryTest
{
  private static final Map<String, Object> QUERY_CONTEXT_UNNEST =
      ImmutableMap.<String, Object>builder()
                  .putAll(QUERY_CONTEXT_DEFAULT)
                  .put(QueryContexts.CTX_SQL_STRINGIFY_ARRAYS, false)
                  .build();

  // test some query stuffs, sort of limited since no native array column types so either need to use constructor or
  // array aggregator
  @Test
  public void testSelectConstantArrayExpressionFromTable()
  {
    notMsqCompatible();
    testQuery(
        "SELECT ARRAY[1,2] as arr, dim1 FROM foo LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "array(1,2)", ColumnType.LONG_ARRAY))
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
  public void testGroupByArrayFromCase()
  {
    cannotVectorize();
    testQuery(
        "SELECT CASE WHEN dim4 = 'a' THEN ARRAY['foo','bar','baz'] END as mv_value, count(1) from numfoo GROUP BY 1",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "case_searched((\"dim4\" == 'a'),array('foo','bar','baz'),null)",
                            ColumnType.STRING_ARRAY
                        ))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING_ARRAY))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 3L},
            new Object[]{ImmutableList.of("foo", "bar", "baz"), 3L}
        )
    );
  }

  @Test
  public void testSelectNonConstantArrayExpressionFromTable()
  {
    testQuery(
        "SELECT ARRAY[CONCAT(dim1, 'word'),'up'] as arr, dim1 FROM foo LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn(
                    "v0",
                    "array(concat(\"dim1\",'word'),'up')",
                    ColumnType.STRING_ARRAY
                ))
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
  public void testSelectNonConstantArrayExpressionFromTableForMultival()
  {
    notMsqCompatible();
    final String sql = "SELECT ARRAY[CONCAT(dim3, 'word'),'up'] as arr, dim1 FROM foo LIMIT 5";
    final Query<?> scanQuery = newScanQueryBuilder()
        .dataSource(CalciteTests.DATASOURCE1)
        .intervals(querySegmentSpec(Filtration.eternity()))
        .virtualColumns(expressionVirtualColumn("v0", "array(concat(\"dim3\",'word'),'up')", ColumnType.STRING_ARRAY))
        .columns("dim1", "v0")
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .limit(5)
        .context(QUERY_CONTEXT_DEFAULT)
        .build();

    // dim3 is a multi-valued string column, so the automatic translation will turn this
    // expression into
    //
    //    `map((dim3) -> array(concat(dim3,'word'),'up'), dim3)`
    //
    // this works, but we still translate the output into a string since that is the current output type
    // in some future this might not auto-convert to a string type (when we support grouping on arrays maybe?)

    testQuery(
        sql,
        ImmutableList.of(scanQuery),
        ImmutableList.of(
            new Object[]{"[[\"aword\",\"up\"],[\"bword\",\"up\"]]", ""},
            new Object[]{"[[\"bword\",\"up\"],[\"cword\",\"up\"]]", "10.1"},
            new Object[]{"[[\"dword\",\"up\"]]", "2"},
            new Object[]{"[[\"word\",\"up\"]]", "1"},
            useDefault ? new Object[]{"[[\"word\",\"up\"]]", "def"} : new Object[]{"[[null,\"up\"]]", "def"}
        )
    );

  }

  @Test
  public void testSomeArrayFunctionsWithScanQuery()
  {
    // Yes these outputs are strange sometimes, arrays are in a partial state of existence so end up a bit
    // stringy for now this is because virtual column selectors are coercing values back to stringish so that
    // multi-valued string dimensions can be grouped on.
    notMsqCompatible();
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{
              "",
              "a",
              "[\"a\",\"b\"]",
              7L,
              0L,
              1.0,
              0.0,
              "[\"a\",\"b\",\"c\"]",
              "[1,2,3]",
              "[1.9,2.2,4.3]",
              "[\"a\",\"b\",\"foo\"]",
              "[\"foo\",\"a\"]",
              "[1,2,7]",
              "[0,1,2]",
              "[1.2,2.2,1.0]",
              "[0.0,1.1,2.2]",
              "[\"a\",\"a\",\"b\"]",
              "[7,0]",
              "[1.0,0.0]",
              7L,
              1.0,
              7L,
              1.0
          }
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{
              "",
              "a",
              "[\"a\",\"b\"]",
              7L,
              null,
              1.0,
              null,
              "[\"a\",\"b\",\"c\"]",
              "[1,2,3]",
              "[1.9,2.2,4.3]",
              "[\"a\",\"b\",\"foo\"]",
              "[\"foo\",\"a\"]",
              "[1,2,7]",
              "[null,1,2]",
              "[1.2,2.2,1.0]",
              "[null,1.1,2.2]",
              "[\"a\",\"a\",\"b\"]",
              "[7,null]",
              "[1.0,null]",
              7L,
              1.0,
              7L,
              1.0
          }
      );
    }
    testQuery(
        "SELECT"
        + " dim1,"
        + " dim2,"
        + " dim3,"
        + " l1,"
        + " l2,"
        + " d1,"
        + " d2,"
        + " ARRAY['a', 'b', 'c'],"
        + " ARRAY[1,2,3],"
        + " ARRAY[1.9, 2.2, 4.3],"
        + " ARRAY_APPEND(dim3, 'foo'),"
        + " ARRAY_PREPEND('foo', ARRAY[dim2]),"
        + " ARRAY_APPEND(ARRAY[1,2], l1),"
        + " ARRAY_PREPEND(l2, ARRAY[1,2]),"
        + " ARRAY_APPEND(ARRAY[1.2,2.2], d1),"
        + " ARRAY_PREPEND(d2, ARRAY[1.1,2.2]),"
        + " ARRAY_CONCAT(dim2,dim3),"
        + " ARRAY_CONCAT(ARRAY[l1],ARRAY[l2]),"
        + " ARRAY_CONCAT(ARRAY[d1],ARRAY[d2]),"
        + " ARRAY_OFFSET(ARRAY[l1],0),"
        + " ARRAY_OFFSET(ARRAY[d1],0),"
        + " ARRAY_ORDINAL(ARRAY[l1],1),"
        + " ARRAY_ORDINAL(ARRAY[d1],1)"
        + " FROM druid.numfoo"
        + " LIMIT 1",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    // these report as strings even though they are not, someday this will not be so
                    expressionVirtualColumn("v0", "array('a','b','c')", ColumnType.STRING_ARRAY),
                    expressionVirtualColumn("v1", "array(1,2,3)", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn("v10", "array_concat(array(\"l1\"),array(\"l2\"))", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn(
                        "v11",
                        "array_concat(array(\"d1\"),array(\"d2\"))",
                        ColumnType.DOUBLE_ARRAY
                    ),
                    expressionVirtualColumn("v12", "array_offset(array(\"l1\"),0)", ColumnType.LONG),
                    expressionVirtualColumn("v13", "array_offset(array(\"d1\"),0)", ColumnType.DOUBLE),
                    expressionVirtualColumn("v14", "array_ordinal(array(\"l1\"),1)", ColumnType.LONG),
                    expressionVirtualColumn("v15", "array_ordinal(array(\"d1\"),1)", ColumnType.DOUBLE),
                    expressionVirtualColumn("v2", "array(1.9,2.2,4.3)", ColumnType.DOUBLE_ARRAY),
                    expressionVirtualColumn("v3", "array_append(\"dim3\",'foo')", ColumnType.STRING_ARRAY),
                    expressionVirtualColumn("v4", "array_prepend('foo',array(\"dim2\"))", ColumnType.STRING_ARRAY),
                    expressionVirtualColumn("v5", "array_append(array(1,2),\"l1\")", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn("v6", "array_prepend(\"l2\",array(1,2))", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn("v7", "array_append(array(1.2,2.2),\"d1\")", ColumnType.DOUBLE_ARRAY),
                    expressionVirtualColumn("v8", "array_prepend(\"d2\",array(1.1,2.2))", ColumnType.DOUBLE_ARRAY),
                    expressionVirtualColumn("v9", "array_concat(\"dim2\",\"dim3\")", ColumnType.STRING_ARRAY)
                )
                .columns(
                    "d1",
                    "d2",
                    "dim1",
                    "dim2",
                    "dim3",
                    "l1",
                    "l2",
                    "v0",
                    "v1",
                    "v10",
                    "v11",
                    "v12",
                    "v13",
                    "v14",
                    "v15",
                    "v2",
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v7",
                    "v8",
                    "v9"
                )
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        expectedResults,
        RowSignature.builder()
                    .add("dim1", ColumnType.STRING)
                    .add("dim2", ColumnType.STRING)
                    .add("dim3", ColumnType.STRING)
                    .add("l1", ColumnType.LONG)
                    .add("l2", ColumnType.LONG)
                    .add("d1", ColumnType.DOUBLE)
                    .add("d2", ColumnType.DOUBLE)
                    .add("EXPR$7", ColumnType.STRING_ARRAY)
                    .add("EXPR$8", ColumnType.LONG_ARRAY)
                    .add("EXPR$9", ColumnType.DOUBLE_ARRAY)
                    .add("EXPR$10", ColumnType.STRING_ARRAY)
                    .add("EXPR$11", ColumnType.STRING_ARRAY)
                    .add("EXPR$12", ColumnType.LONG_ARRAY)
                    .add("EXPR$13", ColumnType.LONG_ARRAY)
                    .add("EXPR$14", ColumnType.DOUBLE_ARRAY)
                    .add("EXPR$15", ColumnType.DOUBLE_ARRAY)
                    .add("EXPR$16", ColumnType.STRING_ARRAY)
                    .add("EXPR$17", ColumnType.LONG_ARRAY)
                    .add("EXPR$18", ColumnType.DOUBLE_ARRAY)
                    .add("EXPR$19", ColumnType.LONG)
                    .add("EXPR$20", ColumnType.DOUBLE)
                    .add("EXPR$21", ColumnType.LONG)
                    .add("EXPR$22", ColumnType.DOUBLE)
                    .build()
    );
  }

  @Test
  public void testSomeArrayFunctionsWithScanQueryNoStringify()
  {
    // when not stringifying arrays, some things are still stringified, because they are inferred to be typed as strings
    // the planner context which controls stringification of arrays does not apply to multi-valued string columns,
    // which will still always be stringified to ultimately adhere to the varchar type
    // as array support increases in the engine this will likely change since using explict array functions should
    // probably kick it into an array
    notMsqCompatible();
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{
              "",
              "a",
              "[\"a\",\"b\"]",
              Arrays.asList("a", "b", "c"),
              Arrays.asList(1L, 2L, 3L),
              Arrays.asList(1.9, 2.2, 4.3),
              Arrays.asList("a", "b", "foo"),
              Arrays.asList("foo", "a"),
              Arrays.asList(1L, 2L, 7L),
              Arrays.asList(0L, 1L, 2L),
              Arrays.asList(1.2, 2.2, 1.0),
              Arrays.asList(0.0, 1.1, 2.2),
              Arrays.asList("a", "a", "b"),
              Arrays.asList(7L, 0L),
              Arrays.asList(1.0, 0.0)
          }
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{
              "",
              "a",
              "[\"a\",\"b\"]",
              Arrays.asList("a", "b", "c"),
              Arrays.asList(1L, 2L, 3L),
              Arrays.asList(1.9, 2.2, 4.3),
              Arrays.asList("a", "b", "foo"),
              Arrays.asList("foo", "a"),
              Arrays.asList(1L, 2L, 7L),
              Arrays.asList(null, 1L, 2L),
              Arrays.asList(1.2, 2.2, 1.0),
              Arrays.asList(null, 1.1, 2.2),
              Arrays.asList("a", "a", "b"),
              Arrays.asList(7L, null),
              Arrays.asList(1.0, null)
          }
      );
    }
    testQuery(
        "SELECT"
        + " dim1,"
        + " dim2,"
        + " dim3,"
        + " ARRAY['a', 'b', 'c'],"
        + " ARRAY[1,2,3],"
        + " ARRAY[1.9, 2.2, 4.3],"
        + " ARRAY_APPEND(dim3, 'foo'),"
        + " ARRAY_PREPEND('foo', ARRAY[dim2]),"
        + " ARRAY_APPEND(ARRAY[1,2], l1),"
        + " ARRAY_PREPEND(l2, ARRAY[1,2]),"
        + " ARRAY_APPEND(ARRAY[1.2,2.2], d1),"
        + " ARRAY_PREPEND(d2, ARRAY[1.1,2.2]),"
        + " ARRAY_CONCAT(dim2,dim3),"
        + " ARRAY_CONCAT(ARRAY[l1],ARRAY[l2]),"
        + " ARRAY_CONCAT(ARRAY[d1],ARRAY[d2])"
        + " FROM druid.numfoo"
        + " LIMIT 1",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "array('a','b','c')", ColumnType.STRING_ARRAY),
                    expressionVirtualColumn("v1", "array(1,2,3)", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn("v10", "array_concat(array(\"l1\"),array(\"l2\"))", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn(
                        "v11",
                        "array_concat(array(\"d1\"),array(\"d2\"))",
                        ColumnType.DOUBLE_ARRAY
                    ),
                    expressionVirtualColumn("v2", "array(1.9,2.2,4.3)", ColumnType.DOUBLE_ARRAY),
                    expressionVirtualColumn("v3", "array_append(\"dim3\",'foo')", ColumnType.STRING_ARRAY),
                    expressionVirtualColumn("v4", "array_prepend('foo',array(\"dim2\"))", ColumnType.STRING_ARRAY),
                    expressionVirtualColumn("v5", "array_append(array(1,2),\"l1\")", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn("v6", "array_prepend(\"l2\",array(1,2))", ColumnType.LONG_ARRAY),
                    expressionVirtualColumn("v7", "array_append(array(1.2,2.2),\"d1\")", ColumnType.DOUBLE_ARRAY),
                    expressionVirtualColumn("v8", "array_prepend(\"d2\",array(1.1,2.2))", ColumnType.DOUBLE_ARRAY),
                    expressionVirtualColumn("v9", "array_concat(\"dim2\",\"dim3\")", ColumnType.STRING_ARRAY)
                )
                .columns(
                    "dim1",
                    "dim2",
                    "dim3",
                    "v0",
                    "v1",
                    "v10",
                    "v11",
                    "v2",
                    "v3",
                    "v4",
                    "v5",
                    "v6",
                    "v7",
                    "v8",
                    "v9"
                )
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                .build()
        ),
        expectedResults
    );
  }

  @Test
  public void testArrayOverlapFilter()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_OVERLAP(dim3, ARRAY['a','b']) LIMIT 5",
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
  public void testArrayOverlapFilterWithExtractionFn()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_OVERLAP(SUBSTRING(dim3, 1, 1), ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    NullHandling.sqlCompatible()
                    ? expressionFilter("array_overlap(substring(\"dim3\", 0, 1),array('a','b'))")
                    : new InDimFilter("dim3", ImmutableList.of("a", "b"), new SubstringDimExtractionFn(0, 1))
                )
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
  public void testArrayOverlapFilterNonLiteral()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_OVERLAP(dim3, ARRAY[dim2]) LIMIT 5",
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
            new Object[]{"[\"a\",\"b\"]"}
        )
    );
  }

  @Test
  public void testArrayContainsFilter()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(dim3, ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    and(
                        equality("dim3", "a", ColumnType.STRING),
                        equality("dim3", "b", ColumnType.STRING)
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
  public void testArrayContainsFilterWithExtractionFn()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(SUBSTRING(dim3, 1, 1), ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(
                    NullHandling.sqlCompatible()
                    ? expressionFilter("array_contains(substring(\"dim3\", 0, 1),array('a','b'))")
                    : and(
                        selector("dim3", "a", new SubstringDimExtractionFn(0, 1)),
                        selector("dim3", "b", new SubstringDimExtractionFn(0, 1))
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
  public void testArrayContainsArrayOfOneElement()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(dim3, ARRAY['a']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(equality("dim3", "a", ColumnType.STRING))
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
  public void testArrayContainsArrayOfNonLiteral()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(dim3, ARRAY[dim2]) LIMIT 5",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .filters(expressionFilter("array_contains(\"dim3\",array(\"dim2\"))"))
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]"}
        )
    );
  }

  @Test
  public void testArraySlice()
  {
    testQuery(
        "SELECT ARRAY_SLICE(dim3, 1) FROM druid.numfoo",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "array_slice(\"dim3\",1)", ColumnType.STRING_ARRAY))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{Collections.singletonList("b")},
            new Object[]{Collections.singletonList("c")},
            new Object[]{Collections.emptyList()},
            new Object[]{useDefault ? null : Collections.emptyList()},
            new Object[]{null},
            new Object[]{null}
        )
    );
  }

  @Test
  public void testArrayLength()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT dim1, ARRAY_LENGTH(dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1, 2 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "array_length(\"dim3\")", ColumnType.LONG))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim1", "_d0", ColumnType.STRING),
                                new DefaultDimensionSpec("v0", "_d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "_d1",
                                OrderByColumnSpec.Direction.DESCENDING,
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
            useDefault ? new Object[]{"2", 1, 1L} : new Object[]{"1", 1, 1L},
            useDefault ? new Object[]{"1", 0, 1L} : new Object[]{"2", 1, 1L},
            new Object[]{"abc", useDefault ? 0 : null, 1L},
            new Object[]{"def", useDefault ? 0 : null, 1L}
        )
    );
  }

  @Test
  public void testArrayAppend()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{ImmutableList.of("a", "b", "foo"), 1L},
          new Object[]{ImmutableList.of("b", "c", "foo"), 1L},
          new Object[]{ImmutableList.of("d", "foo"), 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null, 2L},
          new Object[]{ImmutableList.of("", "foo"), 1L},
          new Object[]{ImmutableList.of("a", "b", "foo"), 1L},
          new Object[]{ImmutableList.of("b", "c", "foo"), 1L},
          new Object[]{ImmutableList.of("d", "foo"), 1L}
      );
    }
    testQuery(
        "SELECT ARRAY_APPEND(dim3, 'foo'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_append(\"dim3\",'foo')",
                            ColumnType.STRING_ARRAY
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testArrayPrepend()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{ImmutableList.of("foo", "a", "b"), 1L},
          new Object[]{ImmutableList.of("foo", "b", "c"), 1L},
          new Object[]{ImmutableList.of("foo", "d"), 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null, 2L},
          new Object[]{ImmutableList.of("foo", ""), 1L},
          new Object[]{ImmutableList.of("foo", "a", "b"), 1L},
          new Object[]{ImmutableList.of("foo", "b", "c"), 1L},
          new Object[]{ImmutableList.of("foo", "d"), 1L}
      );
    }
    testQuery(
        "SELECT ARRAY_PREPEND('foo', dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_prepend('foo',\"dim3\")",
                            ColumnType.STRING_ARRAY
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testArrayPrependAppend()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"", "", 3L},
          new Object[]{"foo,a,b", "a,b,foo", 1L},
          new Object[]{"foo,b,c", "b,c,foo", 1L},
          new Object[]{"foo,d", "d,foo", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null, null, 2L},
          new Object[]{"foo,", ",foo", 1L},
          new Object[]{"foo,a,b", "a,b,foo", 1L},
          new Object[]{"foo,b,c", "b,c,foo", 1L},
          new Object[]{"foo,d", "d,foo", 1L}
      );
    }
    testQuery(
        "SELECT ARRAY_TO_STRING(ARRAY_PREPEND('foo', dim3), ','), ARRAY_TO_STRING(ARRAY_APPEND(dim3, 'foo'), ','), SUM(cnt) FROM druid.numfoo GROUP BY 1,2 ORDER BY 3 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "array_to_string(array_prepend('foo',\"dim3\"),',')",
                                ColumnType.STRING
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "array_to_string(array_append(\"dim3\",'foo'),',')",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING),
                                new DefaultDimensionSpec("v1", "_d1", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
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
  public void testArrayConcat()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{null, 3L},
          new Object[]{ImmutableList.of("a", "b", "a", "b"), 1L},
          new Object[]{ImmutableList.of("b", "c", "b", "c"), 1L},
          new Object[]{ImmutableList.of("d", "d"), 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{null, 2L},
          new Object[]{ImmutableList.of("", ""), 1L},
          new Object[]{ImmutableList.of("a", "b", "a", "b"), 1L},
          new Object[]{ImmutableList.of("b", "c", "b", "c"), 1L},
          new Object[]{ImmutableList.of("d", "d"), 1L}
      );
    }
    testQuery(
        "SELECT ARRAY_CONCAT(dim3, dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_concat(\"dim3\",\"dim3\")",
                            ColumnType.STRING_ARRAY
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        results
    );
  }

  @Test
  public void testArrayOffset()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT ARRAY_OFFSET(dim3, 1), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "array_offset(\"dim3\",1)", ColumnType.STRING))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
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
  public void testArrayGroupAsLongArray()
  {
    notMsqCompatible();
    // Cannot vectorize as we donot have support in native query subsytem for grouping on arrays
    cannotVectorize();
    testQuery(
        "SELECT ARRAY[l1], SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array(\"l1\")",
                            ColumnType.LONG_ARRAY
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.LONG_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        useDefault ? ImmutableList.of(
            new Object[]{ImmutableList.of(0L), 4L},
            new Object[]{ImmutableList.of(7L), 1L},
            new Object[]{ImmutableList.of(325323L), 1L}
        ) : ImmutableList.of(
            new Object[]{Collections.singletonList(null), 3L},
            new Object[]{ImmutableList.of(0L), 1L},
            new Object[]{ImmutableList.of(7L), 1L},
            new Object[]{ImmutableList.of(325323L), 1L}
        )
    );
  }


  @Test
  public void testArrayGroupAsDoubleArray()
  {
    // Cannot vectorize as we donot have support in native query subsytem for grouping on arrays as keys
    cannotVectorize();
    notMsqCompatible();
    testQuery(
        "SELECT ARRAY[d1], SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array(\"d1\")",
                            ColumnType.DOUBLE_ARRAY
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.DOUBLE_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        useDefault ? ImmutableList.of(
            new Object[]{ImmutableList.of(0.0), 4L},
            new Object[]{ImmutableList.of(1.0), 1L},
            new Object[]{ImmutableList.of(1.7), 1L}
        ) :
        ImmutableList.of(
            new Object[]{Collections.singletonList(null), 3L},
            new Object[]{ImmutableList.of(0.0), 1L},
            new Object[]{ImmutableList.of(1.0), 1L},
            new Object[]{ImmutableList.of(1.7), 1L}
        )
    );
  }

  @Test
  public void testArrayGroupAsFloatArray()
  {
    notMsqCompatible();
    // Cannot vectorize as we donot have support in native query subsytem for grouping on arrays as keys
    cannotVectorize();
    testQuery(
        "SELECT ARRAY[f1], SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array(\"f1\")",
                            ColumnType.ofArray(ColumnType.FLOAT)
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.ofArray(ColumnType.FLOAT))
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        useDefault ? ImmutableList.of(
            new Object[]{ImmutableList.of(0.0F), 4L},
            new Object[]{ImmutableList.of(0.10000000149011612F), 1L},
            new Object[]{ImmutableList.of(1.0F), 1L}
        ) :
        ImmutableList.of(
            new Object[]{Collections.singletonList(null), 3L},
            new Object[]{ImmutableList.of(0.0F), 1L},
            new Object[]{ImmutableList.of(0.10000000149011612F), 1L},
            new Object[]{ImmutableList.of(1.0F), 1L}
        )
    );
  }

  @Test
  public void testArrayGroupAsArrayWithFunction()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();
    testQuery(
        "SELECT ARRAY[ARRAY_ORDINAL(dim3, 2)], SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array(array_ordinal(\"dim3\",2))",
                            ColumnType.STRING_ARRAY
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                                          ImmutableList.of(new OrderByColumnSpec(
                                              "a0",
                                              OrderByColumnSpec.Direction.DESCENDING,
                                              StringComparators.NUMERIC
                                          )),
                                          Integer.MAX_VALUE
                                      )
                        )
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{Collections.singletonList(null), 4L},
            new Object[]{ImmutableList.of("b"), 1L},
            new Object[]{ImmutableList.of("c"), 1L}
        )
    );
  }

  @Test
  public void testArrayOrdinal()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT ARRAY_ORDINAL(dim3, 2), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_ordinal(\"dim3\",2)",
                            ColumnType.STRING
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
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
  public void testArrayOffsetOf()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT ARRAY_OFFSET_OF(dim3, 'b'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_offset_of(\"dim3\",'b')",
                            ColumnType.LONG
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        useDefault
        ? ImmutableList.of(
            new Object[]{0, 4L},
            new Object[]{-1, 1L},
            new Object[]{1, 1L}
        )
        : ImmutableList.of(
            new Object[]{null, 4L},
            new Object[]{0, 1L},
            new Object[]{1, 1L}
        )
    );
  }

  @Test
  public void testArrayOrdinalOf()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT ARRAY_ORDINAL_OF(dim3, 'b'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_ordinal_of(\"dim3\",'b')",
                            ColumnType.LONG
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        useDefault
        ? ImmutableList.of(
            new Object[]{0, 3L},
            new Object[]{-1, 1L},
            new Object[]{1, 1L},
            new Object[]{2, 1L}
        )
        : ImmutableList.of(
            new Object[]{null, 4L},
            new Object[]{1, 1L},
            new Object[]{2, 1L}
        )
    );
  }

  @Test
  public void testArrayToString()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
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
        "SELECT ARRAY_TO_STRING(dim3, ','), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "array_to_string(\"dim3\",',')",
                            ColumnType.STRING
                        ))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
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
  public void testArrayToStringToMultiValueString()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{ImmutableList.of("a", "b", "d"), 1L},
          new Object[]{ImmutableList.of("b", "c", "d"), 1L},
          new Object[]{ImmutableList.of("d", "d"), 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{ImmutableList.of("", "d"), 1L},
          new Object[]{ImmutableList.of("a", "b", "d"), 1L},
          new Object[]{ImmutableList.of("b", "c", "d"), 1L},
          new Object[]{ImmutableList.of("d", "d"), 1L}
      );
    }
    testQuery(
        "SELECT STRING_TO_ARRAY(CONCAT(ARRAY_TO_STRING(dim3, ','), ',d'), ','), SUM(cnt) FROM druid.numfoo WHERE ARRAY_LENGTH(dim3) > 0 GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn("v0", "array_length(\"dim3\")", ColumnType.LONG),
                            expressionVirtualColumn(
                                "v1",
                                "string_to_array(concat(array_to_string(\"dim3\",','),',d'),',')",
                                ColumnType.STRING_ARRAY
                            )
                        )
                        .setDimFilter(range("v0", ColumnType.LONG, 0L, null, true, false))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "_d0", ColumnType.STRING_ARRAY)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(new DefaultLimitSpec(
                            ImmutableList.of(new OrderByColumnSpec(
                                "a0",
                                OrderByColumnSpec.Direction.DESCENDING,
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
  public void testArrayAgg()
  {
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_AGG(dim1), ARRAY_AGG(DISTINCT dim1), ARRAY_AGG(DISTINCT dim1) FILTER(WHERE dim1 = 'shazbot') FROM foo WHERE dim1 is not null",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(notNull("dim1"))
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("dim1"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"dim1\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a1",
                              ImmutableSet.of("dim1"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"dim1\")",
                              "array_set_add_all(\"__acc\", \"a1\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new FilteredAggregatorFactory(
                              new ExpressionLambdaAggregatorFactory(
                                  "a2",
                                  ImmutableSet.of("dim1"),
                                  "__acc",
                                  "ARRAY<STRING>[]",
                                  "ARRAY<STRING>[]",
                                  true,
                                  true,
                                  false,
                                  "array_set_add(\"__acc\", \"dim1\")",
                                  "array_set_add_all(\"__acc\", \"a2\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              equality("dim1", "shazbot", ColumnType.STRING)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"[\"10.1\",\"2\",\"1\",\"def\",\"abc\"]", "[\"1\",\"10.1\",\"2\",\"abc\",\"def\"]", null}
            : new Object[]{
                "[\"\",\"10.1\",\"2\",\"1\",\"def\",\"abc\"]",
                "[\"\",\"1\",\"10.1\",\"2\",\"abc\",\"def\"]",
                null
            }
        )
    );
  }

  @Test
  public void testArrayAggMultiValue()
  {
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_AGG(dim3), ARRAY_AGG(DISTINCT dim3) FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("dim3"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"dim3\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a1",
                              ImmutableSet.of("dim3"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"dim3\")",
                              "array_set_add_all(\"__acc\", \"a1\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"[\"a\",\"b\",\"b\",\"c\",\"d\",null,null,null]", "[null,\"a\",\"b\",\"c\",\"d\"]"}
            : new Object[]{"[\"a\",\"b\",\"b\",\"c\",\"d\",\"\",null,null]", "[null,\"\",\"a\",\"b\",\"c\",\"d\"]"}
        )
    );
  }

  @Test
  public void testArrayAggNumeric()
  {
    notMsqCompatible();
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_AGG(l1), ARRAY_AGG(DISTINCT l1), ARRAY_AGG(d1), ARRAY_AGG(DISTINCT d1), ARRAY_AGG(f1), ARRAY_AGG(DISTINCT f1) FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("l1"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"l1\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a1",
                              ImmutableSet.of("l1"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"l1\")",
                              "array_set_add_all(\"__acc\", \"a1\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a2",
                              ImmutableSet.of("d1"),
                              "__acc",
                              "ARRAY<DOUBLE>[]",
                              "ARRAY<DOUBLE>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"d1\")",
                              "array_concat(\"__acc\", \"a2\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a3",
                              ImmutableSet.of("d1"),
                              "__acc",
                              "ARRAY<DOUBLE>[]",
                              "ARRAY<DOUBLE>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"d1\")",
                              "array_set_add_all(\"__acc\", \"a3\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a4",
                              ImmutableSet.of("f1"),
                              "__acc",
                              "ARRAY<DOUBLE>[]",
                              "ARRAY<DOUBLE>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"f1\")",
                              "array_concat(\"__acc\", \"a4\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a5",
                              ImmutableSet.of("f1"),
                              "__acc",
                              "ARRAY<DOUBLE>[]",
                              "ARRAY<DOUBLE>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"f1\")",
                              "array_set_add_all(\"__acc\", \"a5\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{
                "[7,325323,0,0,0,0]",
                "[0,7,325323]",
                "[1.0,1.7,0.0,0.0,0.0,0.0]",
                "[0.0,1.0,1.7]",
                "[1.0,0.10000000149011612,0.0,0.0,0.0,0.0]",
                "[0.0,0.10000000149011612,1.0]"
            }
            : new Object[]{
                "[7,325323,0,null,null,null]",
                "[null,0,7,325323]",
                "[1.0,1.7,0.0,null,null,null]",
                "[null,0.0,1.0,1.7]",
                "[1.0,0.10000000149011612,0.0,null,null,null]",
                "[null,0.0,0.10000000149011612,1.0]"
            }
        )
    );
  }

  @Test
  public void testArrayAggQuantile()
  {
    notMsqCompatible();
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_QUANTILE(ARRAY_AGG(l1), 0.9) FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("l1"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"l1\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .postAggregators(
                      expressionPostAgg("p0", "array_quantile(\"a0\",0.9)")
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        // Different results because there are some nulls in the column. In SQL-compatible mode we ignore them;
        // in replace-with-default mode we treat them as zeroes.
        ImmutableList.of(new Object[]{NullHandling.sqlCompatible() ? 260259.80000000002 : 162665.0})
    );
  }

  @Test
  public void testArrayAggArrays()
  {
    notMsqCompatible();
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_AGG(ARRAY[l1, l2]), ARRAY_AGG(DISTINCT ARRAY[l1, l2]) FROM numfoo",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "array(\"l1\",\"l2\")", ColumnType.LONG_ARRAY)
                  )
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("v0"),
                              "__acc",
                              "ARRAY<ARRAY<LONG>>[]",
                              "ARRAY<ARRAY<LONG>>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"v0\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a1",
                              ImmutableSet.of("v0"),
                              "__acc",
                              "ARRAY<ARRAY<LONG>>[]",
                              "ARRAY<ARRAY<LONG>>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"v0\")",
                              "array_set_add_all(\"__acc\", \"a1\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                  .build()
        ),
        (sql, results) -> {
          // ordering is not stable in array_agg and array_concat_agg
          List<Object[]> expected = ImmutableList.of(
              useDefault ?
              new Object[]{
                  Arrays.asList(
                      Arrays.asList(7L, 0L),
                      Arrays.asList(325323L, 325323L),
                      Arrays.asList(0L, 0L),
                      Arrays.asList(0L, 0L),
                      Arrays.asList(0L, 0L),
                      Arrays.asList(0L, 0L)
                  ),
                  Arrays.asList(
                      Arrays.asList(0L, 0L),
                      Arrays.asList(7L, 0L),
                      Arrays.asList(325323L, 325323L)
                  )
              }
                         :
              new Object[]{
                  Arrays.asList(
                      Arrays.asList(7L, null),
                      Arrays.asList(325323L, 325323L),
                      Arrays.asList(0L, 0L),
                      Arrays.asList(null, null),
                      Arrays.asList(null, null),
                      Arrays.asList(null, null)
                  ),
                  Arrays.asList(
                      Arrays.asList(null, null),
                      Arrays.asList(0L, 0L),
                      Arrays.asList(7L, null),
                      Arrays.asList(325323L, 325323L)
                  )
              }
          );
          assertResultsDeepEquals(sql, expected, results);
        }
    );
  }

  @Test
  public void testArrayConcatAggArrays()
  {
    notMsqCompatible();
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_CONCAT_AGG(ARRAY[l1, l2]), ARRAY_CONCAT_AGG(DISTINCT ARRAY[l1, l2]) FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "array(\"l1\",\"l2\")", ColumnType.LONG_ARRAY)
                  )
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("v0"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              false,
                              false,
                              "array_concat(\"__acc\", \"v0\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a1",
                              ImmutableSet.of("v0"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              false,
                              false,
                              "array_set_add_all(\"__acc\", \"v0\")",
                              "array_set_add_all(\"__acc\", \"a1\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"[7,0,325323,325323,0,0,0,0,0,0,0,0]", "[0,7,325323]"}
            : new Object[]{"[7,null,325323,325323,0,0,null,null,null,null,null,null]", "[null,0,7,325323]"}
        )
    );
  }

  @Test
  public void testArrayAggToString()
  {
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_TO_STRING(ARRAY_AGG(DISTINCT dim1), ',') FROM foo WHERE dim1 is not null",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(notNull("dim1"))
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("dim1"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"dim1\")",
                              "array_set_add_all(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .postAggregators(expressionPostAgg("p0", "array_to_string(\"a0\",',')"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{"1,10.1,2,abc,def"} : new Object[]{",1,10.1,2,abc,def"}
        )
    );
  }

  @Test
  public void testArrayAggExpression()
  {
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_TO_STRING(ARRAY_AGG(DISTINCT CONCAT(dim1, dim2)), ',') FROM foo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "concat(\"dim1\",\"dim2\")", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("v0"),
                              "__acc",
                              "ARRAY<STRING>[]",
                              "ARRAY<STRING>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"v0\")",
                              "array_set_add_all(\"__acc\", \"a0\")",
                              null,
                              null,
                              ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .postAggregators(expressionPostAgg("p0", "array_to_string(\"a0\",',')"))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault ? new Object[]{"10.1,1a,2,a,abc,defabc"} : new Object[]{"null,1a,2,a,defabc"}
        )
    );
  }

  @Test
  public void testArrayAggMaxBytes()
  {
    cannotVectorize();
    notMsqCompatible();
    testQuery(
        "SELECT ARRAY_AGG(l1, 128), ARRAY_AGG(DISTINCT l1, 128) FROM numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("l1"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              true,
                              false,
                              "array_append(\"__acc\", \"l1\")",
                              "array_concat(\"__acc\", \"a0\")",
                              null,
                              null,
                              new HumanReadableBytes(128),
                              TestExprMacroTable.INSTANCE
                          ),
                          new ExpressionLambdaAggregatorFactory(
                              "a1",
                              ImmutableSet.of("l1"),
                              "__acc",
                              "ARRAY<LONG>[]",
                              "ARRAY<LONG>[]",
                              true,
                              true,
                              false,
                              "array_set_add(\"__acc\", \"l1\")",
                              "array_set_add_all(\"__acc\", \"a1\")",
                              null,
                              null,
                              new HumanReadableBytes(128),
                              TestExprMacroTable.INSTANCE
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"[7,325323,0,0,0,0]", "[0,7,325323]"}
            : new Object[]{"[7,325323,0,null,null,null]", "[null,0,7,325323]"}
        )
    );
  }

  @Test
  public void testArrayAggAsArrayFromJoin()
  {
    cannotVectorize();
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{"a", "[\"10.1\",\"2\"]", "10.1,2"},
          new Object[]{"a", "[\"10.1\",\"2\"]", "10.1,2"},
          new Object[]{"a", "[\"10.1\",\"2\"]", "10.1,2"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{"a", "[\"\",\"10.1\",\"2\"]", ",10.1,2"},
          new Object[]{"a", "[\"\",\"10.1\",\"2\"]", ",10.1,2"},
          new Object[]{"a", "[\"\",\"10.1\",\"2\"]", ",10.1,2"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"}
      );
    }
    testQuery(
        "SELECT numfoo.dim4, j.arr, ARRAY_TO_STRING(j.arr, ',') FROM numfoo INNER JOIN (SELECT dim4, ARRAY_AGG(DISTINCT dim1) as arr FROM numfoo WHERE dim1 is not null GROUP BY 1) as j ON numfoo.dim4 = j.dim4",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          new QueryDataSource(
                              GroupByQuery.builder()
                                          .setDataSource(CalciteTests.DATASOURCE3)
                                          .setInterval(querySegmentSpec(Filtration.eternity()))
                                          .setGranularity(Granularities.ALL)
                                          .setDimFilter(notNull("dim1"))
                                          .setDimensions(new DefaultDimensionSpec("dim4", "_d0"))
                                          .setAggregatorSpecs(
                                              aggregators(
                                                  new ExpressionLambdaAggregatorFactory(
                                                      "a0",
                                                      ImmutableSet.of("dim1"),
                                                      "__acc",
                                                      "ARRAY<STRING>[]",
                                                      "ARRAY<STRING>[]",
                                                      true,
                                                      true,
                                                      false,
                                                      "array_set_add(\"__acc\", \"dim1\")",
                                                      "array_set_add_all(\"__acc\", \"a0\")",
                                                      null,
                                                      null,
                                                      ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                                      TestExprMacroTable.INSTANCE
                                                  )
                                              )
                                          )
                                          .setContext(QUERY_CONTEXT_DEFAULT)
                                          .build()
                          ),
                          "j0.",
                          "(\"dim4\" == \"j0._d0\")",
                          JoinType.INNER,
                          null
                      )
                  )
                  .virtualColumns(
                      expressionVirtualColumn("v0", "array_to_string(\"j0.a0\",',')", ColumnType.STRING)
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("dim4", "j0.a0", "v0")
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()

        ),
        expectedResults
    );
  }

  @Test
  public void testArrayAggGroupByArrayAggFromSubquery()
  {
    cannotVectorize();

    testQuery(
        "SELECT dim2, arr, COUNT(*) FROM (SELECT dim2, ARRAY_AGG(DISTINCT dim1) as arr FROM foo WHERE dim1 is not null GROUP BY 1 LIMIT 5) GROUP BY 1,2",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .dimension(new DefaultDimensionSpec(
                    "dim2",
                    "d0",
                    ColumnType.STRING
                ))
                .metric(new DimensionTopNMetricSpec(
                    null,
                    StringComparators.LEXICOGRAPHIC
                ))
                .filters(notNull("dim1"))
                .threshold(5)
                .aggregators(new ExpressionLambdaAggregatorFactory(
                    "a0",
                    ImmutableSet.of("dim1"),
                    "__acc",
                    "ARRAY<STRING>[]",
                    "ARRAY<STRING>[]",
                    true,
                    true,
                    false,
                    "array_set_add(\"__acc\", \"dim1\")",
                    "array_set_add_all(\"__acc\", \"a0\")",
                    null,
                    null,
                    new HumanReadableBytes(1024),
                    ExprMacroTable.nil()
                ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .granularity(Granularities.ALL)
                .context(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                .postAggregators(new ExpressionPostAggregator("s0", "1", null, ExprMacroTable.nil()))
                .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"", ImmutableList.of("10.1", "2", "abc"), 1L},
            new Object[]{"a", ImmutableList.of("1"), 1L},
            new Object[]{"abc", ImmutableList.of("def"), 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, ImmutableList.of("10.1", "abc"), 1L},
            new Object[]{"", ImmutableList.of("2"), 1L},
            new Object[]{"a", ImmutableList.of("", "1"), 1L},
            new Object[]{"abc", ImmutableList.of("def"), 1L}
        )
    );
  }

  @Test
  public void testArrayAggGroupByArrayAggOfLongsFromSubquery()
  {
    notMsqCompatible();
    requireMergeBuffers(3);
    cannotVectorize();
    testQuery(
        "select cntarray, count(*) from ( select dim1, dim2, ARRAY_AGG(cnt) as cntarray from ( select dim1, dim2, dim3, count(*) as cnt from foo group by 1, 2, 3 ) group by 1, 2 ) group by 1",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(new QueryDataSource(
                                            GroupByQuery.builder()
                                                        .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                                        .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                                                        .setGranularity(Granularities.ALL)
                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                                                        .setDimensions(
                                                            new DefaultDimensionSpec("dim1", "d0"),
                                                            new DefaultDimensionSpec("dim2", "d1"),
                                                            new DefaultDimensionSpec("dim3", "d2"
                                                            )
                                                        )
                                                        .setAggregatorSpecs(
                                                            new CountAggregatorFactory("a0"))
                                                        .build()))
                                        .setQuerySegmentSpec(
                                            querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            new DefaultDimensionSpec(
                                                "d0",
                                                "_d0"
                                            ),
                                            new DefaultDimensionSpec(
                                                "d1",
                                                "_d1"
                                            )
                                        )
                                        .setAggregatorSpecs(new ExpressionLambdaAggregatorFactory(
                                            "_a0",
                                            ImmutableSet.of("a0"),
                                            "__acc",
                                            "ARRAY<LONG>[]",
                                            "ARRAY<LONG>[]",
                                            true,
                                            true,
                                            false,
                                            "array_append(\"__acc\", \"a0\")",
                                            "array_concat(\"__acc\", \"_a0\")",
                                            null,
                                            null,
                                            new HumanReadableBytes(1024),
                                            ExprMacroTable.nil()
                                        ))
                                        .build()))
                        .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .setDimensions(new DefaultDimensionSpec("_a0", "d0", ColumnType.LONG_ARRAY))
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{ImmutableList.of(1L), 4L},
            new Object[]{ImmutableList.of(1L, 1L), 2L}
        )
    );
  }

  @Test
  public void testArrayAggGroupByArrayAggOfStringsFromSubquery()
  {
    requireMergeBuffers(3);
    cannotVectorize();
    testQuery(
        "select cntarray, count(*) from ( select dim1, dim2, ARRAY_AGG(cnt) as cntarray from ( select dim1, dim2, dim3, cast( count(*) as VARCHAR ) as cnt from foo group by 1, 2, 3 ) group by 1, 2 ) group by 1",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(new QueryDataSource(
                            GroupByQuery.builder()
                                        .setDataSource(new QueryDataSource(
                                            GroupByQuery.builder()
                                                        .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                                        .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                                                        .setGranularity(Granularities.ALL)
                                                        .setInterval(querySegmentSpec(Filtration.eternity()))
                                                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                                                        .setDimensions(
                                                            new DefaultDimensionSpec("dim1", "d0"),
                                                            new DefaultDimensionSpec("dim2", "d1"),
                                                            new DefaultDimensionSpec("dim3", "d2"
                                                            )
                                                        )
                                                        .setAggregatorSpecs(
                                                            new CountAggregatorFactory("a0"))
                                                        .build()))
                                        .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                                        .setGranularity(Granularities.ALL)
                                        .setDimensions(
                                            new DefaultDimensionSpec("d0", "_d0"),
                                            new DefaultDimensionSpec("d1", "_d1")
                                        )
                                        .setAggregatorSpecs(new ExpressionLambdaAggregatorFactory(
                                            "_a0",
                                            ImmutableSet.of("a0"),
                                            "__acc",
                                            "ARRAY<STRING>[]",
                                            "ARRAY<STRING>[]",
                                            true,
                                            true,
                                            false,
                                            "array_append(\"__acc\", \"a0\")",
                                            "array_concat(\"__acc\", \"_a0\")",
                                            null,
                                            null,
                                            new HumanReadableBytes(1024),
                                            ExprMacroTable.nil()
                                        ))
                                        .build()))
                        .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .setDimensions(new DefaultDimensionSpec("_a0", "d0", ColumnType.STRING_ARRAY))
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .build()
        ),
        ImmutableList.of(
            new Object[]{ImmutableList.of("1"), 4L},
            new Object[]{ImmutableList.of("1", "1"), 2L}
        )
    );
  }

  @Test
  public void testArrayAggGroupByArrayAggOfDoubleFromSubquery()
  {
    notMsqCompatible();
    requireMergeBuffers(3);
    cannotVectorize();
    testQuery(
        "select cntarray, count(*) from ( select dim1, dim2, ARRAY_AGG(cnt) as cntarray from ( select dim1, dim2, dim3, cast( count(*) as DOUBLE ) as cnt from foo group by 1, 2, 3 ) group by 1, 2 ) group by 1",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery
                .builder()
                .setDataSource(new QueryDataSource(
                    GroupByQuery.builder()
                                .setDataSource(new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(new TableDataSource(CalciteTests.DATASOURCE1))
                                                .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                                                .setGranularity(Granularities.ALL)
                                                .setInterval(querySegmentSpec(Filtration.eternity()))
                                                .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                                                .setDimensions(
                                                    new DefaultDimensionSpec("dim1", "d0"),
                                                    new DefaultDimensionSpec("dim2", "d1"),
                                                    new DefaultDimensionSpec("dim3", "d2"
                                                    )
                                                )
                                                .setAggregatorSpecs(
                                                    new CountAggregatorFactory("a0"))
                                                .build()))
                                .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                                .setGranularity(Granularities.ALL)
                                .setDimensions(
                                    new DefaultDimensionSpec("d0", "_d0"),
                                    new DefaultDimensionSpec("d1", "_d1")
                                )
                                .setAggregatorSpecs(new ExpressionLambdaAggregatorFactory(
                                    "_a0",
                                    ImmutableSet.of("a0"),
                                    "__acc",
                                    "ARRAY<DOUBLE>[]",
                                    "ARRAY<DOUBLE>[]",
                                    true,
                                    true,
                                    false,
                                    "array_append(\"__acc\", \"a0\")",
                                    "array_concat(\"__acc\", \"_a0\")",
                                    null,
                                    null,
                                    new HumanReadableBytes(1024),
                                    ExprMacroTable.nil()
                                ))
                                .build()))
                .setQuerySegmentSpec(querySegmentSpec(Filtration.eternity()))
                .setGranularity(Granularities.ALL)
                .setInterval(querySegmentSpec(Filtration.eternity()))
                .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                .setDimensions(new DefaultDimensionSpec("_a0", "d0", ColumnType.DOUBLE_ARRAY))
                .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                .build()
        ),
        ImmutableList.of(
            new Object[]{ImmutableList.of(1.0), 4L},
            new Object[]{ImmutableList.of(1.0, 1.0), 2L}
        )
    );
  }

  @Test
  public void testArrayAggArrayContainsSubquery()
  {
    cannotVectorize();
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{"10.1", ""},
          new Object[]{"2", ""},
          new Object[]{"1", "a"},
          new Object[]{"def", "abc"},
          new Object[]{"abc", ""}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{"", "a"},
          new Object[]{"10.1", null},
          new Object[]{"2", ""},
          new Object[]{"1", "a"},
          new Object[]{"def", "abc"},
          new Object[]{"abc", null}
      );
    }
    testQuery(
        "SELECT dim1,dim2 FROM foo WHERE ARRAY_CONTAINS((SELECT ARRAY_AGG(DISTINCT dim1) FROM foo WHERE dim1 is not null), dim1)",
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE1),
                          new QueryDataSource(
                              Druids.newTimeseriesQueryBuilder()
                                    .dataSource(CalciteTests.DATASOURCE1)
                                    .intervals(querySegmentSpec(Filtration.eternity()))
                                    .granularity(Granularities.ALL)
                                    .filters(notNull("dim1"))
                                    .aggregators(
                                        aggregators(
                                            new ExpressionLambdaAggregatorFactory(
                                                "a0",
                                                ImmutableSet.of("dim1"),
                                                "__acc",
                                                "ARRAY<STRING>[]",
                                                "ARRAY<STRING>[]",
                                                true,
                                                true,
                                                false,
                                                "array_set_add(\"__acc\", \"dim1\")",
                                                "array_set_add_all(\"__acc\", \"a0\")",
                                                null,
                                                null,
                                                ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                                TestExprMacroTable.INSTANCE
                                            )
                                        )
                                    )
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
                  .filters(
                      new ExpressionDimFilter(
                          "array_contains(\"j0.a0\",\"dim1\")",
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .columns("dim1", "dim2")
                  .context(QUERY_CONTEXT_DEFAULT)
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()

        ),
        expectedResults
    );
  }

  @Test
  public void testArrayAggGroupByArrayContainsSubquery()
  {
    cannotVectorize();
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{"", 3L},
          new Object[]{"a", 1L},
          new Object[]{"abc", 1L}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{null, 2L},
          new Object[]{"", 1L},
          new Object[]{"a", 2L},
          new Object[]{"abc", 1L}
      );
    }
    testQuery(
        "SELECT dim2, COUNT(*) FROM foo WHERE ARRAY_CONTAINS((SELECT ARRAY_AGG(DISTINCT dim1) FROM foo WHERE dim1 is not null), dim1) GROUP BY 1",
        QUERY_CONTEXT_DEFAULT,
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
                                          .filters(notNull("dim1"))
                                          .aggregators(
                                              aggregators(
                                                  new ExpressionLambdaAggregatorFactory(
                                                      "a0",
                                                      ImmutableSet.of("dim1"),
                                                      "__acc",
                                                      "ARRAY<STRING>[]",
                                                      "ARRAY<STRING>[]",
                                                      true,
                                                      true,
                                                      false,
                                                      "array_set_add(\"__acc\", \"dim1\")",
                                                      "array_set_add_all(\"__acc\", \"a0\")",
                                                      null,
                                                      null,
                                                      ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                                      TestExprMacroTable.INSTANCE
                                                  )
                                              )
                                          )
                                          .context(QUERY_CONTEXT_DEFAULT)
                                          .build()
                                ),
                                "j0.",
                                "1",
                                JoinType.LEFT,
                                null
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setDimFilter(
                            new ExpressionDimFilter(
                                "array_contains(\"j0.a0\",\"dim1\")",
                                TestExprMacroTable.INSTANCE
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("dim2", "d0")))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setGranularity(Granularities.ALL)
                        .setLimitSpec(NoopLimitSpec.instance())
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()

        ),
        expectedResults
    );

  }

  public static void assertResultsDeepEquals(String sql, List<Object[]> expected, List<Object[]> results)
  {
    for (int row = 0; row < results.size(); row++) {
      for (int col = 0; col < results.get(row).length; col++) {
        final String rowString = StringUtils.format("result #%d: %s", row + 1, sql);
        assertDeepEquals(rowString + " - column: " + col + ":", expected.get(row)[col], results.get(row)[col]);
      }
    }
  }

  public static void assertDeepEquals(String path, Object expected, Object actual)
  {
    if (expected instanceof List && actual instanceof List) {
      List expectedList = (List) expected;
      List actualList = (List) actual;
      Assert.assertEquals(path + " arrays length mismatch", expectedList.size(), actualList.size());
      for (int i = 0; i < expectedList.size(); i++) {
        assertDeepEquals(path + "[" + i + "]", expectedList.get(i), actualList.get(i));
      }
    } else {
      Assert.assertEquals(path, expected, actual);
    }
  }

  @Test
  public void testUnnestInline()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT * FROM UNNEST(ARRAY[1,2,3])",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(
                              new Object[]{1L},
                              new Object[]{2L},
                              new Object[]{3L}
                          ),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of(
                      "EXPR$0"
                  ))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3}
        )
    );
  }

  @Test
  public void testUnnestInlineWithCount()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT COUNT(*) FROM (select c from UNNEST(ARRAY[1,2,3]) as unnested(c))",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(
                              new Object[]{1L},
                              new Object[]{2L},
                              new Object[]{3L}
                          ),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .context(QUERY_CONTEXT_UNNEST)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{3L}
        )
    );
  }

  @Test
  public void testUnnest()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{""},
            new Object[]{""},
            new Object[]{""}
        ) :
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{""},
            new Object[]{null},
            new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestTwice()
  {
    cannotVectorize();
    testQuery(
        "SELECT dim1, MV_TO_ARRAY(dim3), STRING_TO_ARRAY(dim1, U&'\\005C.') AS dim1_split, dim1_split_unnest, dim3_unnest\n"
        + "FROM\n"
        + "  druid.numfoo,\n"
        + "  UNNEST(STRING_TO_ARRAY(dim1, U&'\\005C.')) as t2 (dim1_split_unnest),\n"
        + "  UNNEST(MV_TO_ARRAY(dim3)) as t3 (dim3_unnest)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      UnnestDataSource.create(
                          UnnestDataSource.create(
                              new TableDataSource(CalciteTests.DATASOURCE3),
                              expressionVirtualColumn(
                                  "j0.unnest",
                                  "string_to_array(\"dim1\",'\\u005C.')",
                                  ColumnType.STRING_ARRAY
                              ),
                              null
                          ),
                          expressionVirtualColumn(
                              "_j0.unnest",
                              "\"dim3\"",
                              ColumnType.STRING
                          ),
                          null
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "mv_to_array(\"dim3\")",
                          ColumnType.STRING_ARRAY
                      ),
                      expressionVirtualColumn(
                          "v1",
                          "string_to_array(\"dim1\",'\\u005C.')",
                          ColumnType.STRING_ARRAY
                      )
                  )
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("_j0.unnest", "dim1", "j0.unnest", "v0", "v1"))
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "10", "b"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "10", "c"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "1", "b"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "1", "c"},
            new Object[]{"2", ImmutableList.of("d"), ImmutableList.of("2"), "2", "d"},
            new Object[]{"1", useDefault ? null : ImmutableList.of(""), ImmutableList.of("1"), "1", ""},
            new Object[]{"def", null, ImmutableList.of("def"), "def", NullHandling.defaultStringValue()},
            new Object[]{"abc", null, ImmutableList.of("abc"), "abc", NullHandling.defaultStringValue()}
        ) :
        ImmutableList.of(
            new Object[]{"", ImmutableList.of("a", "b"), ImmutableList.of(""), "", "a"},
            new Object[]{"", ImmutableList.of("a", "b"), ImmutableList.of(""), "", "b"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "10", "b"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "10", "c"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "1", "b"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "1", "c"},
            new Object[]{"2", ImmutableList.of("d"), ImmutableList.of("2"), "2", "d"},
            new Object[]{"1", ImmutableList.of(""), ImmutableList.of("1"), "1", ""},
            new Object[]{"def", null, ImmutableList.of("def"), "def", null},
            new Object[]{"abc", null, ImmutableList.of("abc"), "abc", null}
        )
    );
  }

  @Test
  public void testUnnestTwiceWithFiltersAndExpressions()
  {
    cannotVectorize();
    testQuery(
        "SELECT dim1, MV_TO_ARRAY(dim3), STRING_TO_ARRAY(dim1, U&'\\005C.') AS dim1_split, dim1_split_unnest, dim3_unnest || 'xx'\n"
        + "FROM\n"
        + "  druid.numfoo,\n"
        + "  UNNEST(STRING_TO_ARRAY(dim1, U&'\\005C.')) as t2 (dim1_split_unnest),\n"
        + "  UNNEST(MV_TO_ARRAY(dim3)) as t3 (dim3_unnest)"
        + "WHERE dim1_split_unnest IN ('1', '2') AND dim3_unnest LIKE '_'",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      UnnestDataSource.create(
                          UnnestDataSource.create(
                              new TableDataSource(CalciteTests.DATASOURCE3),
                              expressionVirtualColumn(
                                  "j0.unnest",
                                  "string_to_array(\"dim1\",'\\u005C.')",
                                  ColumnType.STRING_ARRAY
                              ),
                              in("j0.unnest", ImmutableList.of("1", "2"), null)
                          ),
                          expressionVirtualColumn(
                              "_j0.unnest",
                              "\"dim3\"",
                              ColumnType.STRING
                          ),
                          new LikeDimFilter("_j0.unnest", "_", null, null)
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(
                      expressionVirtualColumn(
                          "v0",
                          "mv_to_array(\"dim3\")",
                          ColumnType.STRING_ARRAY
                      ),
                      expressionVirtualColumn(
                          "v1",
                          "string_to_array(\"dim1\",'\\u005C.')",
                          ColumnType.STRING_ARRAY
                      ),
                      expressionVirtualColumn(
                          "v2",
                          "concat(\"_j0.unnest\",'xx')",
                          ColumnType.STRING
                      )
                  )
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("dim1", "j0.unnest", "v0", "v1", "v2"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "1", "bxx"},
            new Object[]{"10.1", ImmutableList.of("b", "c"), ImmutableList.of("10", "1"), "1", "cxx"},
            new Object[]{"2", ImmutableList.of("d"), ImmutableList.of("2"), "2", "dxx"}
        )
    );
  }

  @Test
  public void testUnnestThriceWithFiltersOnDimAndUnnestCol()
  {
    notMsqCompatible();
    cannotVectorize();
    String sql = "    SELECT dimZipf, dim3_unnest1, dim3_unnest2, dim3_unnest3 FROM \n"
                 + "      ( SELECT * FROM \n"
                 + "           ( SELECT * FROM lotsocolumns, UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest1) )"
                 + "           ,UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest2) \n"
                 + "      ), UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest3) "
                 + " WHERE dimZipf=27 AND dim3_unnest1='Baz'";
    List<Query<?>> expectedQuerySc = ImmutableList.of(
        Druids.newScanQueryBuilder()
              .dataSource(
                  UnnestDataSource.create(
                      UnnestDataSource.create(
                          FilteredDataSource.create(
                              UnnestDataSource.create(
                                  new TableDataSource(CalciteTests.DATASOURCE5),
                                  expressionVirtualColumn(
                                      "j0.unnest",
                                      "\"dimMultivalEnumerated\"",
                                      ColumnType.STRING
                                  ),
                                  null
                              ),
                              and(
                                  NullHandling.sqlCompatible()
                                  ? equality("dimZipf", "27", ColumnType.LONG)
                                  : bound("dimZipf", "27", "27", false, false, null, StringComparators.NUMERIC),
                                  equality("j0.unnest", "Baz", ColumnType.STRING)
                              )
                          ),
                          expressionVirtualColumn(
                              "_j0.unnest",
                              "\"dimMultivalEnumerated\"",
                              ColumnType.STRING
                          ), null
                      ),
                      expressionVirtualColumn(
                          "__j0.unnest",
                          "\"dimMultivalEnumerated\"",
                          ColumnType.STRING
                      ),
                      null
                  )
              )
              .intervals(querySegmentSpec(Filtration.eternity()))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .legacy(false)
              .context(QUERY_CONTEXT_UNNEST)
              .virtualColumns(expressionVirtualColumn(
                  "v0",
                  "'Baz'",
                  ColumnType.STRING
              ))
              .columns(ImmutableList.of("__j0.unnest", "_j0.unnest", "dimZipf", "v0"))
              .build()
    );
    testQuery(
        sql,
        QUERY_CONTEXT_UNNEST,
        expectedQuerySc,
        ImmutableList.of(
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Hello"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Hello"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Hello", "Baz"},
            new Object[]{"27", "Baz", "Hello", "Baz"},
            new Object[]{"27", "Baz", "Hello", "Hello"},
            new Object[]{"27", "Baz", "Hello", "World"},
            new Object[]{"27", "Baz", "World", "Baz"},
            new Object[]{"27", "Baz", "World", "Baz"},
            new Object[]{"27", "Baz", "World", "Hello"},
            new Object[]{"27", "Baz", "World", "World"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Hello"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Baz"},
            new Object[]{"27", "Baz", "Baz", "Hello"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Hello", "Baz"},
            new Object[]{"27", "Baz", "Hello", "Baz"},
            new Object[]{"27", "Baz", "Hello", "Hello"},
            new Object[]{"27", "Baz", "Hello", "World"},
            new Object[]{"27", "Baz", "World", "Baz"},
            new Object[]{"27", "Baz", "World", "Baz"},
            new Object[]{"27", "Baz", "World", "Hello"},
            new Object[]{"27", "Baz", "World", "World"}
        )
    );
  }
  @Test
  public void testUnnestThriceWithFiltersOnDimAndAllUnnestColumns()
  {
    notMsqCompatible();
    cannotVectorize();
    String sql = "    SELECT dimZipf, dim3_unnest1, dim3_unnest2, dim3_unnest3 FROM \n"
                 + "      ( SELECT * FROM \n"
                 + "           ( SELECT * FROM lotsocolumns, UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest1) )"
                 + "           ,UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest2) \n"
                 + "      ), UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest3) "
                 + " WHERE dimZipf=27 AND dim3_unnest1='Baz' AND dim3_unnest2='Hello' AND dim3_unnest3='World'";
    List<Query<?>> expectedQuerySc = ImmutableList.of(
        Druids.newScanQueryBuilder()
              .dataSource(
                  UnnestDataSource.create(
                      UnnestDataSource.create(
                          FilteredDataSource.create(
                              UnnestDataSource.create(
                                  new TableDataSource(CalciteTests.DATASOURCE5),
                                  expressionVirtualColumn(
                                      "j0.unnest",
                                      "\"dimMultivalEnumerated\"",
                                      ColumnType.STRING
                                  ),
                                  null
                              ),
                              and(
                                  NullHandling.sqlCompatible()
                                  ? equality("dimZipf", "27", ColumnType.LONG)
                                  : bound("dimZipf", "27", "27", false, false, null, StringComparators.NUMERIC),
                                  equality("j0.unnest", "Baz", ColumnType.STRING)
                              )
                          ),
                          expressionVirtualColumn(
                              "_j0.unnest",
                              "\"dimMultivalEnumerated\"",
                              ColumnType.STRING
                          ), equality("_j0.unnest", "Hello", ColumnType.STRING)
                      ),
                      expressionVirtualColumn(
                          "__j0.unnest",
                          "\"dimMultivalEnumerated\"",
                          ColumnType.STRING
                      ),
                      equality("__j0.unnest", "World", ColumnType.STRING)
                  )
              )
              .intervals(querySegmentSpec(Filtration.eternity()))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .virtualColumns(expressionVirtualColumn(
                  "v0",
                  "'Baz'",
                  ColumnType.STRING
              ))
              .legacy(false)
              .context(QUERY_CONTEXT_UNNEST)
              .columns(ImmutableList.of("__j0.unnest", "_j0.unnest", "dimZipf", "v0"))
              .build()
    );
    testQuery(
        sql,
        QUERY_CONTEXT_UNNEST,
        expectedQuerySc,
        ImmutableList.of(
            new Object[]{"27", "Baz", "Hello", "World"},
            new Object[]{"27", "Baz", "Hello", "World"}
        )
    );
  }

  @Test
  public void testUnnestThriceWithFiltersOnDimAndUnnestColumnsORCombinations()
  {
    cannotVectorize();
    skipVectorize();
    String sql = "    SELECT dimZipf, dim3_unnest1, dim3_unnest2, dim3_unnest3 FROM \n"
                 + "      ( SELECT * FROM \n"
                 + "           ( SELECT * FROM lotsocolumns, UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest1) )"
                 + "           ,UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest2) \n"
                 + "      ), UNNEST(MV_TO_ARRAY(dimMultivalEnumerated)) as ut(dim3_unnest3) "
                 + " WHERE dimZipf=27 AND (dim3_unnest1='Baz' OR dim3_unnest2='Hello') AND dim3_unnest3='World'";
    List<Query<?>> expectedQuerySqlCom = ImmutableList.of(
        Druids.newScanQueryBuilder()
              .dataSource(
                  UnnestDataSource.create(
                      FilteredDataSource.create(
                          UnnestDataSource.create(
                              FilteredDataSource.create(
                                  UnnestDataSource.create(
                                      new TableDataSource(CalciteTests.DATASOURCE5),
                                      expressionVirtualColumn(
                                          "j0.unnest",
                                          "\"dimMultivalEnumerated\"",
                                          ColumnType.STRING
                                      ),
                                      null
                                  ),
                                  NullHandling.sqlCompatible() ? equality("dimZipf", "27", ColumnType.LONG) : range(
                                      "dimZipf",
                                      ColumnType.LONG,
                                      "27",
                                      "27",
                                      false,
                                      false
                                  )
                              ),
                              expressionVirtualColumn(
                                  "_j0.unnest",
                                  "\"dimMultivalEnumerated\"",
                                  ColumnType.STRING
                              ),
                              null
                          ),
                          or(
                              equality("j0.unnest", "Baz", ColumnType.STRING),
                              equality("_j0.unnest", "Hello", ColumnType.STRING)
                          ) // (j0.unnest = Baz || _j0.unnest = Hello)
                      ),
                      expressionVirtualColumn(
                          "__j0.unnest",
                          "\"dimMultivalEnumerated\"",
                          ColumnType.STRING
                      ),
                      equality("__j0.unnest", "World", ColumnType.STRING)
                  )
              )
              .intervals(querySegmentSpec(Filtration.eternity()))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
              .legacy(false)
              .context(QUERY_CONTEXT_UNNEST)
              .columns(ImmutableList.of("__j0.unnest", "_j0.unnest", "dimZipf", "j0.unnest"))
              .build()
    );
    testQuery(
        sql,
        QUERY_CONTEXT_UNNEST, expectedQuerySqlCom,
        ImmutableList.of(
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Hello", "World"},
            new Object[]{"27", "Baz", "World", "World"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Baz", "World"},
            new Object[]{"27", "Baz", "Hello", "World"},
            new Object[]{"27", "Baz", "World", "World"},
            new Object[]{"27", "Hello", "Hello", "World"},
            new Object[]{"27", "World", "Hello", "World"}
        )
    );
  }
  @Test
  public void testUnnestWithGroupBy()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) GROUP BY d3 ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE3),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"}
        ) :
        ImmutableList.of(
            new Object[]{null},
            new Object[]{""},
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByOrderBy()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3, COUNT(*) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested(d3) GROUP BY d3 ORDER BY d3 DESC ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE3),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(new OrderByColumnSpec(
                                    "_d0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.LEXICOGRAPHIC
                                ))
                                .build()
                        )
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"d", 1L},
            new Object[]{"c", 1L},
            new Object[]{"b", 2L},
            new Object[]{"a", 1L},
            new Object[]{"", 3L}
        ) :
        ImmutableList.of(
            new Object[]{"d", 1L},
            new Object[]{"c", 1L},
            new Object[]{"b", 2L},
            new Object[]{"a", 1L},
            new Object[]{"", 1L},
            new Object[]{null, 2L}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByOrderByWithLimit()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3, COUNT(*) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested(d3) GROUP BY d3 ORDER BY d3 ASC LIMIT 4 ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            new TopNQueryBuilder()
                .dataSource(UnnestDataSource.create(
                    new TableDataSource(CalciteTests.DATASOURCE3),
                    expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                    null
                ))
                .intervals(querySegmentSpec(Filtration.eternity()))
                .dimension(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
                .threshold(4)
                .aggregators(aggregators(new CountAggregatorFactory("a0")))
                .context(QUERY_CONTEXT_UNNEST)
                .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"", 3L},
            new Object[]{"a", 1L},
            new Object[]{"b", 2L},
            new Object[]{"c", 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{"", 1L},
            new Object[]{"a", 1L},
            new Object[]{"b", 2L}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByHaving()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3, COUNT(*) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested(d3) GROUP BY d3 HAVING COUNT(*) = 1",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE3),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setHavingSpec(new DimFilterHavingSpec(equality("a0", 1L, ColumnType.LONG), true))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"a", 1L},
            new Object[]{"c", 1L},
            new Object[]{"d", 1L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"a", 1L},
            new Object[]{"c", 1L},
            new Object[]{"d", 1L}
        )
    );
  }

  @Test
  public void testUnnestWithLimit()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) LIMIT 3",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .limit(3)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestFirstQueryOnSelect()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM (select dim1, dim2, dim3 from druid.numfoo), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{""},
            new Object[]{""},
            new Object[]{""}
        ) :
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{""},
            new Object[]{null},
            new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestVirtualWithColumns1()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT strings, m1 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (strings) where (strings='a' and (m1<=10 or strings='b'))",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(Druids.newScanQueryBuilder()
                               .dataSource(UnnestDataSource.create(
                                   new TableDataSource(CalciteTests.DATASOURCE3),
                                   expressionVirtualColumn(
                                       "j0.unnest",
                                       "\"dim3\"",
                                       ColumnType.STRING
                                   ),
                                   equality("j0.unnest", "a", ColumnType.STRING)
                               ))
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                               .legacy(false)
                               .filters(or(
                                   NullHandling.sqlCompatible()
                                   ? range("m1", ColumnType.LONG, null, "10", false, false)
                                   : bound(
                                       "m1",
                                       null,
                                       "10",
                                       false,
                                       false,
                                       null,
                                       StringComparators.NUMERIC
                                   ),
                                   equality("j0.unnest", "b", ColumnType.STRING)
                               ))
                               .context(QUERY_CONTEXT_UNNEST)
                               .columns(ImmutableList.of("j0.unnest", "m1"))
                               .build()),
        ImmutableList.of(new Object[]{"a", 1.0f})
    );
  }

  @Test
  public void testUnnestVirtualWithColumns2()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT strings, m1 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (strings) where (strings='a' or (m1=2 and strings='b'))",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(Druids.newScanQueryBuilder()
                               .dataSource(UnnestDataSource.create(
                                   new TableDataSource(CalciteTests.DATASOURCE3),
                                   expressionVirtualColumn(
                                       "j0.unnest",
                                       "\"dim3\"",
                                       ColumnType.STRING
                                   ),
                                   null
                               ))
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                               .legacy(false) // (j0.unnest = a || (m1 = 2 && j0.unnest = b))
                               .filters(or(
                                   equality("j0.unnest", "a", ColumnType.STRING),
                                   and(
                                       NullHandling.sqlCompatible()
                                       ? equality("m1", "2", ColumnType.FLOAT)
                                       : equality("m1", "2", ColumnType.STRING),
                                       equality("j0.unnest", "b", ColumnType.STRING)
                                   )
                               ))
                               .context(QUERY_CONTEXT_UNNEST)
                               .columns(ImmutableList.of("j0.unnest", "m1"))
                               .build()),
        ImmutableList.of(
            new Object[]{"a", 1.0f},
            new Object[]{"b", 2.0f}
        )
    );
  }
  @Test
  public void testUnnestWithFilters()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM (select * from druid.numfoo where dim2='a'), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          equality("dim2", "a", ColumnType.STRING)
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testUnnestWithFiltersWithExpressionInInnerQuery()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT t,d3 FROM (select FLOOR(__time to hour) t, dim3 from druid.numfoo where dim2='a'), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          equality("dim2", "a", ColumnType.STRING)
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(expressionVirtualColumn(
                      "v0",
                      "timestamp_floor(\"__time\",'PT1H',null,'UTC')",
                      ColumnType.LONG
                  ))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest", "v0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, "a"},
            new Object[]{946684800000L, "b"},
            new Object[]{978307200000L, ""}
        )
    );
  }

  @Test
  public void testUnnestWithInFiltersWithExpressionInInnerQuery()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT t,d3 FROM (select FLOOR(__time to hour) t, dim3 from druid.numfoo where dim2 IN ('a','b')), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          new InDimFilter("dim2", ImmutableList.of("a", "b"), null)
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(expressionVirtualColumn("v0",
                                                          "timestamp_floor(\"__time\",'PT1H',null,'UTC')",
                                                          ColumnType.LONG))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest", "v0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{946684800000L, "a"},
            new Object[]{946684800000L, "b"},
            new Object[]{978307200000L, ""}
        )
    );
  }

  @Test
  public void testUnnestWithFiltersInnerLimit()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM (select dim2,dim3 from druid.numfoo where dim2='a' LIMIT 2), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new QueryDataSource(
                          newScanQueryBuilder()
                              .dataSource(
                                  new TableDataSource(CalciteTests.DATASOURCE3)
                              )
                              .intervals(querySegmentSpec(Filtration.eternity()))
                              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                              .legacy(false)
                              .filters(equality("dim2", "a", ColumnType.STRING))
                              .columns("dim3")
                              .limit(2)
                              .context(QUERY_CONTEXT_UNNEST)
                              .build()
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"}
        ) :
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testUnnestWithFiltersInsideAndOutside()
  {
    skipVectorize();
    testQuery(
        "SELECT d3 FROM\n"
        + "  (select * from druid.numfoo where dim2='a') as t,\n"
        + "  UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)\n"
        + "WHERE t.dim1 <> 'foo'\n"
        + "AND unnested.d3 <> 'b'",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          and(
                              equality("dim2", "a", ColumnType.STRING),
                              not(equality("dim1", "foo", ColumnType.STRING))
                          )
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      not(equality("j0.unnest", "b", ColumnType.STRING))
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testUnnestWithFiltersInsideAndOutside1()
  {
    skipVectorize();
    testQuery(
        "SELECT d3 FROM\n"
        + "  (select * from druid.numfoo where dim2='a'),\n"
        + "  UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)\n"
        + "WHERE dim1 <> 'foo'\n"
        + "AND (unnested.d3 IN ('a', 'c') OR unnested.d3 LIKE '_')",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          and(
                              equality("dim2", "a", ColumnType.STRING),
                              not(equality("dim1", "foo", ColumnType.STRING))
                          )
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      or(
                          new LikeDimFilter("j0.unnest", "_", null, null),
                          in("j0.unnest", ImmutableList.of("a", "c"), null)
                      )
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithFiltersOutside()
  {
    skipVectorize();
    testQuery(
        "SELECT d3 FROM\n"
        + "  druid.numfoo t,\n"
        + "  UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)\n"
        + "WHERE t.dim2='a'\n"
        + "AND t.dim1 <> 'foo'\n"
        + "AND (unnested.d3 IN ('a', 'c') OR unnested.d3 LIKE '_')",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          and(
                              equality("dim2", "a", ColumnType.STRING),
                              not(equality("dim1", "foo", ColumnType.STRING))
                          )
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      or(
                          new LikeDimFilter("j0.unnest", "_", null, null),
                          in("j0.unnest", ImmutableList.of("a", "c"), null)
                      )
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithInFilters()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM (select * from druid.numfoo where dim2 IN ('a','b','ab','abc')), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          new InDimFilter("dim2", ImmutableList.of("a", "b", "ab", "abc"), null)
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{""},
            useDefault ?
            new Object[]{""} : new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestVirtualWithColumns()
  {
    // This tells the test to skip generating (vectorize = force) path
    // Generates only 1 native query with vectorize = false
    skipVectorize();
    // This tells that both vectorize = force and vectorize = false takes the same path of non vectorization
    // Generates 2 native queries with 2 different values of vectorize
    cannotVectorize();
    testQuery(
        "SELECT strings FROM druid.numfoo, UNNEST(ARRAY[dim4, dim5]) as unnested (strings)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "array(\"dim4\",\"dim5\")", ColumnType.STRING_ARRAY),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"aa"},
            new Object[]{"a"},
            new Object[]{"ab"},
            new Object[]{"a"},
            new Object[]{"ba"},
            new Object[]{"b"},
            new Object[]{"ad"},
            new Object[]{"b"},
            new Object[]{"aa"},
            new Object[]{"b"},
            new Object[]{"ab"}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByOrderByOnVirtualColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d24, COUNT(*) FROM druid.numfoo, UNNEST(ARRAY[dim2, dim4]) AS unnested(d24) GROUP BY d24 ORDER BY d24 DESC ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            UnnestDataSource.create(
                                new TableDataSource(CalciteTests.DATASOURCE3),
                                expressionVirtualColumn(
                                    "j0.unnest",
                                    "array(\"dim2\",\"dim4\")",
                                    ColumnType.STRING_ARRAY
                                ),
                                null
                            )
                        )
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setLimitSpec(
                            DefaultLimitSpec
                                .builder()
                                .orderBy(new OrderByColumnSpec(
                                    "_d0",
                                    OrderByColumnSpec.Direction.DESCENDING,
                                    StringComparators.LEXICOGRAPHIC
                                ))
                                .build()
                        )
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"b", 3L},
            new Object[]{"abc", 1L},
            new Object[]{"a", 5L},
            new Object[]{"", 3L}
        ) :
        ImmutableList.of(
            new Object[]{"b", 3L},
            new Object[]{"abc", 1L},
            new Object[]{"a", 5L},
            new Object[]{"", 1L},
            new Object[]{null, 2L}
        )
    );
  }

  @Test
  public void testUnnestWithJoinOnTheLeft()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 from (SELECT * from druid.numfoo JOIN (select dim2 as t from druid.numfoo where dim2 IN ('a','b','ab','abc')) ON dim2=t), UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          new QueryDataSource(
                              newScanQueryBuilder()
                                  .dataSource(
                                      new TableDataSource(CalciteTests.DATASOURCE3)
                                  )
                                  .intervals(querySegmentSpec(Filtration.eternity()))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                  .legacy(false)
                                  .filters(new InDimFilter("dim2", ImmutableList.of("a", "b", "ab", "abc"), null))
                                  .columns("dim2")
                                  .context(QUERY_CONTEXT_UNNEST)
                                  .build()
                          ),
                          "j0.",
                          "(\"dim2\" == \"j0.dim2\")",
                          JoinType.INNER
                      ),
                      expressionVirtualColumn("_j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("_j0.unnest"))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{""},
            new Object[]{""},
            new Object[]{""}
        ) :
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{""},
            new Object[]{""},
            new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestWithConstant()
  {
    // Since there is a constant on the right,
    // Druid will plan this as a join query
    // as there is nothing to correlate between left and right
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT longs FROM druid.numfoo, UNNEST(ARRAY[1,2,3]) as unnested (longs)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      join(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          InlineDataSource.fromIterable(
                              ImmutableList.of(
                                  new Object[]{1L},
                                  new Object[]{2L},
                                  new Object[]{3L}
                              ),
                              RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                          ),
                          "j0.",
                          "1",
                          JoinType.INNER
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.EXPR$0"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{1},
            new Object[]{2},
            new Object[]{3},
            new Object[]{1},
            new Object[]{2},
            new Object[]{3}
        )
    );
  }

  @Test
  public void testUnnestWithSQLFunctionOnUnnestedColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT strlen(d3) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .virtualColumns(expressionVirtualColumn("v0", "strlen(\"j0.unnest\")", ColumnType.LONG))
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("v0"))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{1},
            new Object[]{1},
            new Object[]{1},
            new Object[]{1},
            new Object[]{1},
            new Object[]{0},
            new Object[]{0},
            new Object[]{0}
        ) :
        ImmutableList.of(
            new Object[]{1},
            new Object[]{1},
            new Object[]{1},
            new Object[]{1},
            new Object[]{1},
            new Object[]{0},
            new Object[]{null},
            new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestWithINFiltersWithLeftRewrite()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3 IN ('a','b') and m1 < 10",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          range("m1", ColumnType.LONG, null, 10L, false, true)
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      new InDimFilter("j0.unnest", ImmutableSet.of("a", "b"), null)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithINFiltersWithNoLeftRewrite()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d45 FROM druid.numfoo, UNNEST(ARRAY[dim4,dim5]) as unnested (d45) where d45 IN ('a','b')",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "array(\"dim4\",\"dim5\")", ColumnType.STRING_ARRAY),
                      new InDimFilter("j0.unnest", ImmutableSet.of("a", "b"), null)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"a"},
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithInvalidINFiltersOnUnnestedColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3 IN ('foo','bar')",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      new InDimFilter("j0.unnest", ImmutableSet.of("foo", "bar"), null)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of()
    );
  }

  @Test
  public void testUnnestWithNotFiltersOnUnnestedColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3!='d' ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      not(equality("j0.unnest", "d", ColumnType.STRING))
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{""},
            new Object[]{""},
            new Object[]{""}
        ) :
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{""},
            new Object[]{null},
            new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestWithSelectorFiltersOnSelectedColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3='b'",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      equality("j0.unnest", "b", ColumnType.STRING)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithSelectorFiltersOnVirtualColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d12 FROM druid.numfoo, UNNEST(ARRAY[m1,m2]) as unnested (d12) where d12=1",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "array(\"m1\",\"m2\")", ColumnType.FLOAT_ARRAY),
                      NullHandling.replaceWithDefault()
                      ? selector("j0.unnest", "1")
                      : equality("j0.unnest", 1.0, ColumnType.FLOAT)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1.0f},
            new Object[]{1.0f}
        )
    );
  }

  @Test
  public void testUnnestWithSelectorFiltersOnVirtualStringColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d45 FROM druid.numfoo, UNNEST(ARRAY[dim4,dim5]) as unnested (d45) where d45 IN ('a','ab')",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "array(\"dim4\",\"dim5\")", ColumnType.STRING_ARRAY),
                      new InDimFilter("j0.unnest", ImmutableSet.of("a", "ab"), null)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"a"},
            new Object[]{"ab"},
            new Object[]{"a"},
            new Object[]{"ab"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleAndFiltersOnSelectedColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3='b' and m1 < 10 and m2 < 10",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          and(
                              range("m1", ColumnType.LONG, null, 10L, false, true),
                              range("m2", ColumnType.LONG, null, 10L, false, true)
                          )
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      equality("j0.unnest", "b", ColumnType.STRING)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnSelectedColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3='b' or m1 < 2 ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .filters(
                      or(
                          equality("j0.unnest", "b", ColumnType.STRING),
                          range("m1", ColumnType.LONG, null, 2L, false, true)
                      )
                  )
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleAndFiltersOnSelectedUnnestedColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3 IN ('a','b') and d3 < 'e' ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      new InDimFilter("j0.unnest", ImmutableSet.of("a", "b"), null)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnUnnestedColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3='b' or d3='d' ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      new InDimFilter("j0.unnest", ImmutableSet.of("b", "d"), null)
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"d"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnVariationsOfUnnestedColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where strlen(d3) < 2 or d3='d' ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      or(
                          expressionFilter("(strlen(\"j0.unnest\") < 2)"),
                          equality("j0.unnest", "d", ColumnType.STRING)
                      )
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{""},
            new Object[]{""},
            new Object[]{""}
        ) :
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"d"},
            new Object[]{""}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnSelectedNonUnnestedColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where m1 < 2 or m2 < 2 ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      FilteredDataSource.create(
                          new TableDataSource(CalciteTests.DATASOURCE3),
                          or(
                              range("m1", ColumnType.LONG, null, 2L, false, true),
                              range("m2", ColumnType.LONG, null, 2L, false, true)
                          )
                      ),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"b"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnSelectedVirtualColumns()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d45 FROM druid.numfoo, UNNEST(ARRAY[dim4,dim5]) as unnested (d45) where d45 IN ('a','aa') or m1 < 2 ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "array(\"dim4\",\"dim5\")", ColumnType.STRING_ARRAY),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .filters(
                      or(
                          range("m1", ColumnType.LONG, null, 2L, false, true),
                          new InDimFilter("j0.unnest", ImmutableSet.of("a", "aa"), null)
                      )
                  )
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{"aa"},
            new Object[]{"a"},
            new Object[]{"a"},
            new Object[]{"aa"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnUnnestedColumnsAndOnOriginalColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where d3='b' or dim3='d' ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .filters(
                      or(
                          equality("j0.unnest", "b", ColumnType.STRING),
                          equality("dim3", "d", ColumnType.STRING)
                      )
                  )
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"b"},
            new Object[]{"b"},
            new Object[]{"d"}
        )
    );
  }

  @Test
  public void testUnnestWithMultipleOrFiltersOnUnnestedColumnsAndOnOriginalColumnDiffOrdering()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT dim3, d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3) where dim3='b' or d3='a' ",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .filters(
                      or(
                          equality("dim3", "b", ColumnType.STRING),
                          equality("j0.unnest", "a", ColumnType.STRING)
                      )
                  )
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("dim3", "j0.unnest"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a\",\"b\"]", "a"},
            new Object[]{"[\"a\",\"b\"]", "b"},
            new Object[]{"[\"b\",\"c\"]", "b"},
            new Object[]{"[\"b\",\"c\"]", "c"}
        )
    );
  }

  @Test
  public void testUnnestWithCountOnColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT count(*) d3 FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) as unnested (d3)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .context(QUERY_CONTEXT_UNNEST)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{8L}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByHavingSelector()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3, COUNT(*) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested(d3) GROUP BY d3 HAVING d3='b'",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE3),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            null
                        ))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(equality("j0.unnest", "b", ColumnType.STRING))
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"b", 2L}
        )
    );
  }

  @Test
  public void testUnnestWithSumOnUnnestedVirtualColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "select sum(c) col from druid.numfoo, unnest(ARRAY[m1,m2]) as u(c)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "array(\"m1\",\"m2\")", ColumnType.FLOAT_ARRAY),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .context(QUERY_CONTEXT_UNNEST)
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "j0.unnest")))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{42.0}
        )
    );
  }

  @Test
  public void testUnnestWithSumOnUnnestedColumn()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "select sum(c) col from druid.numfoo, unnest(mv_to_array(dim3)) as u(c)",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE3),
                      expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .virtualColumns(expressionVirtualColumn("v0", "CAST(\"j0.unnest\", 'DOUBLE')", ColumnType.DOUBLE))
                  .context(QUERY_CONTEXT_UNNEST)
                  .aggregators(aggregators(new DoubleSumAggregatorFactory("a0", "v0")))
                  .build()
        ),
        useDefault ?
        ImmutableList.of(
            new Object[]{0.0}
        ) :
        ImmutableList.of(
            new Object[]{null}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByHavingWithWhereOnAggCol()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3, COUNT(*) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested(d3) WHERE d3 IN ('a','c') GROUP BY d3 HAVING COUNT(*) = 1",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE3),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            new InDimFilter("j0.unnest", ImmutableSet.of("a", "c"), null)
                        ))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setHavingSpec(new DimFilterHavingSpec(equality("a0", 1L, ColumnType.LONG), true))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 1L},
            new Object[]{"c", 1L}
        )
    );
  }

  @Test
  public void testUnnestWithGroupByHavingWithWhereOnUnnestCol()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "SELECT d3, COUNT(*) FROM druid.numfoo, UNNEST(MV_TO_ARRAY(dim3)) AS unnested(d3) WHERE d3 IN ('a','c') GROUP BY d3 HAVING d3='a'",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(UnnestDataSource.create(
                            new TableDataSource(CalciteTests.DATASOURCE3),
                            expressionVirtualColumn("j0.unnest", "\"dim3\"", ColumnType.STRING),
                            new InDimFilter("j0.unnest", ImmutableSet.of("a", "c"), null)
                        ))
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .setDimensions(new DefaultDimensionSpec("j0.unnest", "_d0", ColumnType.STRING))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setDimFilter(equality("j0.unnest", "a", ColumnType.STRING))
                        .setContext(QUERY_CONTEXT_UNNEST)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"a", 1L}
        )
    );
  }

  @Test
  public void testUnnestVirtualWithColumnsAndNullIf()
  {
    skipVectorize();
    cannotVectorize();
    testQuery(
        "select c,m2 from druid.foo, unnest(ARRAY[\"m1\", \"m2\"]) as u(c) where NULLIF(c,m2) IS NULL",
        QUERY_CONTEXT_UNNEST,
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(UnnestDataSource.create(
                      new TableDataSource(CalciteTests.DATASOURCE1),
                      expressionVirtualColumn("j0.unnest", "array(\"m1\",\"m2\")", ColumnType.FLOAT_ARRAY),
                      null
                  ))
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .filters(
                      useDefault ? expressionFilter("(\"j0.unnest\" == \"m2\")") :
                      or(
                          expressionFilter("(\"j0.unnest\" == \"m2\")"),
                          and(
                              isNull("j0.unnest"),
                              not(expressionFilter("(\"j0.unnest\" == \"m2\")"))
                          )
                      ))
                  .legacy(false)
                  .context(QUERY_CONTEXT_UNNEST)
                  .columns(ImmutableList.of("j0.unnest", "m2"))
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1.0f, 1.0D},
            new Object[]{1.0f, 1.0D},
            new Object[]{2.0f, 2.0D},
            new Object[]{2.0f, 2.0D},
            new Object[]{3.0f, 3.0D},
            new Object[]{3.0f, 3.0D},
            new Object[]{4.0f, 4.0D},
            new Object[]{4.0f, 4.0D},
            new Object[]{5.0f, 5.0D},
            new Object[]{5.0f, 5.0D},
            new Object[]{6.0f, 6.0D},
            new Object[]{6.0f, 6.0D}
        )
    );
  }
}
