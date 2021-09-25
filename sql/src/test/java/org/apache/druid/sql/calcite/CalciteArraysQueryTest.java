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
import com.google.common.collect.ImmutableSet;
import junitparams.JUnitParamsRunner;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.NoopLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for array functions and array types
 */
@RunWith(JUnitParamsRunner.class)
public class CalciteArraysQueryTest extends BaseCalciteQueryTest
{
  // test some query stuffs, sort of limited since no native array column types so either need to use constructor or
  // array aggregator
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
  public void testGroupByArrayFromCase() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT CASE WHEN dim4 = 'a' THEN ARRAY['foo','bar','baz'] END as mv_value, count(1) from numfoo GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "case_searched((\"dim4\" == 'a'),array('foo','bar','baz'),null)",
                            ValueType.STRING
                        ))
                        .setDimensions(new DefaultDimensionSpec("v0", "_d0"))
                        .setGranularity(Granularities.ALL)
                        .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{null, 3L},
            new Object[]{"bar", 3L},
            new Object[]{"baz", 3L},
            new Object[]{"foo", 3L}
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
  public void testSomeArrayFunctionsWithScanQuery() throws Exception
  {
    // array constructor turns decimals into ints for some reason, this needs fixed in the future
    // also, yes these outputs are strange sometimes, arrays are in a partial state of existence so end up a bit
    // stringy for now this is because virtual column selectors are coercing values back to stringish so that
    // multi-valued string dimensions can be grouped on.
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
              "[1,2,4]",
              "[\"a\",\"b\",\"foo\"]",
              "[\"foo\",\"a\"]",
              "[1,2,7]",
              "[0,1,2]",
              "[1,2,1]",
              "[0,1,2]",
              "[\"a\",\"a\",\"b\"]",
              "[7,0]",
              "[1.0,0.0]",
              "7",
              "1.0",
              "7",
              "1.0"
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
              "[1,2,4]",
              "[\"a\",\"b\",\"foo\"]",
              "[\"foo\",\"a\"]",
              "[1,2,7]",
              "[null,1,2]",
              "[1,2,1]",
              "[null,1,2]",
              "[\"a\",\"a\",\"b\"]",
              "[7,null]",
              "[1.0,null]",
              "7",
              "1.0",
              "7",
              "1.0"
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
                    expressionVirtualColumn("v0", "array('a','b','c')", ValueType.STRING),
                    expressionVirtualColumn("v1", "array(1,2,3)", ValueType.STRING),
                    expressionVirtualColumn("v10", "array_concat(array(\"l1\"),array(\"l2\"))", ValueType.STRING),
                    expressionVirtualColumn("v11", "array_concat(array(\"d1\"),array(\"d2\"))", ValueType.STRING),
                    expressionVirtualColumn("v12", "array_offset(array(\"l1\"),0)", ValueType.STRING),
                    expressionVirtualColumn("v13", "array_offset(array(\"d1\"),0)", ValueType.STRING),
                    expressionVirtualColumn("v14", "array_ordinal(array(\"l1\"),1)", ValueType.STRING),
                    expressionVirtualColumn("v15", "array_ordinal(array(\"d1\"),1)", ValueType.STRING),
                    expressionVirtualColumn("v2", "array(1,2,4)", ValueType.STRING),
                    expressionVirtualColumn("v3", "array_append(\"dim3\",'foo')", ValueType.STRING),
                    expressionVirtualColumn("v4", "array_prepend('foo',array(\"dim2\"))", ValueType.STRING),
                    expressionVirtualColumn("v5", "array_append(array(1,2),\"l1\")", ValueType.STRING),
                    expressionVirtualColumn("v6", "array_prepend(\"l2\",array(1,2))", ValueType.STRING),
                    expressionVirtualColumn("v7", "array_append(array(1,2),\"d1\")", ValueType.STRING),
                    expressionVirtualColumn("v8", "array_prepend(\"d2\",array(1,2))", ValueType.STRING),
                    expressionVirtualColumn("v9", "array_concat(\"dim2\",\"dim3\")", ValueType.STRING)
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
        expectedResults
    );
  }

  @Test
  public void testSomeArrayFunctionsWithScanQueryNoStringify() throws Exception
  {
    // when not stringifying arrays, some things are still stringified, because they are inferred to be typed as strings
    // the planner context which controls stringification of arrays does not apply to multi-valued string columns,
    // which will still always be stringified to ultimately adhere to the varchar type
    // as array support increases in the engine this will likely change since using explict array functions should
    // probably kick it into an array
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{
              "",
              "a",
              "[\"a\",\"b\"]",
              Arrays.asList("a", "b", "c"),
              Arrays.asList(1L, 2L, 3L),
              Arrays.asList(1L, 2L, 4L),
              "[\"a\",\"b\",\"foo\"]",
              Arrays.asList("foo", "a"),
              Arrays.asList(1L, 2L, 7L),
              Arrays.asList(0L, 1L, 2L),
              Arrays.asList(1L, 2L, 1L),
              Arrays.asList(0L, 1L, 2L),
              "[\"a\",\"a\",\"b\"]",
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
              Arrays.asList(1L, 2L, 4L),
              "[\"a\",\"b\",\"foo\"]",
              Arrays.asList("foo", "a"),
              Arrays.asList(1L, 2L, 7L),
              Arrays.asList(null, 1L, 2L),
              Arrays.asList(1L, 2L, 1L),
              Arrays.asList(null, 1L, 2L),
              "[\"a\",\"a\",\"b\"]",
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
                    // these report as strings even though they are not, someday this will not be so
                    expressionVirtualColumn("v0", "array('a','b','c')", ValueType.STRING),
                    expressionVirtualColumn("v1", "array(1,2,3)", ValueType.STRING),
                    expressionVirtualColumn("v10", "array_concat(array(\"l1\"),array(\"l2\"))", ValueType.STRING),
                    expressionVirtualColumn("v11", "array_concat(array(\"d1\"),array(\"d2\"))", ValueType.STRING),
                    expressionVirtualColumn("v2", "array(1,2,4)", ValueType.STRING),
                    expressionVirtualColumn("v3", "array_append(\"dim3\",'foo')", ValueType.STRING),
                    expressionVirtualColumn("v4", "array_prepend('foo',array(\"dim2\"))", ValueType.STRING),
                    expressionVirtualColumn("v5", "array_append(array(1,2),\"l1\")", ValueType.STRING),
                    expressionVirtualColumn("v6", "array_prepend(\"l2\",array(1,2))", ValueType.STRING),
                    expressionVirtualColumn("v7", "array_append(array(1,2),\"d1\")", ValueType.STRING),
                    expressionVirtualColumn("v8", "array_prepend(\"d2\",array(1,2))", ValueType.STRING),
                    expressionVirtualColumn("v9", "array_concat(\"dim2\",\"dim3\")", ValueType.STRING)
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
  public void testArrayOverlapFilter() throws Exception
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
  public void testArrayOverlapFilterNonLiteral() throws Exception
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
            new Object[]{"[\"a\",\"b\"]"},
            new Object[]{useDefault ? "" : null}
        )
    );
  }

  @Test
  public void testArrayContainsFilter() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(dim3, ARRAY['a','b']) LIMIT 5",
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
  public void testArrayContainsArrayOfOneElement() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(dim3, ARRAY['a']) LIMIT 5",
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
  public void testArrayContainsArrayOfNonLiteral() throws Exception
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE ARRAY_CONTAINS(dim3, ARRAY[dim2]) LIMIT 5",
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
  public void testArraySlice() throws Exception
  {
    testQuery(
        "SELECT ARRAY_SLICE(dim3, 1) FROM druid.numfoo",
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
  public void testArrayLength() throws Exception
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
            new Object[]{"1", 1, 1L},
            new Object[]{"2", 1, 1L},
            new Object[]{"abc", 1, 1L},
            new Object[]{"def", 1, 1L}
        )
    );
  }

  @Test
  public void testArrayAppend() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
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
        "SELECT ARRAY_APPEND(dim3, 'foo'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
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
  public void testArrayPrepend() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
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
        "SELECT ARRAY_PREPEND('foo', dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
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
  public void testArrayPrependAppend() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
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
  public void testArrayConcat() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
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
        "SELECT ARRAY_CONCAT(dim3, dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
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
  public void testArrayOffset() throws Exception
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
  public void testArrayOrdinal() throws Exception
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
  public void testArrayOffsetOf() throws Exception
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
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{useDefault ? -1 : null, 4L},
            new Object[]{0, 1L},
            new Object[]{1, 1L}
        )
    );
  }

  @Test
  public void testArrayOrdinalOf() throws Exception
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
                                OrderByColumnSpec.Direction.DESCENDING,
                                StringComparators.NUMERIC
                            )),
                            Integer.MAX_VALUE
                        ))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{useDefault ? -1 : null, 4L},
            new Object[]{1, 1L},
            new Object[]{2, 1L}
        )
    );
  }

  @Test
  public void testArrayToString() throws Exception
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
  public void testArrayToStringToMultiValueString() throws Exception
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"d", 7L},
          new Object[]{null, 3L},
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
        "SELECT STRING_TO_ARRAY(CONCAT(ARRAY_TO_STRING(dim3, ','), ',d'), ','), SUM(cnt) FROM druid.numfoo WHERE ARRAY_LENGTH(dim3) > 0 GROUP BY 1 ORDER BY 2 DESC",
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
  public void testArrayAgg() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_AGG(dim1), ARRAY_AGG(DISTINCT dim1), ARRAY_AGG(DISTINCT dim1) FILTER(WHERE dim1 = 'shazbot') FROM foo WHERE dim1 is not null",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(not(selector("dim1", null, null)))
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("dim1"),
                              "__acc",
                              "[]",
                              "[]",
                              true,
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
                              "[]",
                              "[]",
                              true,
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
                                  "[]",
                                  "[]",
                                  true,
                                  "array_set_add(\"__acc\", \"dim1\")",
                                  "array_set_add_all(\"__acc\", \"a2\")",
                                  null,
                                  null,
                                  ExpressionLambdaAggregatorFactory.DEFAULT_MAX_SIZE_BYTES,
                                  TestExprMacroTable.INSTANCE
                              ),
                              selector("dim1", "shazbot", null)
                          )
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            useDefault
            ? new Object[]{"[\"10.1\",\"2\",\"1\",\"def\",\"abc\"]", "[\"1\",\"2\",\"abc\",\"def\",\"10.1\"]", null}
            : new Object[]{
                "[\"\",\"10.1\",\"2\",\"1\",\"def\",\"abc\"]",
                "[\"\",\"1\",\"2\",\"abc\",\"def\",\"10.1\"]",
                null
            }
        )
    );
  }

  @Test
  public void testArrayAggMultiValue() throws Exception
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
                              "[]",
                              "[]",
                              true,
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
                              "[]",
                              "[]",
                              true,
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
            : new Object[]{"[\"a\",\"b\",\"b\",\"c\",\"d\",\"\",null,null]", "[\"\",null,\"a\",\"b\",\"c\",\"d\"]"}
        )
    );
  }

  @Test
  public void testArrayAggNumeric() throws Exception
  {
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
                              "<LONG>[]",
                              "<LONG>[]",
                              true,
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
                              "<LONG>[]",
                              "<LONG>[]",
                              true,
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
                              "<DOUBLE>[]",
                              "<DOUBLE>[]",
                              true,
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
                              "<DOUBLE>[]",
                              "<DOUBLE>[]",
                              true,
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
                              "<DOUBLE>[]",
                              "<DOUBLE>[]",
                              true,
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
                              "<DOUBLE>[]",
                              "<DOUBLE>[]",
                              true,
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
                "[1.0,0.0,1.7]",
                "[1.0,0.10000000149011612,0.0,0.0,0.0,0.0]",
                "[1.0,0.10000000149011612,0.0]"
            }
            : new Object[]{
                "[7,325323,0,null,null,null]",
                "[0,null,7,325323]",
                "[1.0,1.7,0.0,null,null,null]",
                "[1.0,0.0,null,1.7]",
                "[1.0,0.10000000149011612,0.0,null,null,null]",
                "[1.0,0.10000000149011612,0.0,null]"
            }
        )
    );
  }

  @Test
  public void testArrayAggToString() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT ARRAY_TO_STRING(ARRAY_AGG(DISTINCT dim1), ',') FROM foo WHERE dim1 is not null",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(not(selector("dim1", null, null)))
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("dim1"),
                              "__acc",
                              "[]",
                              "[]",
                              true,
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
            useDefault ? new Object[]{"1,2,abc,def,10.1"} : new Object[]{",1,2,abc,def,10.1"}
        )
    );
  }

  @Test
  public void testArrayAggExpression() throws Exception
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
                      expressionVirtualColumn("v0", "concat(\"dim1\",\"dim2\")", ValueType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new ExpressionLambdaAggregatorFactory(
                              "a0",
                              ImmutableSet.of("v0"),
                              "__acc",
                              "[]",
                              "[]",
                              true,
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
            useDefault ? new Object[]{"1a,a,2,abc,10.1,defabc"} : new Object[]{"null,1a,a,2,defabc"}
        )
    );
  }

  @Test
  public void testArrayAggMaxBytes() throws Exception
  {
    cannotVectorize();
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
                              "<LONG>[]",
                              "<LONG>[]",
                              true,
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
                              "<LONG>[]",
                              "<LONG>[]",
                              true,
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
            : new Object[]{"[7,325323,0,null,null,null]", "[0,null,7,325323]"}
        )
    );
  }

  @Test
  public void testArrayAggAsArrayFromJoin() throws Exception
  {
    cannotVectorize();
    List<Object[]> expectedResults;
    if (useDefault) {
      expectedResults = ImmutableList.of(
          new Object[]{"a", "[\"2\",\"10.1\"]", "2,10.1"},
          new Object[]{"a", "[\"2\",\"10.1\"]", "2,10.1"},
          new Object[]{"a", "[\"2\",\"10.1\"]", "2,10.1"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"}
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{"a", "[\"\",\"2\",\"10.1\"]", ",2,10.1"},
          new Object[]{"a", "[\"\",\"2\",\"10.1\"]", ",2,10.1"},
          new Object[]{"a", "[\"\",\"2\",\"10.1\"]", ",2,10.1"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"},
          new Object[]{"b", "[\"1\",\"abc\",\"def\"]", "1,abc,def"}
      );
    }
    testQuery(
        "SELECT numfoo.dim4, j.arr, ARRAY_TO_STRING(j.arr, ',') FROM numfoo INNER JOIN (SELECT dim4, ARRAY_AGG(DISTINCT dim1) as arr FROM numfoo WHERE dim1 is not null GROUP BY 1) as j ON numfoo.dim4 = j.dim4",
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
                                          .setDimFilter(not(selector("dim1", null, null)))
                                          .setDimensions(new DefaultDimensionSpec("dim4", "_d0"))
                                          .setAggregatorSpecs(
                                              aggregators(
                                                  new ExpressionLambdaAggregatorFactory(
                                                      "a0",
                                                      ImmutableSet.of("dim1"),
                                                      "__acc",
                                                      "[]",
                                                      "[]",
                                                      true,
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
                      expressionVirtualColumn("v0", "array_to_string(\"j0.a0\",',')", ValueType.STRING)
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
  public void testArrayAggGroupByArrayAggFromSubquery() throws Exception
  {
    cannotVectorize();
    // yo, can't group on array types right now so expect failure
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Cannot create query type helper from invalid type [STRING_ARRAY]");
    testQuery(
        "SELECT dim2, arr, COUNT(*) FROM (SELECT dim2, ARRAY_AGG(DISTINCT dim1) as arr FROM foo WHERE dim1 is not null GROUP BY 1 LIMIT 5) GROUP BY 1,2",
        ImmutableList.of(),
        ImmutableList.of()
    );
  }

  @Test
  public void testArrayAggArrayContainsSubquery() throws Exception
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
                                    .filters(not(selector("dim1", null, null)))
                                    .aggregators(
                                        aggregators(
                                            new ExpressionLambdaAggregatorFactory(
                                                "a0",
                                                ImmutableSet.of("dim1"),
                                                "__acc",
                                                "[]",
                                                "[]",
                                                true,
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
  public void testArrayAggGroupByArrayContainsSubquery() throws Exception
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
                                          .filters(not(selector("dim1", null, null)))
                                          .aggregators(
                                              aggregators(
                                                  new ExpressionLambdaAggregatorFactory(
                                                      "a0",
                                                      ImmutableSet.of("dim1"),
                                                      "__acc",
                                                      "[]",
                                                      "[]",
                                                      true,
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
}
