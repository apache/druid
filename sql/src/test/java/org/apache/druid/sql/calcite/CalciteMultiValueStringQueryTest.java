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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CalciteMultiValueStringQueryTest extends BaseCalciteQueryTest
{
  // various queries on multi-valued string dimensions using them like strings
  @Test
  public void testMultiValueStringWorksLikeStringGroupBy()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();
    Map<String, Object> groupByOnMultiValueColumnEnabled = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    groupByOnMultiValueColumnEnabled.put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, true);
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
        groupByOnMultiValueColumnEnabled,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ColumnType.STRING))
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
        expected
    );
  }

  @Test
  public void testMultiValueStringGroupByDoesNotWork()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();
    Map<String, Object> groupByOnMultiValueColumnDisabled = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    groupByOnMultiValueColumnDisabled.put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false);
    testQueryThrows(
        "SELECT concat(dim3, 'foo'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        groupByOnMultiValueColumnDisabled,
        ImmutableList.of(),
        exception -> {
          exception.expect(RuntimeException.class);
          expectedException.expectMessage(StringUtils.format(
              "Encountered multi-value dimension [%s] that cannot be processed with '%s' set to false."
              + " Consider setting '%s' to true in your query context.",
              "v0",
              GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING,
              GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING
          ));
        }
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringGroupByWithFilter()
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
                        .setVirtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ColumnType.STRING))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.STRING)
                            )
                        )
                        .setDimFilter(selector("v0", "bfoo", null))
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
            new Object[]{"bfoo", 2L},
            new Object[]{"afoo", 1L},
            new Object[]{"cfoo", 1L}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringScan()
  {
    final String nullVal = NullHandling.replaceWithDefault() ? "foo" : null;
    testQuery(
        "SELECT concat(dim3, 'foo') FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ColumnType.STRING))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"afoo\",\"bfoo\"]"},
            new Object[]{"[\"bfoo\",\"cfoo\"]"},
            new Object[]{"dfoo"},
            new Object[]{"foo"},
            new Object[]{nullVal},
            new Object[]{nullVal}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringSelfConcatScan()
  {
    final String nullVal = NullHandling.replaceWithDefault() ? "-lol-" : null;
    testQuery(
        "SELECT concat(dim3, '-lol-', dim3) FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'-lol-',\"dim3\")", ColumnType.STRING))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"[\"a-lol-a\",\"b-lol-b\"]"},
            new Object[]{"[\"b-lol-b\",\"c-lol-c\"]"},
            new Object[]{"d-lol-d"},
            new Object[]{"-lol-"},
            new Object[]{nullVal},
            new Object[]{nullVal}
        )
    );
  }

  @Test
  public void testMultiValueStringWorksLikeStringScanWithFilter()
  {
    testQuery(
        "SELECT concat(dim3, 'foo') FROM druid.numfoo where concat(dim3, 'foo') = 'bfoo'",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
                .virtualColumns(expressionVirtualColumn("v0", "concat(\"dim3\",'foo')", ColumnType.STRING))
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

  // these are a copy of the ARRAY functions tests in CalciteArraysQueryTest
  @Test
  public void testMultiValueStringOverlapFilter()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_OVERLAP(dim3, ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
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
  public void testMultiValueStringOverlapFilterNonLiteral()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_OVERLAP(dim3, ARRAY[dim2]) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
                .filters(expressionFilter("array_overlap(\"dim3\",array(\"dim2\"))"))
                .columns("dim3")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(5)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{"[\"a\",\"b\"]"})
    );
  }

  @Test
  public void testMultiValueStringContainsFilter()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_CONTAINS(dim3, ARRAY['a','b']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
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
  public void testMultiValueStringContainsArrayOfOneElement()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_CONTAINS(dim3, ARRAY['a']) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
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
  public void testMultiValueStringContainsArrayOfNonLiteral()
  {
    testQuery(
        "SELECT dim3 FROM druid.numfoo WHERE MV_CONTAINS(dim3, ARRAY[dim2]) LIMIT 5",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
                .filters(expressionFilter("array_contains(\"dim3\",array(\"dim2\"))"))
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
  public void testMultiValueStringSlice()
  {
    testQuery(
        "SELECT MV_SLICE(dim3, 1) FROM druid.numfoo",
        ImmutableList.of(
            new Druids.ScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE3)
                .eternityInterval()
                .virtualColumns(expressionVirtualColumn("v0", "array_slice(\"dim3\",1)", ColumnType.STRING))
                .columns(ImmutableList.of("v0"))
                .context(QUERY_CONTEXT_DEFAULT)
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .legacy(false)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"b"},
            new Object[]{"c"},
            new Object[]{"[]"},
            new Object[]{useDefault ? NULL_STRING : "[]"},
            new Object[]{NULL_STRING},
            new Object[]{NULL_STRING}
        )
    );
  }

  @Test
  public void testMultiValueStringLength()
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
  public void testMultiValueStringAppend()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"", 3L},
          new Object[]{"foo", 3L},
          new Object[]{"b", 2L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L},
          new Object[]{"d", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"foo", 4L},
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
  public void testMultiValueStringPrepend()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"", 3L},
          new Object[]{"foo", 3L},
          new Object[]{"b", 2L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L},
          new Object[]{"d", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"foo", 4L},
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
  public void testMultiValueStringPrependAppend()
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
  public void testMultiValueStringConcat()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"b", 4L},
          new Object[]{"", 3L},
          new Object[]{"a", 2L},
          new Object[]{"c", 2L},
          new Object[]{"d", 2L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"b", 4L},
          new Object[]{null, 2L},
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
  public void testMultiValueStringConcatBackwardsCompat0dot22andOlder()
  {
    try {
      ExpressionProcessing.initializeForHomogenizeNullMultiValueStrings();
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
          "SELECT MV_CONCAT(dim3, dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
          ImmutableList.of(
              GroupByQuery.builder()
                          .setDataSource(CalciteTests.DATASOURCE3)
                          .setInterval(querySegmentSpec(Filtration.eternity()))
                          .setGranularity(Granularities.ALL)
                          .setVirtualColumns(expressionVirtualColumn(
                              "v0",
                              "array_concat(\"dim3\",\"dim3\")",
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
    finally {
      ExpressionProcessing.initializeForTests(null);
    }
  }

  @Test
  public void testMultiValueStringOffset()
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
  public void testMultiValueStringOrdinal()
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
                        .setVirtualColumns(expressionVirtualColumn("v0", "array_ordinal(\"dim3\",2)", ColumnType.STRING))
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
  public void testMultiValueStringOffsetOf()
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
  public void testMultiValueStringOrdinalOf()
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
  public void testMultiValueStringToString()
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
        "SELECT MV_TO_STRING(dim3, ','), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
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
  public void testMultiValueStringToStringToMultiValueString()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    ImmutableList<Object[]> results;
    if (useDefault) {
      results = ImmutableList.of(
          new Object[]{"d", 4L},
          new Object[]{"b", 2L},
          new Object[]{"a", 1L},
          new Object[]{"c", 1L}
      );
    } else {
      results = ImmutableList.of(
          new Object[]{"d", 5L},
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
                            expressionVirtualColumn("v0", "array_length(\"dim3\")", ColumnType.LONG),
                            expressionVirtualColumn(
                                "v1",
                                "string_to_array(concat(array_to_string(\"dim3\",','),',d'),',')",
                                ColumnType.STRING
                            )
                        )
                        .setDimFilter(bound("v0", "0", null, true, false, null, StringComparators.NUMERIC))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v1", "_d0", ColumnType.STRING)
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
  public void testMultiValueListFilter()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_FILTER_ONLY(dim3, ARRAY['b']), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ListFilteredVirtualColumn(
                                "v0",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
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
            new Object[]{"b", 2L}
        )
    );
  }

  @Test
  public void testMultiValueListFilterDeny()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_FILTER_NONE(dim3, ARRAY['b']), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ListFilteredVirtualColumn(
                                "v0",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                false
                            )
                        )
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
        useDefault ?
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 3L},
            new Object[]{"a", 1L},
            new Object[]{"c", 1L},
            new Object[]{"d", 1L}
        ) :
        ImmutableList.of(
            new Object[]{NullHandling.defaultStringValue(), 2L},
            new Object[]{"", 1L},
            new Object[]{"a", 1L},
            new Object[]{"c", 1L},
            new Object[]{"d", 1L}
        )
    );
  }

  @Test
  public void testMultiValueListFilterComposed()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_LENGTH(MV_FILTER_ONLY(dim3, ARRAY['b'])), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "array_length(\"v1\")",
                                ColumnType.LONG
                            ),
                            new ListFilteredVirtualColumn(
                                "v1",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
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
        useDefault ? ImmutableList.of(
            new Object[]{0, 4L},
            new Object[]{1, 2L}
        ) : ImmutableList.of(
            // the fallback expression would actually produce 3 rows, 2 nulls, 2 0's, and 2 1s
            // instead of 4 nulls and two 1's we get when using the 'native' list filtered virtual column
            // this is because of slight differences between filter and the native
            // selector, which treats a 0 length array as null instead of an empty array like is produced by filter
            new Object[]{null, 4L},
            new Object[]{1, 2L}
        )
    );
  }

  @Test
  public void testMultiValueListFilterComposedNested()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT COALESCE(MV_FILTER_ONLY(dim3, ARRAY['b']), 'no b'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"v1\"),\"v1\",'no b')",
                                ColumnType.STRING
                            ),
                            new ListFilteredVirtualColumn(
                                "v1",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
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
        // this behavior is strange - you might be expecting instead of default values it should actually be 'no b', but
        // instead we end up with something else
        //
        // it happens when using 'notnull' on the mv-filtered virtual column because deferred expression selector
        // returns a 0 sized row, which is treated as a missing value by the grouping, and so the expression which would
        // evaluate and return 'no b' is never evaluated. If we were not using the deferred selector, the results would
        // be 'no b' because IndexedInts representing [], [null], and null are homogenized into [null] to handle
        // variation between segments.
        //
        // if the 'notnull' was instead using the array filtering fallback expression
        // case_searched(notnull(filter((x) -> array_contains(array('b'), x), \"dim3\")),\"v1\",'no b')
        // where it doesn't use the deferred selector because it is no longer a single input expression, it would still
        // evaluate to null because the filter expression never returns null, only an empty array, which is not null,
        // so it evaluates 'v1' which of course is null because it is an empty row
        useDefault ? ImmutableList.of(
            new Object[]{"", 4L},
            new Object[]{"b", 2L}
        ) : ImmutableList.of(
            new Object[]{null, 4L},
            new Object[]{"b", 2L}
        )
    );
  }

  @Test
  public void testMultiValueListFilterComposedNested2Input()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT COALESCE(MV_FILTER_ONLY(dim3, ARRAY['b']), dim1), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"v1\"),\"v1\",\"dim1\")",
                                ColumnType.STRING
                            ),
                            new ListFilteredVirtualColumn(
                                "v1",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
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
        // with 2 inputs, the non-deferred selector is used, and so the values of dim1 are used for all of the 'null'
        // values returned by MV_FILTER_ONLY
        ImmutableList.of(
            new Object[]{"b", 2L},
            new Object[]{"1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
    );
  }

  @Test
  public void testMultiValueListFilterComposedNestedNullLiteral()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();
    Set<String> filter = new HashSet<>();
    filter.add(null);
    filter.add("b");

    testQuery(
        "SELECT COALESCE(MV_FILTER_ONLY(dim3, ARRAY[NULL, 'b']), 'no b'), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"v1\"),\"v1\",'no b')",
                                ColumnType.STRING
                            ),
                            new ListFilteredVirtualColumn(
                                "v1",
                                DefaultDimensionSpec.of("dim3"),
                                filter,
                                true
                            )
                        )
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
        // unfortunately, unable to work around the strange behavior by adding nulls to the allow list, since not all of the values are actually
        // [], so some of them end up as 'no b'
        useDefault ? ImmutableList.of(
            new Object[]{"", 2L},
            new Object[]{"b", 2L},
            new Object[]{"no b", 2L}
        ) : ImmutableList.of(
            new Object[]{null, 3L},
            new Object[]{"b", 2L},
            new Object[]{"no b", 1L}
        )
    );
  }

  @Test
  public void testMultiValueListFilterComposedDeny()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_LENGTH(MV_FILTER_NONE(dim3, ARRAY['b'])), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "array_length(\"v1\")",
                                ColumnType.LONG
                            ),
                            new ListFilteredVirtualColumn(
                                "v1",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                false
                            )
                        )
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
        ? ImmutableList.of(new Object[]{0, 3L}, new Object[]{1, 3L})
        : ImmutableList.of(new Object[]{1, 4L}, new Object[]{null, 2L})
    );
  }

  @Test
  public void testMultiValueListFilterComposedMultipleExpressions()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT MV_LENGTH(MV_FILTER_ONLY(dim3, ARRAY['b'])), MV_LENGTH(dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1,2 ORDER BY 3 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "array_length(\"v2\")",
                                ColumnType.LONG
                            ),
                            expressionVirtualColumn(
                                "v1",
                                "array_length(\"dim3\")",
                                ColumnType.LONG
                            ),
                            new ListFilteredVirtualColumn(
                                "v2",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "_d0", ColumnType.LONG),
                                new DefaultDimensionSpec("v1", "_d1", ColumnType.LONG)
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
        useDefault ? ImmutableList.of(
            new Object[]{0, 0, 3L},
            new Object[]{1, 2, 2L},
            new Object[]{0, 1, 1L}
        ) : ImmutableList.of(
            new Object[]{null, null, 2L},
            new Object[]{null, 1, 2L},
            new Object[]{1, 2, 2L}
        )
    );
  }

  @Test
  public void testFilterOnMultiValueListFilterNoMatch()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT dim3, SUM(cnt) FROM druid.numfoo WHERE MV_FILTER_ONLY(dim3, ARRAY['b']) = 'a' GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ListFilteredVirtualColumn(
                                "v0",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
                        .setDimFilter(selector("v0", "a", null))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim3", "_d0", ColumnType.STRING)
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
        ImmutableList.of()
    );
  }

  @Test
  public void testFilterOnMultiValueListFilterMatch()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT dim3, SUM(cnt) FROM druid.numfoo WHERE MV_FILTER_ONLY(dim3, ARRAY['b']) = 'b' GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ListFilteredVirtualColumn(
                                "v0",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
                        .setDimFilter(selector("v0", "b", null))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim3", "_d0", ColumnType.STRING)
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
            new Object[]{"b", 2L},
            new Object[]{"a", 1L},
            new Object[]{"c", 1L}
        )
    );
  }

  @Test
  public void testFilterOnMultiValueListFilterMatchLike()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();

    testQuery(
        "SELECT dim3, SUM(cnt) FROM druid.numfoo WHERE MV_FILTER_ONLY(dim3, ARRAY['b']) LIKE 'b%' GROUP BY 1 ORDER BY 2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ListFilteredVirtualColumn(
                                "v0",
                                DefaultDimensionSpec.of("dim3"),
                                ImmutableSet.of("b"),
                                true
                            )
                        )
                        .setDimFilter(new LikeDimFilter("v0", "b%", null, null))
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim3", "_d0", ColumnType.STRING)
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
            new Object[]{"b", 2L},
            new Object[]{"a", 1L},
            new Object[]{"c", 1L}
        )
    );
  }

  @Test
  public void testMultiValueToArrayGroupAsArrayWithMultiValueDimension()
  {
    // Cannot vectorize as we donot have support in native query subsytem for grouping on arrays as keys
    cannotVectorize();
    testQuery(
        "SELECT MV_TO_ARRAY(dim3), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "mv_to_array(\"dim3\")",
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
        useDefault ? ImmutableList.of(
            new Object[]{null, 3L},
            new Object[]{ImmutableList.of("a", "b"), 1L},
            new Object[]{ImmutableList.of("b", "c"), 1L},
            new Object[]{ImmutableList.of("d"), 1L}
        ) :
        ImmutableList.of(
            new Object[]{null, 2L},
            new Object[]{ImmutableList.of(""), 1L},
            new Object[]{ImmutableList.of("a", "b"), 1L},
            new Object[]{ImmutableList.of("b", "c"), 1L},
            new Object[]{ImmutableList.of("d"), 1L}
        )
    );
  }


  @Test
  public void testMultiValueToArrayGroupAsArrayWithSingleValueDim()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();
    testQuery(
        "SELECT MV_TO_ARRAY(dim1), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "mv_to_array(\"dim1\")",
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
        useDefault ? ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{ImmutableList.of("1"), 1L},
            new Object[]{ImmutableList.of("10.1"), 1L},
            new Object[]{ImmutableList.of("2"), 1L},
            new Object[]{ImmutableList.of("abc"), 1L},
            new Object[]{ImmutableList.of("def"), 1L}
        ) :
        ImmutableList.of(
            new Object[]{ImmutableList.of(""), 1L},
            new Object[]{ImmutableList.of("1"), 1L},
            new Object[]{ImmutableList.of("10.1"), 1L},
            new Object[]{ImmutableList.of("2"), 1L},
            new Object[]{ImmutableList.of("abc"), 1L},
            new Object[]{ImmutableList.of("def"), 1L}
        )
    );
  }

  @Test
  public void testMultiValueToArrayGroupAsArrayWithSingleValueDimIsNotConvertedToTopN()
  {
    // Cannot vectorize due to usage of expressions.
    cannotVectorize();
    // Test for method {@link org.apache.druid.sql.calcite.rel.DruidQuery.toTopNQuery()} so that it does not convert
    // group by on array to topn
    testQuery(
        "SELECT MV_TO_ARRAY(dim1), SUM(cnt) FROM druid.numfoo GROUP BY 1 ORDER BY 2 DESC limit 10",
        QUERY_CONTEXT_NO_STRINGIFY_ARRAY,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE3)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(expressionVirtualColumn(
                            "v0",
                            "mv_to_array(\"dim1\")",
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
                            10
                        ))
                        .setContext(QUERY_CONTEXT_NO_STRINGIFY_ARRAY)
                        .build()
        ),
        useDefault ? ImmutableList.of(
            new Object[]{null, 1L},
            new Object[]{ImmutableList.of("1"), 1L},
            new Object[]{ImmutableList.of("10.1"), 1L},
            new Object[]{ImmutableList.of("2"), 1L},
            new Object[]{ImmutableList.of("abc"), 1L},
            new Object[]{ImmutableList.of("def"), 1L}
        ) :
        ImmutableList.of(
            new Object[]{ImmutableList.of(""), 1L},
            new Object[]{ImmutableList.of("1"), 1L},
            new Object[]{ImmutableList.of("10.1"), 1L},
            new Object[]{ImmutableList.of("2"), 1L},
            new Object[]{ImmutableList.of("abc"), 1L},
            new Object[]{ImmutableList.of("def"), 1L}
        )
    );
  }

  @Test
  public void testMultiValueToArrayMoreArgs()
  {
    testQueryThrows(
        "SELECT MV_TO_ARRAY(dim3,dim3) FROM druid.numfoo",
        exception -> {
          exception.expect(SqlPlanningException.class);
          exception.expectMessage("Invalid number of arguments to function");
        }
    );
  }

  @Test
  public void testMultiValueToArrayNoArgs()
  {
    testQueryThrows(
        "SELECT MV_TO_ARRAY() FROM druid.numfoo",
        exception -> {
          exception.expect(SqlPlanningException.class);
          exception.expectMessage("Invalid number of arguments to function");
        }
    );
  }

  @Test
  public void testMultiValueToArrayArgsWithMultiValueDimFunc()
  {
    testQueryThrows(
        "SELECT MV_TO_ARRAY(concat(dim3,'c')) FROM druid.numfoo",
        exception -> exception.expect(RuntimeException.class)
    );
  }

  @Test
  public void testMultiValueToArrayArgsWithSingleDimFunc()
  {
    testQueryThrows(
        "SELECT MV_TO_ARRAY(concat(dim1,'c')) FROM druid.numfoo",
        exception -> exception.expect(RuntimeException.class)
    );
  }

  @Test
  public void testMultiValueToArrayArgsWithConstant()
  {
    testQueryThrows(
        "SELECT MV_TO_ARRAY(concat(dim1,'c')) FROM druid.numfoo",
        exception -> exception.expect(RuntimeException.class)
    );
  }

  @Test
  public void testMultiValueToArrayArgsWithArray()
  {
    testQueryThrows(
        "SELECT MV_TO_ARRAY(Array[1,2]) FROM druid.numfoo",
        exception -> exception.expect(RuntimeException.class)
    );
  }
}
