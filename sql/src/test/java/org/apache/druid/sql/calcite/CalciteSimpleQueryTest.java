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
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

/**
 * This class tests simple aggregation SQL queries, i.e., no joins and no nested queries.
 */
public class CalciteSimpleQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testGroupByTimeAndDim()
  {
    testQuery(
        "SELECT FLOOR(__time TO MONTH), dim2, SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d0", 0, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), "", 2L},
            new Object[]{timestamp("2000-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "", 1L},
            new Object[]{timestamp("2001-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "abc", 1L}
        )
        : ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), null, 1L},
            new Object[]{timestamp("2000-01-01"), "", 1L},
            new Object[]{timestamp("2000-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), null, 1L},
            new Object[]{timestamp("2001-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "abc", 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTime()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
        : ImmutableList.of(
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
  public void testGroupByDimAndTimeWhereOnTime()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "WHERE FLOOR(__time TO MONTH) = TIMESTAMP '2001-01-01'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Intervals.of("2001-01-01/P1M")))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
        : ImmutableList.of(
            new Object[]{null, timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTimeOnDim()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "WHERE dim2 LIKE 'a%'\n"
        + "GROUP BY 1, 2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setDimFilter(new LikeDimFilter("dim2", "a%", null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
        : ImmutableList.of(
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupByTimeAndDimOrderByDim()
  {
    testQuery(
        "SELECT FLOOR(__time TO MONTH), dim2, SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY dim2",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", Direction.ASCENDING)),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d0", 0, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), "", 2L},
            new Object[]{timestamp("2001-01-01"), "", 1L},
            new Object[]{timestamp("2000-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "abc", 1L}
        )
        : ImmutableList.of(
            new Object[]{timestamp("2000-01-01"), null, 1L},
            new Object[]{timestamp("2001-01-01"), null, 1L},
            new Object[]{timestamp("2000-01-01"), "", 1L},
            new Object[]{timestamp("2000-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "abc", 1L}
        )
    );
  }

  @Test
  public void testGroupByTimeAndDimOrderByDimDesc()
  {
    testQuery(
        "SELECT FLOOR(__time TO MONTH), dim2, SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY dim2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ColumnType.LONG),
                                new DefaultDimensionSpec("dim2", "d1")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(new OrderByColumnSpec("d1", Direction.DESCENDING)),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d0", 0, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{timestamp("2001-01-01"), "abc", 1L},
            new Object[]{timestamp("2000-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "a", 1L},
            new Object[]{timestamp("2000-01-01"), "", 2L},
            new Object[]{timestamp("2001-01-01"), "", 1L}
        )
        : ImmutableList.of(
            new Object[]{timestamp("2001-01-01"), "abc", 1L},
            new Object[]{timestamp("2000-01-01"), "a", 1L},
            new Object[]{timestamp("2001-01-01"), "a", 1L},
            new Object[]{timestamp("2000-01-01"), "", 1L},
            new Object[]{timestamp("2000-01-01"), null, 1L},
            new Object[]{timestamp("2001-01-01"), null, 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTimeOrderByTime()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY FLOOR(__time TO MONTH)",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", Direction.ASCENDING, StringComparators.NUMERIC)
                                ),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
        : ImmutableList.of(
            new Object[]{null, timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{null, timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTimeOrderByTimeDesc()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY FLOOR(__time TO MONTH) DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", Direction.DESCENDING, StringComparators.NUMERIC)
                                ),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"a", timestamp("2000-01-01"), 1L}
        )
        : ImmutableList.of(
            new Object[]{null, timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{null, timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTimeOrderByTimeAndDim()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY FLOOR(__time TO MONTH), dim2 DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d1", Direction.ASCENDING, StringComparators.NUMERIC),
                                    new OrderByColumnSpec("d0", Direction.DESCENDING)
                                ),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"", timestamp("2001-01-01"), 1L}
        )
        : ImmutableList.of(
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 1L},
            new Object[]{null, timestamp("2000-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{null, timestamp("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTimeOrderByDimAndTime()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2\n"
        + "ORDER BY dim2, FLOOR(__time TO MONTH) DESC",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG)
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d0", Direction.ASCENDING),
                                    new OrderByColumnSpec("d1", Direction.DESCENDING, StringComparators.NUMERIC)
                                ),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"", timestamp("2001-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 2L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
        : ImmutableList.of(
            new Object[]{null, timestamp("2001-01-01"), 1L},
            new Object[]{null, timestamp("2000-01-01"), 1L},
            new Object[]{"", timestamp("2000-01-01"), 1L},
            new Object[]{"a", timestamp("2001-01-01"), 1L},
            new Object[]{"a", timestamp("2000-01-01"), 1L},
            new Object[]{"abc", timestamp("2001-01-01"), 1L}
        )
    );
  }

  @Test
  public void testGroupByDimAndTimeAndDimOrderByDimAndTimeDim()
  {
    testQuery(
        "SELECT dim2, FLOOR(__time TO MONTH), dim1, SUM(cnt)\n"
        + "FROM druid.foo\n"
        + "GROUP BY 1, 2, 3\n"
        + "ORDER BY dim2 DESC, FLOOR(__time TO MONTH) DESC, dim1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "timestamp_floor(\"__time\",'P1M',null,'UTC')",
                                ColumnType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ColumnType.LONG),
                                new DefaultDimensionSpec("dim1", "d2")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec("d0", Direction.DESCENDING),
                                    new OrderByColumnSpec("d1", Direction.DESCENDING, StringComparators.NUMERIC),
                                    new OrderByColumnSpec("d2", Direction.ASCENDING)
                                    ),
                                null
                            )
                        )
                        .setContext(withTimestampResultContext(QUERY_CONTEXT_DEFAULT, "d1", 1, Granularities.MONTH))
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"abc", timestamp("2001-01-01"), "def", 1L},
            new Object[]{"a", timestamp("2001-01-01"), "1", 1L},
            new Object[]{"a", timestamp("2000-01-01"), "", 1L},
            new Object[]{"", timestamp("2001-01-01"), "abc", 1L},
            new Object[]{"", timestamp("2000-01-01"), "10.1", 1L},
            new Object[]{"", timestamp("2000-01-01"), "2", 1L}
        )
        : ImmutableList.of(
            new Object[]{"abc", timestamp("2001-01-01"), "def", 1L},
            new Object[]{"a", timestamp("2001-01-01"), "1", 1L},
            new Object[]{"a", timestamp("2000-01-01"), "", 1L},
            new Object[]{"", timestamp("2000-01-01"), "2", 1L},
            new Object[]{null, timestamp("2001-01-01"), "abc", 1L},
            new Object[]{null, timestamp("2000-01-01"), "10.1", 1L}
        )
    );
  }

  @Test
  public void testEarliestByLatestByWithExpression()
  {
    testBuilder()
        .sql("SELECT\n"
            + "  channel\n"
            + " ,cityName\n"
            + " ,EARLIEST_BY(\"cityName\", MILLIS_TO_TIMESTAMP(17), 125) as latest_by_time_page\n"
            + " ,LATEST_BY(\"cityName\", MILLIS_TO_TIMESTAMP(17), 126) as latest_by_time_page\n"
            + " ,EARLIEST_BY(\"cityName\", TIMESTAMPADD(HOUR, 1, \"__time\"), 127) as latest_by_time_page\n"
            + " ,LATEST_BY(\"cityName\", TIMESTAMPADD(HOUR, 1, \"__time\"), 128) as latest_by_time_page\n"
            + "FROM druid.wikipedia\n"
            + "where channel < '#b' and cityName < 'B'\n"
            + "GROUP BY 1,2"
            )
        .expectedQueries(
            ImmutableList.of(
                GroupByQuery.builder()
                    .setDataSource(CalciteTests.WIKIPEDIA)
                    .setInterval(querySegmentSpec(Filtration.eternity()))
                    .setGranularity(Granularities.ALL)
                    .setVirtualColumns(
                        expressionVirtualColumn("v0", "17", ColumnType.LONG),
                        expressionVirtualColumn("v1", "(\"__time\" + 3600000)", ColumnType.LONG)
                        )
                    .setDimensions(dimensions(new DefaultDimensionSpec("channel", "d0"),
                        new DefaultDimensionSpec("cityName", "d1")))
                    .setDimFilter(
                        and(range("channel", ColumnType.STRING, null, "#b", false, true),
                            range("cityName", ColumnType.STRING, null, "B", false, true)))
                    .setAggregatorSpecs(
                        ImmutableList.of(
                            new StringFirstAggregatorFactory("a0", "cityName", "v0", 125),
                            new StringLastAggregatorFactory("a1", "cityName", "v0", 126),
                            new StringFirstAggregatorFactory("a2", "cityName", "v1", 127),
                            new StringLastAggregatorFactory("a3", "cityName", "v1", 128)
                        )
                        )
                    .setContext(QUERY_CONTEXT_DEFAULT)
                    .build()))
        .expectedResults(
            useDefault ? ImmutableList.of(
                new Object[]{"#ar.wikipedia", "", "", "", "", ""},
                new Object[] {"#ar.wikipedia", "Amman", "Amman", "Amman", "Amman", "Amman"})
            : ImmutableList.of(
                new Object[] {"#ar.wikipedia", "Amman", "Amman", "Amman", "Amman", "Amman"})
        )
        .run();
  }
}
