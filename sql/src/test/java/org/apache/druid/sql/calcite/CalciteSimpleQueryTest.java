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
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

/**
 * This class tests simple aggregation SQL queries, i.e., no joins and no nested queries.
 */
public class CalciteSimpleQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testGroupByTimeAndDim() throws Exception
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
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.LONG),
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
  public void testGroupByDimAndTime() throws Exception
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
  public void testGroupByDimAndTimeWhereOnTime() throws Exception
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
  public void testGroupByDimAndTimeOnDim() throws Exception
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
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ValueType.LONG)
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
  public void testGroupByTimeAndDimOrderByDim() throws Exception
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
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.LONG),
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
  public void testGroupByTimeAndDimOrderByDimDesc() throws Exception
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
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0", ValueType.LONG),
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
  public void testGroupByDimAndTimeOrderByTime() throws Exception
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
  public void testGroupByDimAndTimeOrderByTimeDesc() throws Exception
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
  public void testGroupByDimAndTimeOrderByTimeAndDim() throws Exception
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
  public void testGroupByDimAndTimeOrderByDimAndTime() throws Exception
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
  public void testGroupByDimAndTimeAndDimOrderByDimAndTimeDim() throws Exception
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
                                ValueType.LONG
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("dim2", "d0"),
                                new DefaultDimensionSpec("v0", "d1", ValueType.LONG),
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
}
