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
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.AllGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

@RunWith(JUnitParamsRunner.class)
public class CalciteCorrelatedQueryTest extends BaseCalciteQueryTest
{

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCorrelatedSubquery(Map<String, Object> queryContext)
  {
    cannotVectorize();
    queryContext = withLeftDirectAccessEnabled(queryContext);

    testQuery(
        "select country, ANY_VALUE(\n"
        + "        select avg(\"users\") from (\n"
        + "            select floor(__time to day), count(distinct user) \"users\" from visits f where f.country = visits.country group by 1\n"
        + "        )\n"
        + "     ) as \"DAU\"\n"
        + "from visits \n"
        + "group by 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.USERVISITDATASOURCE),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(
                                                    GroupByQuery.builder()
                                                                .setDataSource(CalciteTests.USERVISITDATASOURCE)
                                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                                .setVirtualColumns(new ExpressionVirtualColumn(
                                                                    "v0",
                                                                    "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                                    ColumnType.LONG,
                                                                    TestExprMacroTable.INSTANCE
                                                                ))
                                                                .setDimFilter(not(selector("country", null, null)))
                                                                .setDimensions(
                                                                    new DefaultDimensionSpec(
                                                                        "v0",
                                                                        "d0",
                                                                        ColumnType.LONG
                                                                    ),
                                                                    new DefaultDimensionSpec(
                                                                        "country",
                                                                        "d1"
                                                                    )
                                                                )
                                                                .setAggregatorSpecs(new CardinalityAggregatorFactory(
                                                                    "a0:a",
                                                                    null,
                                                                    Collections.singletonList(
                                                                        new DefaultDimensionSpec(
                                                                            "user",
                                                                            "user"
                                                                        )),
                                                                    false,
                                                                    true
                                                                ))
                                                                .setPostAggregatorSpecs(Collections.singletonList(new HyperUniqueFinalizingPostAggregator(
                                                                    "a0",
                                                                    "a0:a"
                                                                )))
                                                                .setContext(
                                                                    withTimestampResultContext(
                                                                        queryContext,
                                                                        "d0",
                                                                        Granularities.DAY
                                                                    )
                                                                )
                                                                .setGranularity(new AllGranularity())
                                                                .build()
                                                )
                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                                .setAggregatorSpecs(
                                                    new LongSumAggregatorFactory("_a0:sum", "a0"),
                                                    useDefault
                                                    ? new CountAggregatorFactory("_a0:count")
                                                    : new FilteredAggregatorFactory(
                                                        new CountAggregatorFactory("_a0:count"),
                                                        not(selector("a0", null, null))
                                                    )
                                                )
                                                .setPostAggregatorSpecs(Collections.singletonList(new ArithmeticPostAggregator(
                                                    "_a0",
                                                    "quotient",
                                                    Arrays
                                                        .asList(
                                                            new FieldAccessPostAggregator(null, "_a0:sum"),
                                                            new FieldAccessPostAggregator(null, "_a0:count")
                                                        )
                                                )))
                                                .setGranularity(new AllGranularity())
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    makeColumnExpression("country"),
                                    makeColumnExpression("j0._d0")
                                ),
                                JoinType.LEFT
                            )
                        )
                        .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                        .setDimensions(new DefaultDimensionSpec("country", "d0"))
                        .setAggregatorSpecs(new LongAnyAggregatorFactory("a0", "j0._a0"))
                        .setGranularity(new AllGranularity())
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"India", 2L},
            new Object[]{"USA", 1L},
            new Object[]{"canada", 3L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCorrelatedSubqueryWithLeftFilter(Map<String, Object> queryContext)
  {
    cannotVectorize();
    queryContext = withLeftDirectAccessEnabled(queryContext);

    testQuery(
        "select country, ANY_VALUE(\n"
        + "        select max(\"users\") from (\n"
        + "            select floor(__time to day), count(*) \"users\" from visits f where f.country = visits.country group by 1\n"
        + "        )\n"
        + "     ) as \"dailyVisits\"\n"
        + "from visits \n"
        + " where city = 'B' and __time between '2021-01-01 01:00:00' AND '2021-01-02 23:59:59'"
        + " group by 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.USERVISITDATASOURCE),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(
                                                    GroupByQuery.builder()
                                                                .setDataSource(CalciteTests.USERVISITDATASOURCE)
                                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                                .setVirtualColumns(new ExpressionVirtualColumn(
                                                                    "v0",
                                                                    "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                                    ColumnType.LONG,
                                                                    TestExprMacroTable.INSTANCE
                                                                ))
                                                                .setDimFilter(not(selector("country", null, null)))
                                                                .setDimensions(
                                                                    new DefaultDimensionSpec(
                                                                        "v0",
                                                                        "d0",
                                                                        ColumnType.LONG
                                                                    ),
                                                                    new DefaultDimensionSpec(
                                                                        "country",
                                                                        "d1"
                                                                    )
                                                                )
                                                                .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                                                                .setContext(
                                                                    withTimestampResultContext(
                                                                        queryContext,
                                                                        "d0",
                                                                        Granularities.DAY
                                                                    )
                                                                )
                                                                .setGranularity(new AllGranularity())
                                                                .build()
                                                )
                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                                .setAggregatorSpecs(
                                                    new LongMaxAggregatorFactory("_a0", "a0")
                                                )
                                                .setGranularity(new AllGranularity())
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    makeColumnExpression("country"),
                                    makeColumnExpression("j0._d0")
                                ),
                                JoinType.LEFT,
                                selector("city", "B", null)
                            )
                        )
                        .setQuerySegmentSpec(querySegmentSpec(Intervals.of(
                            "2021-01-01T01:00:00.000Z/2021-01-02T23:59:59.001Z")))
                        .setDimensions(new DefaultDimensionSpec("country", "d0"))
                        .setAggregatorSpecs(new LongAnyAggregatorFactory("a0", "j0._a0"))
                        .setGranularity(new AllGranularity())
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"canada", 4L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCorrelatedSubqueryWithLeftFilter_leftDirectAccessDisabled(Map<String, Object> queryContext)
  {
    cannotVectorize();

    testQuery(
        "select country, ANY_VALUE(\n"
        + "        select max(\"users\") from (\n"
        + "            select floor(__time to day), count(*) \"users\" from visits f where f.country = visits.country group by 1\n"
        + "        )\n"
        + "     ) as \"dailyVisits\"\n"
        + "from visits \n"
        + " where city = 'B' and __time between '2021-01-01 01:00:00' AND '2021-01-02 23:59:59'"
        + " group by 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new QueryDataSource(newScanQueryBuilder().dataSource(CalciteTests.USERVISITDATASOURCE)
                                                                         .intervals(querySegmentSpec(Intervals.of(
                                                                             "2021-01-01T01:00:00.000Z/2021-01-02T23:59:59.001Z")))
                                                                         .filters(selector("city", "B", null))
                                                                         .columns("__time", "city", "country")
                                                                         .build()),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(
                                                    GroupByQuery.builder()
                                                                .setDataSource(CalciteTests.USERVISITDATASOURCE)
                                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                                .setVirtualColumns(new ExpressionVirtualColumn(
                                                                    "v0",
                                                                    "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                                    ColumnType.LONG,
                                                                    TestExprMacroTable.INSTANCE
                                                                ))
                                                                .setDimFilter(not(selector("country", null, null)))
                                                                .setDimensions(
                                                                    new DefaultDimensionSpec(
                                                                        "v0",
                                                                        "d0",
                                                                        ColumnType.LONG
                                                                    ),
                                                                    new DefaultDimensionSpec(
                                                                        "country",
                                                                        "d1"
                                                                    )
                                                                )
                                                                .setAggregatorSpecs(new CountAggregatorFactory("a0"))
                                                                .setContext(
                                                                    withTimestampResultContext(
                                                                        queryContext,
                                                                        "d0",
                                                                        Granularities.DAY
                                                                    )
                                                                )
                                                                .setGranularity(new AllGranularity())
                                                                .build()
                                                )
                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                                .setAggregatorSpecs(
                                                    new LongMaxAggregatorFactory("_a0", "a0")
                                                )
                                                .setGranularity(new AllGranularity())
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    makeColumnExpression("country"),
                                    makeColumnExpression("j0._d0")
                                ),
                                JoinType.LEFT
                            )
                        )
                        .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                        .setDimensions(new DefaultDimensionSpec("country", "d0"))
                        .setAggregatorSpecs(new LongAnyAggregatorFactory("a0", "j0._a0"))
                        .setGranularity(new AllGranularity())
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"canada", 4L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCorrelatedSubqueryWithCorrelatedQueryFilter(Map<String, Object> queryContext)
  {
    cannotVectorize();
    queryContext = withLeftDirectAccessEnabled(queryContext);

    testQuery(
        "select country, ANY_VALUE(\n"
        + "        select max(\"users\") from (\n"
        + "            select floor(__time to day), count(user) \"users\" from visits f where f.country = visits.country and f.city = 'A' group by 1\n"
        + "        )\n"
        + "     ) as \"dailyVisits\"\n"
        + "from visits \n"
        + " where city = 'B'"
        + " group by 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.USERVISITDATASOURCE),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(
                                                    GroupByQuery.builder()
                                                                .setDataSource(CalciteTests.USERVISITDATASOURCE)
                                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                                .setVirtualColumns(new ExpressionVirtualColumn(
                                                                    "v0",
                                                                    "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                                    ColumnType.LONG,
                                                                    TestExprMacroTable.INSTANCE
                                                                ))
                                                                .setDimensions(
                                                                    new DefaultDimensionSpec(
                                                                        "v0",
                                                                        "d0",
                                                                        ColumnType.LONG
                                                                    ),
                                                                    new DefaultDimensionSpec(
                                                                        "country",
                                                                        "d1"
                                                                    )
                                                                )
                                                                .setAggregatorSpecs(new FilteredAggregatorFactory(
                                                                    new CountAggregatorFactory("a0"),
                                                                    not(selector("user", null, null))
                                                                ))
                                                                .setDimFilter(and(
                                                                    selector("city", "A", null),
                                                                    not(selector("country", null, null))
                                                                ))
                                                                .setContext(
                                                                    withTimestampResultContext(
                                                                        queryContext,
                                                                        "d0",
                                                                        Granularities.DAY
                                                                    )
                                                                )
                                                                .setGranularity(new AllGranularity())
                                                                .build()
                                                )
                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                                .setAggregatorSpecs(
                                                    new LongMaxAggregatorFactory("_a0", "a0")
                                                )
                                                .setGranularity(new AllGranularity())
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    makeColumnExpression("country"),
                                    makeColumnExpression("j0._d0")
                                ),
                                JoinType.LEFT,
                                selector("city", "B", null)
                            )
                        )
                        .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                        .setDimensions(new DefaultDimensionSpec("country", "d0"))
                        .setAggregatorSpecs(new LongAnyAggregatorFactory("a0", "j0._a0"))
                        .setGranularity(new AllGranularity())
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"canada", 2L}
        )
    );
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testCorrelatedSubqueryWithCorrelatedQueryFilter_Scan(Map<String, Object> queryContext)
  {
    cannotVectorize();
    queryContext = withLeftDirectAccessEnabled(queryContext);
    testQuery(
        "select country, ANY_VALUE(\n"
        + "        select max(\"users\") from (\n"
        + "            select floor(__time to day), count(user) \"users\" from visits f where f.country = visits.country and f.city = 'A' group by 1\n"
        + "        )\n"
        + "     ) as \"dailyVisits\"\n"
        + "from visits \n"
        + " where city = 'B'"
        + " group by 1",
        queryContext,
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(
                            join(
                                new TableDataSource(CalciteTests.USERVISITDATASOURCE),
                                new QueryDataSource(
                                    GroupByQuery.builder()
                                                .setDataSource(
                                                    GroupByQuery.builder()
                                                                .setDataSource(CalciteTests.USERVISITDATASOURCE)
                                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                                .setVirtualColumns(new ExpressionVirtualColumn(
                                                                    "v0",
                                                                    "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                                    ColumnType.LONG,
                                                                    TestExprMacroTable.INSTANCE
                                                                ))
                                                                .setDimensions(
                                                                    new DefaultDimensionSpec(
                                                                        "v0",
                                                                        "d0",
                                                                        ColumnType.LONG
                                                                    ),
                                                                    new DefaultDimensionSpec(
                                                                        "country",
                                                                        "d1"
                                                                    )
                                                                )
                                                                .setAggregatorSpecs(new FilteredAggregatorFactory(
                                                                    new CountAggregatorFactory("a0"),
                                                                    not(selector("user", null, null))
                                                                ))
                                                                .setDimFilter(and(
                                                                    selector("city", "A", null),
                                                                    not(selector("country", null, null))
                                                                ))
                                                                .setContext(
                                                                    withTimestampResultContext(
                                                                        queryContext,
                                                                        "d0",
                                                                        Granularities.DAY
                                                                    )
                                                                )
                                                                .setGranularity(new AllGranularity())
                                                                .build()
                                                )
                                                .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                                                .setDimensions(new DefaultDimensionSpec("d1", "_d0"))
                                                .setAggregatorSpecs(
                                                    new LongMaxAggregatorFactory("_a0", "a0")
                                                )
                                                .setGranularity(new AllGranularity())
                                                .setContext(queryContext)
                                                .build()
                                ),
                                "j0.",
                                equalsCondition(
                                    makeColumnExpression("country"),
                                    makeColumnExpression("j0._d0")
                                ),
                                JoinType.LEFT,
                                selector("city", "B", null)
                            )
                        )
                        .setQuerySegmentSpec(querySegmentSpec(Intervals.ETERNITY))
                        .setDimensions(new DefaultDimensionSpec("country", "d0"))
                        .setAggregatorSpecs(new LongAnyAggregatorFactory("a0", "j0._a0"))
                        .setGranularity(new AllGranularity())
                        .setContext(queryContext)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"canada", 2L}
        )
    );
  }

  private Map<String, Object> withTimestampResultContext(
      Map<String, Object> input,
      String timestampResultField,
      Granularity granularity
  )
  {
    return withTimestampResultContext(input, timestampResultField, 0, granularity);
  }
}
