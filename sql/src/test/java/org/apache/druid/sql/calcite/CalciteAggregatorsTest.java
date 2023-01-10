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
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.any.DoubleAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.FloatAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.LongAnyAggregatorFactory;
import org.apache.druid.query.aggregation.any.StringAnyAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

public class CalciteAggregatorsTest extends BaseCalciteQueryTest
{

  @Test
  public void testGroupByWithPostAggregatorReferencingTimeFloorColumnOnTimeseries()
  {
    cannotVectorize();

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

  @Test
  public void testEarliestAggregators()
  {
    // Cannot vectorize EARLIEST aggregator.
    skipVectorize();

    testQuery(
        "SELECT "
        + "EARLIEST(cnt), EARLIEST(m1), EARLIEST(dim1, 10), "
        + "EARLIEST(cnt + 1), EARLIEST(m1 + 1), EARLIEST(dim1 || CAST(cnt AS VARCHAR), 10), "
        + "EARLIEST_BY(cnt, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(m1, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(dim1, MILLIS_TO_TIMESTAMP(l1), 10), "
        + "EARLIEST_BY(cnt + 1, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(m1 + 1, MILLIS_TO_TIMESTAMP(l1)), EARLIEST_BY(dim1 || CAST(cnt AS VARCHAR), MILLIS_TO_TIMESTAMP(l1), 10) "
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongFirstAggregatorFactory("a0", "cnt", null),
                          new FloatFirstAggregatorFactory("a1", "m1", null),
                          new StringFirstAggregatorFactory("a2", "dim1", null, 10),
                          new LongFirstAggregatorFactory("a3", "v0", null),
                          new FloatFirstAggregatorFactory("a4", "v1", null),
                          new StringFirstAggregatorFactory("a5", "v2", null, 10),
                          new LongFirstAggregatorFactory("a6", "cnt", "l1"),
                          new FloatFirstAggregatorFactory("a7", "m1", "l1"),
                          new StringFirstAggregatorFactory("a8", "dim1", "l1", 10),
                          new LongFirstAggregatorFactory("a9", "v0", "l1"),
                          new FloatFirstAggregatorFactory("a10", "v1", "l1"),
                          new StringFirstAggregatorFactory("a11", "v2", "l1", 10)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 1.0f, "", 2L, 2.0f, "1", 1L, 3.0f, "2", 2L, 4.0f, "21"}
        )
    );
  }

  @Test
  public void testLatestVectorAggregators()
  {
    testQuery(
        "SELECT "
        + "LATEST(cnt), LATEST(cnt + 1), LATEST(m1), LATEST(m1+1) "
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT)
                  )
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "cnt", null),
                          new LongLastAggregatorFactory("a1", "v0", null),
                          new FloatLastAggregatorFactory("a2", "m1", null),
                          new FloatLastAggregatorFactory("a3", "v1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 2L, 6.0f, 7.0f}
        )
    );
  }

  @Test
  public void testLatestAggregators()
  {

    testQuery(
        "SELECT "
        + "LATEST(cnt), LATEST(m1), LATEST(dim1, 10), "
        + "LATEST(cnt + 1), LATEST(m1 + 1), LATEST(dim1 || CAST(cnt AS VARCHAR), 10), "
        + "LATEST_BY(cnt, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(m1, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(dim1, MILLIS_TO_TIMESTAMP(l1), 10), "
        + "LATEST_BY(cnt + 1, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(m1 + 1, MILLIS_TO_TIMESTAMP(l1)), LATEST_BY(dim1 || CAST(cnt AS VARCHAR), MILLIS_TO_TIMESTAMP(l1), 10) "
        + "FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ColumnType.STRING)
                  )
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "cnt", null),
                          new FloatLastAggregatorFactory("a1", "m1", null),
                          new StringLastAggregatorFactory("a2", "dim1", null, 10),
                          new LongLastAggregatorFactory("a3", "v0", null),
                          new FloatLastAggregatorFactory("a4", "v1", null),
                          new StringLastAggregatorFactory("a5", "v2", null, 10),
                          new LongLastAggregatorFactory("a6", "cnt", "l1"),
                          new FloatLastAggregatorFactory("a7", "m1", "l1"),
                          new StringLastAggregatorFactory("a8", "dim1", "l1", 10),
                          new LongLastAggregatorFactory("a9", "v0", "l1"),
                          new FloatLastAggregatorFactory("a10", "v1", "l1"),
                          new StringLastAggregatorFactory("a11", "v2", "l1", 10)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{1L, 6.0f, "abc", 2L, 7.0f, "abc1", 1L, 2.0f, "10.1", 2L, 3.0f, "10.11"}
        )
    );
  }

  // This test the on-heap version of the AnyAggregator (Double/Float/Long) against numeric columns
  // that have null values (when run in SQL compatible null mode)
  @Test
  public void testAnyAggregatorsOnHeapNumericNulls()
  {
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
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{7L, 1.0, 1.0f}
        )
    );
  }

  // This test the on-heap version of the AnyAggregator (Double/Float/Long/String)
  @Test
  public void testAnyAggregator()
  {
    // Cannot vectorize virtual expressions.
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
                      expressionVirtualColumn("v0", "(\"cnt\" + 1)", ColumnType.LONG),
                      expressionVirtualColumn("v1", "(\"m1\" + 1)", ColumnType.FLOAT),
                      expressionVirtualColumn("v2", "concat(\"dim1\",CAST(\"cnt\", 'STRING'))", ColumnType.STRING)
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
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        NullHandling.sqlCompatible()
        ? ImmutableList.of(new Object[]{1L, 1.0f, 1.0, "", 2L, 2.0f, "1"})
        : ImmutableList.of(new Object[]{1L, 1.0f, 1.0, "", 2L, 2.0f, "1"})
    );
  }


  @Test
  public void testEarliestAggregatorsNumericNulls()
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
                          new LongFirstAggregatorFactory("a0", "l1", null),
                          new DoubleFirstAggregatorFactory("a1", "d1", null),
                          new FloatFirstAggregatorFactory("a2", "f1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{7L, 1.0, 1.0f}
        )
    );
  }

  @Test
  public void testLatestAggregatorsNumericNull()
  {
    testQuery(
        "SELECT LATEST(l1), LATEST(d1), LATEST(f1) FROM druid.numfoo",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(
                      aggregators(
                          new LongLastAggregatorFactory("a0", "l1", null),
                          new DoubleLastAggregatorFactory("a1", "d1", null),
                          new FloatLastAggregatorFactory("a2", "f1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                NullHandling.defaultLongValue(),
                NullHandling.defaultDoubleValue(),
                NullHandling.defaultFloatValue()
            }
        )
    );
  }

  @Test
  public void testFirstLatestAggregatorsSkipNulls()
  {
    // Cannot vectorize EARLIEST aggregator.
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
                          new StringFirstAggregatorFactory("a0", "dim1", null, 32),
                          new LongLastAggregatorFactory("a1", "l1", null),
                          new DoubleLastAggregatorFactory("a2", "d1", null),
                          new FloatLastAggregatorFactory("a3", "f1", null)
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // first row of dim1 is empty string, which is null in default mode, last non-null numeric rows are zeros
            new Object[]{useDefault ? "10.1" : "", 0L, 0.0, 0.0f}
        )
    );
  }

  @Test
  public void testAnyAggregatorsDoesNotSkipNulls()
  {
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
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        // first row has null for l2, d2, f2 and dim1 as empty string (which is null in default mode)
        ImmutableList.of(
            useDefault ? new Object[]{"", 0L, 0.0, 0f} : new Object[]{"", null, null, null}
        )
    );
  }

  @Test
  public void testAnyAggregatorsSkipNullsWithFilter()
  {
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
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            // first row of dim1 is empty string, which is null in default mode
            new Object[]{"10.1", 325323L, 1.7, 0.1f}
        )
    );
  }

}
