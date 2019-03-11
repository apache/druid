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
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.Test;

public class CalciteParameterQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testSelectConstantExpression() throws Exception
  {
    testQuery(
        "SELECT 1 + ?",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{2}
        ),
        ImmutableList.of(new SqlParameter(1, SqlType.INTEGER, 1))
    );
  }

  @Test
  public void testSelectConstantExpressionFromTable() throws Exception
  {
    testQuery(
        "SELECT 1 + ?, dim1 FROM foo LIMIT ?",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "2", ValueType.LONG))
                .columns("dim1", "v0")
                .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{2, ""}
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.INTEGER, 1),
            new SqlParameter(2, SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testSelectCountStart() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(1, SqlType.INTEGER, 10),
            new SqlParameter(2, SqlType.INTEGER, 0)
        ),
        "SELECT exp(count(*)) + ?, sum(m2)  FROM druid.foo WHERE  dim2 = ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(selector("dim2", "0", null))
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
  }

  @Test
  public void testTimestamp() throws Exception
  {
    long val = new TimestampString("2000-01-01 00:00:00").getMillisSinceEpoch();
    // with millis
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(1, SqlType.INTEGER, 10),
            new SqlParameter(
                2,
                SqlType.TIMESTAMP,
                DateTimes.of("2999-01-01T00:00:00Z").getMillis()
            )
        ),
        "SELECT exp(count(*)) + ?, sum(m2)  FROM druid.foo WHERE  __time >= ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of("2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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


    // with timestampstring
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(1, SqlType.INTEGER, 10),
            new SqlParameter(
                2,
                SqlType.TIMESTAMP,
                "2999-01-01 00:00:00"
            )
        ),
        "SELECT exp(count(*)) + ?, sum(m2)  FROM druid.foo WHERE  __time >= ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of("2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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
  }


  @Test
  public void testSelectTrimFamily() throws Exception
  {
    // TRIM has some whacky parsing. Make sure the different forms work.

    testQuery(
        "SELECT\n"
        + "TRIM(BOTH 'x' FROM ?),\n"
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
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.VARCHAR, "xfoox")
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
        + "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{8L, 1249L, 156L, -5L, 1111L}
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.VARCHAR, "druid"),
            new SqlParameter(2, SqlType.VARCHAR, "foo")
        )
    );
  }

  @Test
  public void testSelectWithProjection() throws Exception
  {
    testQuery(
        "SELECT SUBSTRING(dim2, ?, ?) FROM druid.foo LIMIT ?",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(
                    expressionVirtualColumn("v0", "substring(\"dim2\", 0, 1)", ValueType.STRING)
                )
                .columns("v0")
                .limit(2)
                .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{NULL_VALUE}
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.INTEGER, 1),
            new SqlParameter(2, SqlType.INTEGER, 1),
            new SqlParameter(3, SqlType.INTEGER, 2)
        )
    );
  }

  @Test
  public void testSelfJoinWithFallback() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_FALLBACK,
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new SqlParameter(1, SqlType.VARCHAR, "")
        ),
        "SELECT x.dim1, y.dim1, y.dim2\n"
        + "FROM\n"
        + "  druid.foo x INNER JOIN druid.foo y ON x.dim1 = y.dim2\n"
        + "WHERE\n"
        + "  x.dim1 <> ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1")
                .filters(not(selector("dim1", "", null)))
                .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build(),
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns("dim1", "dim2")
                .resultFormat(ScanQuery.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc", "def", "abc"}
        )
    );
  }

  @Test
  public void testColumnComparison() throws Exception
  {
    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - CAST(? as INT) = dim1 GROUP BY dim1, m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(expressionFilter("((\"m1\" - 1) == \"dim1\")"))
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
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testHavingOnRatio() throws Exception
  {
    // Test for https://github.com/apache/incubator-druid/issues/4264
    testQuery(
        "SELECT\n"
        + "  dim1,\n"
        + "  COUNT(*) FILTER(WHERE dim2 <> ?)/COUNT(*) as ratio\n"
        + "FROM druid.foo\n"
        + "GROUP BY dim1\n"
        + "HAVING COUNT(*) FILTER(WHERE dim2 <> ?)/COUNT(*) = ?",
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
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.VARCHAR, "a"),
            new SqlParameter(2, SqlType.VARCHAR, "a"),
            new SqlParameter(3, SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testPruneDeadAggregatorsThroughPostProjection() throws Exception
  {
    // Test for ProjectAggregatePruneUnusedCallRule.
    testQuery(
        "SELECT\n"
        + "  CASE 'foo'\n"
        + "  WHEN ? THEN SUM(cnt) / CAST(? as INT)\n"
        + "  WHEN ? THEN SUM(m1) / CAST(? as INT)\n"
        + "  WHEN ? THEN SUM(m2) / CAST(? as INT)\n"
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
        ImmutableList.of(new Object[]{2.1}),
        ImmutableList.of(
            new SqlParameter(1, SqlType.VARCHAR, "bar"),
            new SqlParameter(2, SqlType.INTEGER, 10),
            new SqlParameter(3, SqlType.VARCHAR, "foo"),
            new SqlParameter(4, SqlType.INTEGER, 10),
            new SqlParameter(5, SqlType.VARCHAR, "baz"),
            new SqlParameter(6, SqlType.INTEGER, 10)
        )
    );
  }

  @Test
  public void testFilterOnFloat() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m1 >= ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(bound("m1", "0.9", null, false, false, null, StringComparators.NUMERIC))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.FLOAT, 0.9)
        )
    );
  }

  @Test
  public void testFilterOnDouble() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE m2 >= ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .filters(bound("m2", "0.9", null, false, false, null, StringComparators.NUMERIC))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.DOUBLE, 0.9)
        )
    );
  }

  @Test
  public void testCountStarWithLongColumnFiltersOnFloatLiterals() throws Exception
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt > ? and cnt < ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      bound("cnt", "1.1", "100000001", true, true, null, StringComparators.NUMERIC)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(),
        ImmutableList.of(
            new SqlParameter(1, SqlType.DOUBLE, 1.1),
            new SqlParameter(2, SqlType.DOUBLE, 100000001.0)
        )
    );
    // calcite will strip the trailing zeros when creating float and double literals for whatever reason
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = ?",
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
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.DOUBLE, 1.0)
        )
    );
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      selector("cnt", "100000001", null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(),
        ImmutableList.of(
            new SqlParameter(1, SqlType.DOUBLE, 100000001.0)
        )
    );
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = ? or cnt = ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      in("cnt", ImmutableList.of("1.0", "100000001"), null)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{6L}
        ),
        ImmutableList.of(
            new SqlParameter(1, SqlType.DOUBLE, 1.0),
            new SqlParameter(2, SqlType.DOUBLE, 100000001.0)
        )
    );
  }
}
