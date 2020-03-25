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

/**
 * This class has copied a subset of the tests in {@link CalciteQueryTest} and replaced various parts of queries with
 * dynamic parameters. It is NOT important that this file remains in sync with {@link CalciteQueryTest}, the tests
 * were merely chosen to produce a selection of parameter types and positions within query expressions and have been
 * renamed to reflect this
 */
public class CalciteParameterQueryTest extends BaseCalciteQueryTest
{
  private final boolean useDefault = NullHandling.replaceWithDefault();

  @Test
  public void testSelectConstantParamGetsConstant() throws Exception
  {
    testQuery(
        "SELECT 1 + ?",
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{2}
        ),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );
  }

  @Test
  public void testParamsGetOptimizedIntoConstant() throws Exception
  {
    testQuery(
        "SELECT 1 + ?, dim1 FROM foo LIMIT ?",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(querySegmentSpec(Filtration.eternity()))
                .virtualColumns(expressionVirtualColumn("v0", "2", ValueType.LONG))
                .columns("dim1", "v0")
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .limit(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{2, ""}
        ),
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 1),
            new SqlParameter(SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testParametersInSelectAndFilter() throws Exception
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(SqlType.INTEGER, 0)
        ),
        "SELECT exp(count(*)) + ?, sum(m2) FROM druid.foo WHERE  dim2 = ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(numericSelector("dim2", "0", null))
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
  public void testSelectTrimFamilyWithParameters() throws Exception
  {
    // TRIM has some whacky parsing. Abuse this to test a bunch of parameters

    testQuery(
        "SELECT\n"
        + "TRIM(BOTH ? FROM ?),\n"
        + "TRIM(TRAILING ? FROM ?),\n"
        + "TRIM(? FROM ?),\n"
        + "TRIM(TRAILING FROM ?),\n"
        + "TRIM(?),\n"
        + "BTRIM(?),\n"
        + "BTRIM(?, ?),\n"
        + "LTRIM(?),\n"
        + "LTRIM(?, ?),\n"
        + "RTRIM(?),\n"
        + "RTRIM(?, ?),\n"
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
            new SqlParameter(SqlType.VARCHAR, "x"),
            new SqlParameter(SqlType.VARCHAR, "xfoox"),
            new SqlParameter(SqlType.VARCHAR, "x"),
            new SqlParameter(SqlType.VARCHAR, "xfoox"),
            new SqlParameter(SqlType.VARCHAR, " "),
            new SqlParameter(SqlType.VARCHAR, " foo "),
            new SqlParameter(SqlType.VARCHAR, " foo "),
            new SqlParameter(SqlType.VARCHAR, " foo "),
            new SqlParameter(SqlType.VARCHAR, " foo "),
            new SqlParameter(SqlType.VARCHAR, "xfoox"),
            new SqlParameter(SqlType.VARCHAR, "x"),
            new SqlParameter(SqlType.VARCHAR, " foo "),
            new SqlParameter(SqlType.VARCHAR, "xfoox"),
            new SqlParameter(SqlType.VARCHAR, "x"),
            new SqlParameter(SqlType.VARCHAR, " foo "),
            new SqlParameter(SqlType.VARCHAR, "xfoox"),
            new SqlParameter(SqlType.VARCHAR, "x")
        )
    );
  }

  @Test
  public void testParamsInInformationSchema() throws Exception
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
            new SqlParameter(SqlType.VARCHAR, "druid"),
            new SqlParameter(SqlType.VARCHAR, "foo")
        )
    );
  }

  @Test
  public void testParamsInSelectExpressionAndLimit() throws Exception
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
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"a"},
            new Object[]{NULL_STRING}
        ),
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 1),
            new SqlParameter(SqlType.INTEGER, 1),
            new SqlParameter(SqlType.INTEGER, 2)
        )
    );
  }

  @Test
  public void testParamsTuckedInACast() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT dim1, m1, COUNT(*) FROM druid.foo WHERE m1 - CAST(? as INT) = dim1 GROUP BY dim1, m1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setDimFilter(expressionFilter("((\"m1\" - 1) == CAST(\"dim1\", 'DOUBLE'))"))
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
            new SqlParameter(SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testParametersInStrangePlaces() throws Exception
  {
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
            new SqlParameter(SqlType.VARCHAR, "a"),
            new SqlParameter(SqlType.VARCHAR, "a"),
            new SqlParameter(SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testParametersInCases() throws Exception
  {
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
            new SqlParameter(SqlType.VARCHAR, "bar"),
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(SqlType.VARCHAR, "foo"),
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(SqlType.VARCHAR, "baz"),
            new SqlParameter(SqlType.INTEGER, 10)
        )
    );
  }


  @Test
  public void testTimestamp() throws Exception
  {
    // with millis
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(
                SqlType.TIMESTAMP,
                DateTimes.of("2999-01-01T00:00:00Z").getMillis()
            )
        ),
        "SELECT exp(count(*)) + ?, sum(m2)  FROM druid.foo WHERE  __time >= ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of(
                                   "2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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
  public void testTimestampString() throws Exception
  {
    // with timestampstring
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(
                SqlType.TIMESTAMP,
                "2999-01-01 00:00:00"
            )
        ),
        "SELECT exp(count(*)) + ?, sum(m2)  FROM druid.foo WHERE  __time >= ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of(
                                   "2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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
  public void testDate() throws Exception
  {
    // with date from millis

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DONT_SKIP_EMPTY_BUCKETS,
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(
                SqlType.DATE,
                "2999-01-01"
            )
        ),
        "SELECT exp(count(*)) + ?, sum(m2)  FROM druid.foo WHERE  __time >= ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Intervals.of(
                                   "2999-01-01T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
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
  public void testDoubles() throws Exception
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
            new SqlParameter(SqlType.DOUBLE, 1.1),
            new SqlParameter(SqlType.FLOAT, 100000001.0)
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
            new SqlParameter(SqlType.DOUBLE, 1.0),
            new SqlParameter(SqlType.FLOAT, 100000001.0)
        )
    );
  }

  @Test
  public void testFloats() throws Exception
  {
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
        ImmutableList.of(new Object[]{6L}),
        ImmutableList.of(new SqlParameter(SqlType.REAL, 1.0f))
    );
  }

  @Test
  public void testLongs() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(bound("l1", "3", null, true, false, null, StringComparators.NUMERIC))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L}),
        ImmutableList.of(new SqlParameter(SqlType.BIGINT, 3L))
    );
  }

  @Test
  public void testMissingParameter() throws Exception
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Parameter: [?0] is not bound");
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > ?",
        ImmutableList.of(),
        ImmutableList.of(new Object[]{3L}),
        ImmutableList.of()
    );
  }

  @Test
  public void testPartiallyMissingParameter() throws Exception
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Parameter: [?1] is not bound");
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > ? AND f1 = ?",
        ImmutableList.of(),
        ImmutableList.of(new Object[]{3L}),
        ImmutableList.of(new SqlParameter(SqlType.BIGINT, 3L))
    );
  }

  @Test
  public void testWrongTypeParameter() throws Exception
  {
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > ? AND f1 = ?",
        useDefault ? ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      and(
                          bound("l1", "3", null, true, false, null, StringComparators.NUMERIC),
                          selector("f1", useDefault ? "0.0" : null, null)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(TIMESERIES_CONTEXT_DEFAULT)
                  .build()
        ) : ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(new SqlParameter(SqlType.BIGINT, 3L), new SqlParameter(SqlType.VARCHAR, "wat"))
    );
  }

  @Test
  public void testNullParameter() throws Exception
  {
    // contrived example of using null as an sql parameter to at least test the codepath because lots of things dont
    // actually work as null and things like 'IS NULL' fail to parse in calcite if expressed as 'IS ?'
    cannotVectorize();

    // this will optimize out the 3rd argument because 2nd argument will be constant and not null
    testQuery(
        "SELECT COALESCE(dim2, ?, ?), COUNT(*) FROM druid.foo GROUP BY 1\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'parameter')",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ValueType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L},
            new Object[]{"parameter", 3L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L},
            new Object[]{"parameter", 2L}
        ),
        ImmutableList.of(new SqlParameter(SqlType.VARCHAR, "parameter"), new SqlParameter(SqlType.VARCHAR, null))
    );

    // when converting to rel expression, this will optimize out 2nd argument to coalesce which is null
    testQuery(
        "SELECT COALESCE(dim2, ?, ?), COUNT(*) FROM druid.foo GROUP BY 1\n",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            expressionVirtualColumn(
                                "v0",
                                "case_searched(notnull(\"dim2\"),\"dim2\",'parameter')",
                                ValueType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "v0", ValueType.STRING)))
                        .setAggregatorSpecs(aggregators(new CountAggregatorFactory("a0")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault() ?
        ImmutableList.of(
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L},
            new Object[]{"parameter", 3L}
        ) :
        ImmutableList.of(
            new Object[]{"", 1L},
            new Object[]{"a", 2L},
            new Object[]{"abc", 1L},
            new Object[]{"parameter", 2L}
        ),
        ImmutableList.of(new SqlParameter(SqlType.VARCHAR, null), new SqlParameter(SqlType.VARCHAR, "parameter"))
    );
  }
}
