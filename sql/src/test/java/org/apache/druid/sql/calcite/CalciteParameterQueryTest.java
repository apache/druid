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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.http.SqlParameter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class has copied a subset of the tests in {@link CalciteQueryTest} and
 * replaced various parts of queries with dynamic parameters. It is NOT
 * important that this file remains in sync with {@link CalciteQueryTest}, the
 * tests were merely chosen to produce a selection of parameter types and
 * positions within query expressions and have been renamed to reflect this
 */
public class CalciteParameterQueryTest extends BaseCalciteQueryTest
{
  @Test
  public void testSelectConstantParamGetsConstant()
  {
    testQuery(
        "SELECT 1 + ?",
        ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(new Object[]{2L}),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .columns("EXPR$0")
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .legacy(false)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{2}
        ),
        ImmutableList.of(new SqlParameter(SqlType.INTEGER, 1))
    );
  }

  @Test
  public void testParamsGetOptimizedIntoConstant()
  {
    testQuery(
        "SELECT 1 + ?, dim1 FROM foo LIMIT ?",
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
        ),
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 1),
            new SqlParameter(SqlType.INTEGER, 1)
        )
    );
  }

  @Test
  public void testParametersInSelectAndFilter()
  {
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 10),
            new SqlParameter(SqlType.INTEGER, 0)
        ),
        "SELECT exp(count(*)) + ?, sum(m2) FROM druid.foo WHERE  dim2 = ?",
        CalciteTests.REGULAR_USER_AUTH_RESULT,
        ImmutableList.of(Druids.newTimeseriesQueryBuilder()
                               .dataSource(CalciteTests.DATASOURCE1)
                               .intervals(querySegmentSpec(Filtration.eternity()))
                               .filters(numericEquality("dim2", 0L, ColumnType.LONG))
                               .granularity(Granularities.ALL)
                               .aggregators(aggregators(
                                   new CountAggregatorFactory("a0"),
                                   new DoubleSumAggregatorFactory("a1", "m2")
                               ))
                               .postAggregators(
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)", ColumnType.DOUBLE)
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );
  }

  @Test
  public void testSelectTrimFamilyWithParameters()
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
                      expressionPostAgg("p0", "'foo'", ColumnType.STRING),
                      expressionPostAgg("p1", "'xfoo'", ColumnType.STRING),
                      expressionPostAgg("p2", "'foo'", ColumnType.STRING),
                      expressionPostAgg("p3", "' foo'", ColumnType.STRING),
                      expressionPostAgg("p4", "'foo'", ColumnType.STRING),
                      expressionPostAgg("p5", "'foo'", ColumnType.STRING),
                      expressionPostAgg("p6", "'foo'", ColumnType.STRING),
                      expressionPostAgg("p7", "'foo '", ColumnType.STRING),
                      expressionPostAgg("p8", "'foox'", ColumnType.STRING),
                      expressionPostAgg("p9", "' foo'", ColumnType.STRING),
                      expressionPostAgg("p10", "'xfoo'", ColumnType.STRING)
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
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
  public void testParamsInInformationSchema()
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
            new Object[]{8L, 1249L, 156.125, -5L, 1111L}
        ),
        ImmutableList.of(
            new SqlParameter(SqlType.VARCHAR, "druid"),
            new SqlParameter(SqlType.VARCHAR, "foo")
        )
    );
  }

  @Test
  public void testParamsInSelectExpressionAndLimit()
  {
    testQuery(
        "SELECT SUBSTRING(dim2, ?, ?) FROM druid.foo LIMIT ?",
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
        ),
        ImmutableList.of(
            new SqlParameter(SqlType.INTEGER, 1),
            new SqlParameter(SqlType.INTEGER, 1),
            new SqlParameter(SqlType.INTEGER, 2)
        )
    );
  }

  @Test
  public void testParamsTuckedInACast()
  {
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
                            new DefaultDimensionSpec("m1", "d1", ColumnType.FLOAT)
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
  public void testParametersInStrangePlaces()
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
                                not(equality("dim2", "a", ColumnType.STRING))
                            ),
                            new CountAggregatorFactory("a1")
                        ))
                        .setPostAggregatorSpecs(ImmutableList.of(
                            expressionPostAgg("p0", "(\"a0\" / \"a1\")", ColumnType.LONG)
                        ))
                        .setHavingSpec(having(expressionFilter("((\"a0\" / \"a1\") == 1)")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        NullHandling.replaceWithDefault()
        ? ImmutableList.of(
            new Object[]{"10.1", 1L},
            new Object[]{"2", 1L},
            new Object[]{"abc", 1L},
            new Object[]{"def", 1L}
        )
        : ImmutableList.of(
            new Object[]{"2", 1L},
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
  public void testParametersInCases()
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
                  .postAggregators(expressionPostAgg("p0", "(\"a0\" / 10)", ColumnType.DOUBLE))
                  .context(QUERY_CONTEXT_DEFAULT)
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
  public void testTimestamp()
  {
    // with millis
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
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
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)", ColumnType.DOUBLE)
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );

  }

  @Test
  public void testTimestampString()
  {
    // with timestampstring
    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
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
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)", ColumnType.DOUBLE)
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );
  }

  @Test
  public void testDate()
  {
    // with date from millis

    testQuery(
        PLANNER_CONFIG_DEFAULT,
        QUERY_CONTEXT_DEFAULT,
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
                                   expressionPostAgg("p0", "(exp(\"a0\") + 10)", ColumnType.DOUBLE)
                               )
                               .context(QUERY_CONTEXT_DEFAULT)
                               .build()),
        ImmutableList.of(
            new Object[]{11.0, NullHandling.defaultDoubleValue()}
        )
    );
  }

  @Test
  public void testDoubles()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt > ? and cnt < ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      NullHandling.replaceWithDefault()
                      ? bound("cnt", "1.1", "100000001", true, true, null, StringComparators.NUMERIC)
                      : range("cnt", ColumnType.DOUBLE, 1.1, 100000001.0, true, true)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{0L}),
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
                      NullHandling.replaceWithDefault()
                      ? in("cnt", ImmutableList.of("1.0", "100000001"))
                      : in("cnt", ColumnType.DOUBLE, ImmutableList.of(1.0, 1.00000001E8))
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
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
  public void testFloats()
  {
    testQuery(
        "SELECT COUNT(*) FROM druid.foo WHERE cnt = ?",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      equality("cnt", 1.0, ColumnType.DOUBLE)
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{6L}),
        ImmutableList.of(new SqlParameter(SqlType.REAL, 1.0f))
    );
  }

  @Test
  public void testLongs()
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
                  .filters(range("l1", ColumnType.LONG, 3L, null, true, false))
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{2L}),
        ImmutableList.of(new SqlParameter(SqlType.BIGINT, 3L))
    );
  }

  @Test
  public void testMissingParameter()
  {
    DruidException exception = assertThrows(
        DruidException.class,
        () -> testQuery(
            "SELECT COUNT(*)\n"
                + "FROM druid.numfoo\n"
                + "WHERE l1 > ?",
            ImmutableList.of(),
            ImmutableList.of(new Object[] {3L}),
            ImmutableList.of()
        )
    );
    assertThat(
        exception,
        DruidExceptionMatcher.invalidSqlInput().expectMessageIs("No value bound for parameter (position [1])")
    );
  }

  @Test
  public void testPartiallyMissingParameter()
  {
    DruidException exception = assertThrows(
        DruidException.class,
        () -> testQuery(
            "SELECT COUNT(*)\n"
                + "FROM druid.numfoo\n"
                + "WHERE l1 > ? AND f1 = ?",
            ImmutableList.of(),
            ImmutableList.of(new Object[] {3L}),
            ImmutableList.of(new SqlParameter(SqlType.BIGINT, 3L))
        )
    );
    assertThat(
        exception,
        DruidExceptionMatcher.invalidSqlInput().expectMessageIs("No value bound for parameter (position [2])")
    );
  }

  @Test
  public void testPartiallyMissingParameterInTheMiddle()
  {
    List<SqlParameter> params = new ArrayList<>();
    params.add(null);
    params.add(new SqlParameter(SqlType.INTEGER, 1));
    DruidException exception = assertThrows(
        DruidException.class,
        () -> testQuery(
            "SELECT 1 + ?, dim1 FROM foo LIMIT ?",
            ImmutableList.of(),
            ImmutableList.of(),
            params
        )
    );

    assertThat(
        exception,
        DruidExceptionMatcher.invalidSqlInput().expectMessageIs("No value bound for parameter (position [1])")
    );
  }

  @Test
  public void testWrongTypeParameter()
  {
    if (!useDefault) {
      // cannot vectorize inline datasource
      cannotVectorize();
    }
    testQuery(
        "SELECT COUNT(*)\n"
        + "FROM druid.numfoo\n"
        + "WHERE l1 > ? AND f1 = ?",
        useDefault
        ? ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE3)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .filters(
                      and(
                          bound("l1", "3", null, true, false, null, StringComparators.NUMERIC),
                          selector("f1", "0.0", null)
                      )
                  )
                  .aggregators(aggregators(new CountAggregatorFactory("a0")))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        )
        : ImmutableList.of(
            Druids.newScanQueryBuilder()
                  .dataSource(
                      InlineDataSource.fromIterable(
                          ImmutableList.of(
                              new Object[]{0L}
                          ),
                          RowSignature.builder().add("EXPR$0", ColumnType.LONG).build()
                      )
                  )
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .columns("EXPR$0")
                  .legacy(false)
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{0L}),
        ImmutableList.of(new SqlParameter(SqlType.BIGINT, 3L), new SqlParameter(SqlType.VARCHAR, "wat"))
    );
  }

  @Test
  public void testNullParameter()
  {
    cannotVectorize();
    // contrived example of using null as an sql parameter to at least test the codepath because lots of things dont
    // actually work as null and things like 'IS NULL' fail to parse in calcite if expressed as 'IS ?'

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
                                "nvl(\"dim2\",'parameter')",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
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
                                "nvl(\"dim2\",'parameter')",
                                ColumnType.STRING
                            )
                        )
                        .setDimensions(dimensions(new DefaultDimensionSpec("v0", "d0", ColumnType.STRING)))
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
