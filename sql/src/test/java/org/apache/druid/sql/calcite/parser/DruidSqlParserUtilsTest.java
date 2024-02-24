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

package org.apache.druid.sql.calcite.parser;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlPostfixOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.sql.calcite.expression.TimeUnits;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.hamcrest.MatcherAssert;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Enclosed.class)
public class DruidSqlParserUtilsTest
{
  /**
   * Sanity checking that the formats of TIME_FLOOR(__time, Period) work as expected
   */
  @RunWith(Parameterized.class)
  public static class TimeFloorToGranularityConversionTest
  {
    @Parameterized.Parameters(name = "{1}")
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{"PT1H", Granularities.HOUR}
      );
    }

    String periodString;
    Granularity expectedGranularity;

    public TimeFloorToGranularityConversionTest(String periodString, Granularity expectedGranularity)
    {
      this.periodString = periodString;
      this.expectedGranularity = expectedGranularity;
    }

    @Test
    public void testGranularityFromTimeFloor()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString(this.periodString, SqlParserPos.ZERO));
      final SqlNode timeFloorCall = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(
          timeFloorCall);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }
  }

  /**
   * Sanity checking that FLOOR(__time TO TimeUnit()) works as intended with the supported granularities
   */
  @RunWith(Parameterized.class)
  public static class FloorToGranularityConversionTest
  {
    @Parameterized.Parameters(name = "{1}")
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{TimeUnit.SECOND, TimeUnits.toPeriod(TimeUnitRange.SECOND), Granularities.SECOND},
          new Object[]{TimeUnit.MINUTE, TimeUnits.toPeriod(TimeUnitRange.MINUTE), Granularities.MINUTE},
          new Object[]{TimeUnit.HOUR, TimeUnits.toPeriod(TimeUnitRange.HOUR), Granularities.HOUR},
          new Object[]{TimeUnit.DAY, TimeUnits.toPeriod(TimeUnitRange.DAY), Granularities.DAY},
          new Object[]{TimeUnit.WEEK, TimeUnits.toPeriod(TimeUnitRange.WEEK), Granularities.WEEK},
          new Object[]{TimeUnit.MONTH, TimeUnits.toPeriod(TimeUnitRange.MONTH), Granularities.MONTH},
          new Object[]{TimeUnit.QUARTER, TimeUnits.toPeriod(TimeUnitRange.QUARTER), Granularities.QUARTER},
          new Object[]{TimeUnit.YEAR, TimeUnits.toPeriod(TimeUnitRange.YEAR), Granularities.YEAR}
      );
    }

    TimeUnit timeUnit;
    Period period;
    Granularity expectedGranularity;

    public FloorToGranularityConversionTest(TimeUnit timeUnit, Period period, Granularity expectedGranularity)
    {
      this.timeUnit = timeUnit;
      this.period = period;
      this.expectedGranularity = expectedGranularity;
    }

    @Test
    public void testGetGranularityFromFloor()
    {
      // parserPos doesn't matter
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(this.timeUnit, null, SqlParserPos.ZERO));
      final SqlNode floorCall = SqlStdOperatorTable.FLOOR.createCall(args);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(floorCall);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }

    /**
     * Tests clause like "PARTITIONED BY 'day'"
     */
    @Test
    public void testConvertSqlNodeToGranularityAsLiteral()
    {
      SqlNode sqlNode = SqlLiteral.createCharString(timeUnit.name(), SqlParserPos.ZERO);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }

    /**
     * Tests clause like "PARTITIONED BY PT1D"
     */
    @Test
    public void testConvertSqlNodeToPeriodFormGranularityAsIdentifier()
    {
      SqlNode sqlNode = new SqlIdentifier(period.toString(), SqlParserPos.ZERO);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }

    /**
     * Tests clause like "PARTITIONED BY 'PT1D'"
     */
    @Test
    public void testConvertSqlNodeToPeriodFormGranularityAsLiteral()
    {
      SqlNode sqlNode = SqlLiteral.createCharString(period.toString(), SqlParserPos.ZERO);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }
  }

  /**
   * Test class that validates the resolution of "CLUSTERED BY" columns to output columns.
   */
  public static class ResolveClusteredByColumnsTest
  {
    @Test
    public void testNullClusteredByAndSource()
    {
      Assert.assertNull(DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(null, null));
    }

    @Test
    public void testNullClusteredBy()
    {
      final ImmutableList<Pair<Integer, String>> fields = ImmutableList.of(
          Pair.of(1, "__time"),
          Pair.of(2, "foo"),
          Pair.of(3, "bar")
      );
      Assert.assertNull(
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(
              null,
              fields
          )
      );
    }

    @Test
    public void testSimpledClusteredByWithNullSource()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIdentifier("FOO", new SqlParserPos(0, 2)));
      SqlBasicCall sqlBasicCall1 = new SqlBasicCall(
          new SqlAsOperator(),
          new SqlNode[]{
              new SqlIdentifier("DIM3", SqlParserPos.ZERO),
              new SqlIdentifier("DIM3_ALIAS", SqlParserPos.ZERO)
          },
          new SqlParserPos(0, 3)
      );
      args.add(sqlBasicCall1);
      Assert.assertEquals(
          Arrays.asList("__time", "FOO", "DIM3_ALIAS"),
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(args, null)
      );
    }

    @Test
    public void testSimpleClusteredBy()
    {
      final ImmutableList<Pair<Integer, String>> sourceFieldMappings = ImmutableList.of(
          Pair.of(1, "__time"),
          Pair.of(2, "FOO"),
          Pair.of(3, "BOO")
      );

      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("FOO", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      Assert.assertEquals(
          Arrays.asList("__time", "FOO", "BOO"),
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(clusteredByArgs, sourceFieldMappings)
      );
    }


    @Test
    public void testClusteredByOrdinalsAndAliases()
    {
      final ImmutableList<Pair<Integer, String>> sourceFieldMappings = ImmutableList.of(
          Pair.of(1, "__time"),
          Pair.of(2, "DIM3"),
          Pair.of(3, "DIM3_ALIAS"),
          Pair.of(4, "floor_dim4_time"),
          Pair.of(5, "DIM5"),
          Pair.of(5, "DIM6"),
          Pair.of(7, "TIME_FLOOR(\"timestamps\", 'PT1H')")
      );

      // Construct the clustered by args
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("4", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("5", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("7", SqlParserPos.ZERO));

      Assert.assertEquals(
          Arrays.asList("DIM3_ALIAS", "floor_dim4_time", "DIM5", "TIME_FLOOR(\"timestamps\", 'PT1H')"),
          DruidSqlParserUtils.resolveClusteredByColumnsToOutputColumns(clusteredByArgs, sourceFieldMappings)
      );
    }
  }

  public static class ClusteredByColumnsValidationTest
  {
    /**
     * Tests an empty CLUSTERED BY clause
     */
    @Test
    public void testEmptyClusteredByColumnsValid()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);

      DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs);
    }

    /**
     * Tests clause "CLUSTERED BY DIM1, DIM2 ASC, 3"
     */
    @Test
    public void testClusteredByColumnsValid()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("DIM1", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("DIM2 ASC", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs);
    }

    /**
     * Tests clause "CLUSTERED BY DIM1, DIM2 ASC, 3, DIM4 DESC"
     */
    @Test
    public void testClusteredByColumnsWithDescThrowsException()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("DIM1", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("DIM2 ASC", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("3", SqlParserPos.ZERO));

      final SqlBasicCall sqlBasicCall = new SqlBasicCall(
          new SqlPostfixOperator("DESC", SqlKind.DESCENDING, 2, null, null, null),
          new SqlNode[]{
              new SqlIdentifier("DIM4", SqlParserPos.ZERO)
          },
          new SqlParserPos(0, 3)
      );
      clusteredByArgs.add(sqlBasicCall);

      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs("Invalid CLUSTERED BY clause [`DIM4` DESC]: cannot sort in descending order.")
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs));
    }

    /**
     * Tests clause "CLUSTERED BY DIM1, DIM2, 3, -10"
     */
    @Test
    public void testClusteredByColumnsWithNegativeOrdinalThrowsException()
    {
      final SqlNodeList clusteredByArgs = new SqlNodeList(SqlParserPos.ZERO);
      clusteredByArgs.add(new SqlIdentifier("DIM1", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("DIM2", SqlParserPos.ZERO));
      clusteredByArgs.add(new SqlIdentifier("3", SqlParserPos.ZERO));
      clusteredByArgs.add(SqlLiteral.createExactNumeric("-10", SqlParserPos.ZERO));

      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs("Ordinal [-10] specified in the CLUSTERED BY clause is invalid. It must be a positive integer.")
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.validateClusteredByColumns(clusteredByArgs));
    }
  }

  public static class FloorToGranularityConversionErrorsTest
  {
    /**
     * Tests clause like "PARTITIONED BY CEIL(__time TO DAY)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectFunctionCall()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.CEIL.createCall(args);
      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs(
              "Invalid operator[CEIL] specified. PARTITIONED BY clause only supports FLOOR(__time TO <unit>)"
              + " and TIME_FLOOR(__time, period) functions."
          )
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode));
    }

    /**
     * Tests clause like "PARTITIONED BY FLOOR(__time)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectNumberOfArguments()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.FLOOR.createCall(args);
      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs(
              "FLOOR in PARTITIONED BY clause must have 2 arguments, but only [1] provided."
          )
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode));
    }

    /**
     * Tests clause like "PARTITIONED BY FLOOR(timestamps TO DAY)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithWrongIdentifierInFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("timestamps", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.FLOOR.createCall(args);
      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs(
              "Invalid argument[timestamps] provided. The first argument to FLOOR in PARTITIONED BY"
              + " clause must be __time."
          )
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode));
    }

    /**
     * Tests clause like "PARTITIONED BY TIME_FLOOR(timestamps, 'PT1H')"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithWrongIdentifierInTimeFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("timestamps", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString("PT1H", SqlParserPos.ZERO));
      final SqlNode sqlNode = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs(
              "Invalid argument[timestamps] provided. The first argument to TIME_FLOOR in"
              + " PARTITIONED BY clause must be __time."
          )
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode));
    }

    /**
     * Tests clause like "PARTITIONED BY FLOOR(__time to ISOYEAR)"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectIngestionGranularityInFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(TimeUnit.ISOYEAR, null, SqlParserPos.ZERO));
      final SqlNode sqlNode = SqlStdOperatorTable.FLOOR.createCall(args);
      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs(
              "ISOYEAR is not a valid period granularity for ingestion."
          )
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode));
    }

    /**
     * Tests clause like "PARTITIONED BY TIME_FLOOR(__time, 'abc')"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectIngestionGranularityInTimeFloorFunction()
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString("abc", SqlParserPos.ZERO));
      final SqlNode sqlNode = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      DruidExceptionMatcher
          .invalidSqlInput()
          .expectMessageIs(
              "granularity['abc'] is an invalid period literal."
          )
          .assertThrowsAndMatches(() -> DruidSqlParserUtils.convertSqlNodeToGranularity(sqlNode));
    }
  }

  public static class NonParameterizedTests
  {
    private static final DateTimeZone TZ_LOS_ANGELES = DateTimes.inferTzFromString("America/Los_Angeles");

    @Test
    public void test_parseTimeStampWithTimeZone_timestamp_utc()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP,
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_timestamp_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP,
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_timestampWithLocalTimeZone()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_timestampWithLocalTimeZone_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createTimestamp(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES),
              DruidTypeSystem.DEFAULT_TIMESTAMP_PRECISION,
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestamp()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC).toString(),
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestampWithLocalTimeZone()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678");

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, DateTimeZone.UTC).toString(),
              SqlParserPos.ZERO
          ),
          DateTimeZone.UTC
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestamp_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES).toString(),
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestampWithLocalTimeZone_losAngeles()
    {
      final DateTime ts = DateTimes.of("2000-01-02T03:04:05.678").withZone(TZ_LOS_ANGELES);

      final String s = DruidSqlParserUtils.parseTimeStampWithTimeZone(
          SqlLiteral.createUnknown(
              SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.getSpaceName(),
              Calcites.jodaToCalciteTimestampString(ts, TZ_LOS_ANGELES).toString(),
              SqlParserPos.ZERO
          ),
          TZ_LOS_ANGELES
      );

      Assert.assertEquals(String.valueOf(ts.getMillis()), s);
    }

    @Test
    public void test_parseTimeStampWithTimeZone_unknownTimestamp_invalid()
    {
      final DruidException e = Assert.assertThrows(
          DruidException.class,
          () -> DruidSqlParserUtils.parseTimeStampWithTimeZone(
              SqlLiteral.createUnknown(
                  SqlTypeName.TIMESTAMP.getSpaceName(),
                  "not a timestamp",
                  SqlParserPos.ZERO
              ),
              DateTimeZone.UTC
          )
      );

      MatcherAssert.assertThat(
          e,
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageContains("Cannot get a timestamp from sql expression")
      );
    }
  }
}
