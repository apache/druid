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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.sql.calcite.expression.builtin.TimeFloorOperatorConversion;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    public void testGranularityFromTimeFloor() throws ParseException
    {
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(SqlLiteral.createCharString(this.periodString, SqlParserPos.ZERO));
      final SqlNode timeFloorCall = TimeFloorOperatorConversion.SQL_FUNCTION.createCall(args);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(
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
          new Object[]{TimeUnit.SECOND, Granularities.SECOND},
          new Object[]{TimeUnit.MINUTE, Granularities.MINUTE},
          new Object[]{TimeUnit.HOUR, Granularities.HOUR},
          new Object[]{TimeUnit.DAY, Granularities.DAY},
          new Object[]{TimeUnit.WEEK, Granularities.WEEK},
          new Object[]{TimeUnit.MONTH, Granularities.MONTH},
          new Object[]{TimeUnit.QUARTER, Granularities.QUARTER},
          new Object[]{TimeUnit.YEAR, Granularities.YEAR}
      );
    }

    TimeUnit timeUnit;
    Granularity expectedGranularity;

    public FloorToGranularityConversionTest(TimeUnit timeUnit, Granularity expectedGranularity)
    {
      this.timeUnit = timeUnit;
      this.expectedGranularity = expectedGranularity;
    }

    @Test
    public void testGetGranularityFromFloor() throws ParseException
    {
      // parserPos doesn't matter
      final SqlNodeList args = new SqlNodeList(SqlParserPos.ZERO);
      args.add(new SqlIdentifier("__time", SqlParserPos.ZERO));
      args.add(new SqlIntervalQualifier(this.timeUnit, null, SqlParserPos.ZERO));
      final SqlNode floorCall = SqlStdOperatorTable.FLOOR.createCall(args);
      Granularity actualGranularity = DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(floorCall);
      Assert.assertEquals(expectedGranularity, actualGranularity);
    }
  }

  public static class FloorToGranularityConversionTestErrors
  {
    /**
     * Tests clause like "PARTITIONED BY 'day'"
     */
    @Test
    public void testConvertSqlNodeToGranularityWithIncorrectNode()
    {
      SqlNode sqlNode = SqlLiteral.createCharString("day", SqlParserPos.ZERO);
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals(
          "Encountered 'day' after PARTITIONED BY. Expected HOUR, DAY, MONTH, YEAR, ALL TIME, FLOOR function or TIME_FLOOR function",
          e.getMessage()
      );
    }

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
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals(
          "PARTITIONED BY clause only supports FLOOR(__time TO <unit> and TIME_FLOOR(__time, period) functions",
          e.getMessage()
      );
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
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("FLOOR in PARTITIONED BY clause must have two arguments", e.getMessage());
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
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("First argument to FLOOR in PARTITIONED BY clause can only be __time", e.getMessage());
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
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("First argument to TIME_FLOOR in PARTITIONED BY clause can only be __time", e.getMessage());
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
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("ISOYEAR is not a valid granularity for ingestion", e.getMessage());
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
      ParseException e = Assert.assertThrows(
          ParseException.class,
          () -> DruidSqlParserUtils.convertSqlNodeToGranularityThrowingParseExceptions(sqlNode)
      );
      Assert.assertEquals("'abc' is an invalid period string", e.getMessage());
    }
  }
}
