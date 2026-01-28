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

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.error.DruidException;
import org.junit.Assert;
import org.junit.Test;

public class DruidSqlParserTest
{
  @Test
  public void test_sqlLiteralToContextValue_null()
  {
    final SqlLiteral literal = SqlLiteral.createNull(SqlParserPos.ZERO);
    Assert.assertNull(DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_string()
  {
    final SqlLiteral literal = SqlLiteral.createCharString("abc", SqlParserPos.ZERO);
    Assert.assertEquals("abc", DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_stringWithSpecialChars()
  {
    final SqlLiteral literal = SqlLiteral.createCharString("hello\nworld\t\"test\"", SqlParserPos.ZERO);
    Assert.assertEquals("hello\nworld\t\"test\"", DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_integer()
  {
    // Numbers within Long range are converted to Long.
    final SqlLiteral literal = SqlLiteral.createExactNumeric("42", SqlParserPos.ZERO);
    Assert.assertEquals(42L, DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_negativeInteger()
  {
    final SqlLiteral literal = SqlLiteral.createExactNumeric("-123", SqlParserPos.ZERO);
    Assert.assertEquals(-123L, DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_decimal()
  {
    // Decimals are converted to Double.
    final SqlLiteral literal = SqlLiteral.createExactNumeric("3.14159", SqlParserPos.ZERO);
    Assert.assertEquals(3.14159, DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_largeNumber()
  {
    // Integers outside Long range are retained as strings.
    final SqlLiteral literal = SqlLiteral.createExactNumeric("123456789012345678901234567890", SqlParserPos.ZERO);
    Assert.assertEquals("123456789012345678901234567890", DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_approximateNumeric()
  {
    final SqlLiteral literal = SqlLiteral.createApproxNumeric("1.23E10", SqlParserPos.ZERO);
    Assert.assertEquals(1.23E10, DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_booleanTrue()
  {
    final SqlLiteral literal = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    Assert.assertEquals(true, DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_booleanFalse()
  {
    final SqlLiteral literal = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);
    Assert.assertEquals(false, DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_timestamp()
  {
    // Timestamps are returned as strings in ISO8601 format
    final TimestampString timestampString = new TimestampString("2023-01-15 14:30:00");
    final SqlLiteral literal =
        SqlLiteral.createTimestamp(SqlTypeName.TIMESTAMP, timestampString, 0, SqlParserPos.ZERO);
    Assert.assertEquals("2023-01-15T14:30:00.000Z", DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_timestampWithFractionalSeconds()
  {
    final TimestampString timestampString = new TimestampString("2023-01-15 14:30:00.123");
    final SqlLiteral literal =
        SqlLiteral.createTimestamp(SqlTypeName.TIMESTAMP, timestampString, 3, SqlParserPos.ZERO);
    Assert.assertEquals("2023-01-15T14:30:00.123Z", DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_date()
  {
    final DateString dateString = new DateString("2023-01-15");
    final SqlLiteral literal = SqlLiteral.createDate(dateString, SqlParserPos.ZERO);
    Assert.assertEquals("2023-01-15T00:00:00.000Z", DruidSqlParser.sqlLiteralToContextValue(literal));
  }

  @Test
  public void test_sqlLiteralToContextValue_unsupportedType()
  {
    final SqlLiteral literal = SqlLiteral.createSymbol(SqlTypeName.BINARY, SqlParserPos.ZERO);
    final DruidException exception = Assert.assertThrows(
        DruidException.class,
        () -> DruidSqlParser.sqlLiteralToContextValue(literal)
    );
    Assert.assertTrue(exception.getMessage().contains("Unsupported type for SET"));
  }
}
