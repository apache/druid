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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link SqlIntervalWeekRewriteShuttle} rewrites
 * {@code INTERVAL ... WEEK} into the equivalent {@code INTERVAL ... DAY}
 * with the value multiplied by seven, and {@code INTERVAL ... QUARTER} into
 * the equivalent {@code INTERVAL ... MONTH} with the value multiplied by three,
 * working around CALCITE-6581.
 */
public class SqlIntervalWeekRewriteShuttleTest extends CalciteTestBase
{
  private final SqlIntervalWeekRewriteShuttle shuttle = new SqlIntervalWeekRewriteShuttle();

  @Test
  public void testQuotedSingleWeekLiteralBecomesSevenDays()
  {
    final SqlNode original = SqlLiteral.createInterval(
        1,
        "1",
        new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertNotSame(original, rewritten);
    assertTrue(rewritten instanceof SqlIntervalLiteral);
    final SqlIntervalLiteral.IntervalValue value =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) rewritten).getValue();
    assertEquals("7", value.getIntervalLiteral());
    assertEquals(TimeUnitRange.DAY, value.getIntervalQualifier().timeUnitRange);
    assertEquals(1, value.getSign());
  }

  @Test
  public void testQuotedMultiWeekLiteralIsMultipliedBySeven()
  {
    final SqlNode original = SqlLiteral.createInterval(
        1,
        "3",
        new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    final SqlIntervalLiteral.IntervalValue value =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) rewritten).getValue();
    assertEquals("21", value.getIntervalLiteral());
    assertEquals(TimeUnitRange.DAY, value.getIntervalQualifier().timeUnitRange);
  }

  @Test
  public void testNegativeQuotedWeekLiteralPreservesSign()
  {
    final SqlNode original = SqlLiteral.createInterval(
        -1,
        "2",
        new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    final SqlIntervalLiteral.IntervalValue value =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) rewritten).getValue();
    assertEquals("14", value.getIntervalLiteral());
    assertEquals(TimeUnitRange.DAY, value.getIntervalQualifier().timeUnitRange);
    assertEquals(-1, value.getSign());
  }

  @Test
  public void testDayLiteralIsLeftUntouched()
  {
    final SqlNode original = SqlLiteral.createInterval(
        1,
        "1",
        new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertSame(original, rewritten);
  }

  @Test
  public void testMonthLiteralIsLeftUntouched()
  {
    final SqlNode original = SqlLiteral.createInterval(
        1,
        "5",
        new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertSame(original, rewritten);
  }

  @Test
  public void testUnquotedWeekIntervalCallBecomesMultiplyDayCall()
  {
    // Mirrors what Druid's parser produces for `INTERVAL 1 WEEK`:
    //   SqlStdOperatorTable.INTERVAL.createCall(pos, n, qualifier)
    final SqlNode original = SqlStdOperatorTable.INTERVAL.createCall(
        SqlParserPos.ZERO,
        SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
        new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO)
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertNotSame(original, rewritten);
    assertTrue(rewritten instanceof SqlBasicCall);
    final SqlBasicCall rewrittenCall = (SqlBasicCall) rewritten;
    assertSame(SqlStdOperatorTable.INTERVAL, rewrittenCall.getOperator());

    // Operand 0 should be (numeric * 7).
    final SqlNode numeric = rewrittenCall.operand(0);
    assertTrue(numeric instanceof SqlBasicCall);
    final SqlBasicCall numericCall = (SqlBasicCall) numeric;
    assertSame(SqlStdOperatorTable.MULTIPLY, numericCall.getOperator());
    assertEquals(2, numericCall.operandCount());
    assertEquals("7", ((SqlLiteral) numericCall.operand(1)).toValue());

    // Operand 1 should now be a DAY qualifier.
    final SqlNode qualifier = rewrittenCall.operand(1);
    assertTrue(qualifier instanceof SqlIntervalQualifier);
    assertEquals(TimeUnitRange.DAY, ((SqlIntervalQualifier) qualifier).timeUnitRange);
  }

  @Test
  public void testUnquotedDayIntervalCallIsLeftUntouched()
  {
    final SqlNode original = SqlStdOperatorTable.INTERVAL.createCall(
        SqlParserPos.ZERO,
        SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO),
        new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO)
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertSame(original, rewritten);
  }

  @Test
  public void testQuotedSingleQuarterLiteralBecomesThreeMonths()
  {
    final SqlNode original = SqlLiteral.createInterval(
        1,
        "1",
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertNotSame(original, rewritten);
    assertTrue(rewritten instanceof SqlIntervalLiteral);
    final SqlIntervalLiteral.IntervalValue value =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) rewritten).getValue();
    assertEquals("3", value.getIntervalLiteral());
    assertEquals(TimeUnitRange.MONTH, value.getIntervalQualifier().timeUnitRange);
    assertEquals(1, value.getSign());
  }

  @Test
  public void testQuotedMultiQuarterLiteralIsMultipliedByThree()
  {
    final SqlNode original = SqlLiteral.createInterval(
        1,
        "4",
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    final SqlIntervalLiteral.IntervalValue value =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) rewritten).getValue();
    assertEquals("12", value.getIntervalLiteral());
    assertEquals(TimeUnitRange.MONTH, value.getIntervalQualifier().timeUnitRange);
  }

  @Test
  public void testNegativeQuotedQuarterLiteralPreservesSign()
  {
    final SqlNode original = SqlLiteral.createInterval(
        -1,
        "2",
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO),
        SqlParserPos.ZERO
    );

    final SqlNode rewritten = original.accept(shuttle);
    final SqlIntervalLiteral.IntervalValue value =
        (SqlIntervalLiteral.IntervalValue) ((SqlIntervalLiteral) rewritten).getValue();
    assertEquals("6", value.getIntervalLiteral());
    assertEquals(TimeUnitRange.MONTH, value.getIntervalQualifier().timeUnitRange);
    assertEquals(-1, value.getSign());
  }

  @Test
  public void testUnquotedQuarterIntervalCallBecomesMultiplyMonthCall()
  {
    // Mirrors what Druid's parser produces for `INTERVAL 1 QUARTER`:
    //   SqlStdOperatorTable.INTERVAL.createCall(pos, n, qualifier)
    final SqlNode original = SqlStdOperatorTable.INTERVAL.createCall(
        SqlParserPos.ZERO,
        SqlLiteral.createExactNumeric("2", SqlParserPos.ZERO),
        new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO)
    );

    final SqlNode rewritten = original.accept(shuttle);
    assertNotSame(original, rewritten);
    assertTrue(rewritten instanceof SqlBasicCall);
    final SqlBasicCall rewrittenCall = (SqlBasicCall) rewritten;
    assertSame(SqlStdOperatorTable.INTERVAL, rewrittenCall.getOperator());

    // Operand 0 should be (numeric * 3).
    final SqlNode numeric = rewrittenCall.operand(0);
    assertTrue(numeric instanceof SqlBasicCall);
    final SqlBasicCall numericCall = (SqlBasicCall) numeric;
    assertSame(SqlStdOperatorTable.MULTIPLY, numericCall.getOperator());
    assertEquals(2, numericCall.operandCount());
    assertEquals("3", ((SqlLiteral) numericCall.operand(1)).toValue());

    // Operand 1 should now be a MONTH qualifier.
    final SqlNode qualifier = rewrittenCall.operand(1);
    assertTrue(qualifier instanceof SqlIntervalQualifier);
    assertEquals(TimeUnitRange.MONTH, ((SqlIntervalQualifier) qualifier).timeUnitRange);
  }

  @Test
  public void testParsedWeekIntervalIsRewritten() throws Exception
  {
    // End-to-end check using the actual parser, both forms.
    assertParsedUnitIsRewritten("SELECT TIMESTAMP '1970-01-01 00:00:00' + INTERVAL '1' WEEK", TimeUnitRange.WEEK);
    assertParsedUnitIsRewritten("SELECT TIMESTAMP '1970-01-01 00:00:00' + INTERVAL 1 WEEK", TimeUnitRange.WEEK);
    assertParsedUnitIsRewritten("SELECT TIMESTAMP '1970-01-01 00:00:00' + INTERVAL 2 WEEK", TimeUnitRange.WEEK);
  }

  @Test
  public void testParsedQuarterIntervalIsRewritten() throws Exception
  {
    // End-to-end check using the actual parser, both forms.
    assertParsedUnitIsRewritten("SELECT TIMESTAMP '1970-01-01 00:00:00' + INTERVAL '1' QUARTER", TimeUnitRange.QUARTER);
    assertParsedUnitIsRewritten("SELECT TIMESTAMP '1970-01-01 00:00:00' + INTERVAL 1 QUARTER", TimeUnitRange.QUARTER);
    assertParsedUnitIsRewritten("SELECT TIMESTAMP '1970-01-01 00:00:00' + INTERVAL 2 QUARTER", TimeUnitRange.QUARTER);
  }

  private void assertParsedUnitIsRewritten(String sql, TimeUnitRange unit) throws Exception
  {
    final SqlNode parsed = SqlParser.create(sql).parseQuery();
    final SqlNode rewritten = parsed.accept(shuttle);
    final UnitFinder finder = new UnitFinder(unit);
    rewritten.accept(finder);
    assertTrue(
        !finder.found,
        "Rewritten tree for [" + sql + "] should not contain a " + unit + " qualifier, but it did."
    );
  }

  /**
   * SqlShuttle that records whether it visited any plain
   * {@link SqlIntervalQualifier} for the given unit (with null timeFrameName).
   */
  private static class UnitFinder extends org.apache.calcite.sql.util.SqlShuttle
  {
    private final TimeUnitRange targetUnit;
    boolean found;

    UnitFinder(TimeUnitRange targetUnit)
    {
      this.targetUnit = targetUnit;
    }

    @Override
    public SqlNode visit(SqlIntervalQualifier intervalQualifier)
    {
      if (intervalQualifier.timeUnitRange == targetUnit
          && intervalQualifier.timeFrameName == null) {
        found = true;
      }
      return intervalQualifier;
    }
  }
}
