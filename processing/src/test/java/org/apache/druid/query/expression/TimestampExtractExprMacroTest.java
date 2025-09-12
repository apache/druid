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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * These tests are copied from examples here -
 * https://www.postgresql.org/docs/10/functions-datetime.html
 */
public class TimestampExtractExprMacroTest
{
  private TimestampExtractExprMacro target;

  @Before
  public void setUp()
  {
    target = new TimestampExtractExprMacro();
  }

  @Test
  public void testApplyExtractDecadeShouldExtractTheCorrectDecade()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.DECADE.toString()).toExpr()
        ));
    Assert.assertEquals(200, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractCenturyShouldExtractTheCorrectCentury()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2000-12-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.CENTURY.toString()).toExpr()
        ));
    Assert.assertEquals(20, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractCenturyShouldBeTwentyFirstCenturyIn2001()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.CENTURY.toString()).toExpr()
        ));
    Assert.assertEquals(21, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractMilleniumShouldExtractTheCorrectMillenium()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2000-12-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.MILLENNIUM.toString()).toExpr()
        ));
    Assert.assertEquals(2, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractMilleniumShouldBeThirdMilleniumIn2001()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.MILLENNIUM.toString()).toExpr()
        ));
    Assert.assertEquals(3, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractDowWithTimeZoneShouldBeFriday()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2023-12-15").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.DOW.toString()).toExpr(),
            ExprEval.of("UTC").toExpr()
        ));
    Assert.assertEquals(5, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractDowWithDynamicTimeZoneShouldBeFriday()
  {
    Expr expression = Parser.parse("timestamp_extract(time, 'DOW', timezone)", TestExprMacroTable.INSTANCE);
    Expr.ObjectBinding bindings = InputBindings.forInputSuppliers(
        ImmutableMap.of(
            "time", InputBindings.inputSupplier(ExpressionType.STRING, () -> "2023-12-15"),
            "timezone", InputBindings.inputSupplier(ExpressionType.STRING, () -> "UTC")
        )
    );
    Assert.assertEquals(5, expression.eval(bindings).asInt());
  }

  @Test
  public void testApplyExtractEpochShouldExtractTheCorrectEpochSeconds()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-01-01T00:00:00Z").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.EPOCH.toString()).toExpr()
        ));
    Assert.assertEquals(978307200, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractMillisecondShouldExtractTheCorrectMillisecond()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16T12:34:56.789Z").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.MILLISECOND.toString()).toExpr()
        ));
    Assert.assertEquals(789, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractSecondShouldExtractTheCorrectSecond()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16T12:34:45Z").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.SECOND.toString()).toExpr()
        ));
    Assert.assertEquals(45, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractMinuteShouldExtractTheCorrectMinute()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16T12:34:56Z").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.MINUTE.toString()).toExpr()
        ));
    Assert.assertEquals(34, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractHourShouldExtractTheCorrectHour()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16T12:34:56Z").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.HOUR.toString()).toExpr()
        ));
    Assert.assertEquals(12, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractDayShouldExtractTheCorrectDay()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16T12:34:56Z").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.DAY.toString()).toExpr()
        ));
    Assert.assertEquals(16, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractIsodowShouldExtractTheCorrectIsoDayOfWeek()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2023-12-15").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.ISODOW.toString()).toExpr()
        ));
    Assert.assertEquals(5, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractDoyShouldExtractTheCorrectDayOfYear()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.DOY.toString()).toExpr()
        ));
    Assert.assertEquals(47, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractWeekShouldExtractTheCorrectWeek()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.WEEK.toString()).toExpr()
        ));
    Assert.assertEquals(7, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractMonthShouldExtractTheCorrectMonth()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.MONTH.toString()).toExpr()
        ));
    Assert.assertEquals(2, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractQuarterShouldExtractTheCorrectQuarter()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.QUARTER.toString()).toExpr()
        ));
    Assert.assertEquals(1, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractQuarterSecondQuarterShouldExtractTheCorrectQuarter()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-05-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.QUARTER.toString()).toExpr()
        ));
    Assert.assertEquals(2, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractYearShouldExtractTheCorrectYear()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.YEAR.toString()).toExpr()
        ));
    Assert.assertEquals(2001, expression.eval(InputBindings.nilBindings()).asInt());
  }

  @Test
  public void testApplyExtractIsoYearShouldExtractTheCorrectIsoYear()
  {
    Expr expression = target.apply(
        ImmutableList.of(
            ExprEval.of("2001-02-16").toExpr(),
            ExprEval.of(TimestampExtractExprMacro.Unit.ISOYEAR.toString()).toExpr()
        ));
    Assert.assertEquals(2001, expression.eval(InputBindings.nilBindings()).asInt());
  }
}
