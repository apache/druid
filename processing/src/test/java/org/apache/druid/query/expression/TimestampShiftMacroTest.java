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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Years;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;

public class TimestampShiftMacroTest extends MacroTestBase
{
  public TimestampShiftMacroTest()
  {
    super(new TimestampShiftExprMacro());
  }

  private DateTime timestamp = DateTimes.of("2020-11-05T04:05:06");
  //TIME_SHIFT(<timestamp_expr>, <period>, <step>, [<timezone>])

  @Test
  public void testZeroArguments()
  {
    assertException(
        IAE.class,
        "Function[timestamp_shift] requires 3 to 4 arguments",
        () -> apply(Collections.emptyList())
    );
  }

  @Test
  public void testOneArguments()
  {
    assertException(
        IAE.class,
        "Function[timestamp_shift] requires 3 to 4 arguments",
        () -> apply(
            ImmutableList.of(
                ExprEval.of(timestamp.getMillis()).toExpr()
            )
        )
    );
  }

  @Test
  public void testTwoArguments()
  {
    assertException(
        IAE.class,
        "Function[timestamp_shift] requires 3 to 4 arguments",
        () -> apply(
            ImmutableList.of(
                ExprEval.of(timestamp.getMillis()).toExpr(),
                ExprEval.ofString("P1M").toExpr()
            )
        )
    );
  }

  @Test
  public void testMoreThanFourArguments()
  {
    assertException(
        IAE.class,
        "Function[timestamp_shift] requires 3 to 4 arguments",
        () -> apply(
            ImmutableList.of(
                ExprEval.of(timestamp.getMillis()).toExpr(),
                ExprEval.ofString("P1M").toExpr(),
                ExprEval.ofString("1").toExpr(),
                ExprEval.ofString("+08:00").toExpr(),
                ExprEval.ofString("extra").toExpr()
            )
        )
    );
  }

  @Test
  public void testZeroStep()
  {
    int step = 0;
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("P1M").toExpr(),
            ExprEval.of(step).toExpr()
        ));

    Assertions.assertEquals(
        timestamp.withPeriodAdded(Months.ONE, step).getMillis(),
        expr.eval(InputBindings.nilBindings()).asLong()
    );
  }

  @Test
  public void testPositiveStep()
  {
    int step = 5;
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("P1M").toExpr(),
            ExprEval.of(step).toExpr()
        ));

    Assertions.assertEquals(
        timestamp.withPeriodAdded(Months.ONE, step).getMillis(),
        expr.eval(InputBindings.nilBindings()).asLong()
    );
  }

  @Test
  public void testNegativeStep()
  {
    int step = -3;
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("P1M").toExpr(),
            ExprEval.of(step).toExpr()
        ));

    Assertions.assertEquals(
        timestamp.withPeriodAdded(Months.ONE, step).getMillis(),
        expr.eval(InputBindings.nilBindings()).asLong()
    );
  }

  @Test
  public void testPeriodMinute()
  {
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("PT1M").toExpr(),
            ExprEval.of(1).toExpr()
        ));

    Assertions.assertEquals(
        timestamp.withPeriodAdded(Minutes.ONE, 1).getMillis(),
        expr.eval(InputBindings.nilBindings()).asLong()
    );
  }

  @Test
  public void testPeriodDay()
  {
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("P1D").toExpr(),
            ExprEval.of(1).toExpr()
        ));

    Assertions.assertEquals(
        timestamp.withPeriodAdded(Days.ONE, 1).getMillis(),
        expr.eval(InputBindings.nilBindings()).asLong()
    );
  }

  @Test
  public void testPeriodYearAndTimeZone()
  {
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("P1Y").toExpr(),
            ExprEval.of(1).toExpr(),
            ExprEval.ofString("America/Los_Angeles").toExpr()
        ));

    Assertions.assertEquals(
        timestamp.toDateTime(DateTimes.inferTzFromString("America/Los_Angeles")).withPeriodAdded(Years.ONE, 1).getMillis(),
        expr.eval(InputBindings.nilBindings()).asLong()
    );
  }

  @Test
  public void testDynamicExpression()
  {
    // step parameter is not a literal expression
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.of(timestamp.getMillis()).toExpr(),
            ExprEval.ofString("P1Y").toExpr(),
            Parser.parse("\"step\"", ExprMacroTable.nil()), // "step" is not a literal
            ExprEval.ofString("America/Los_Angeles").toExpr()
        ));

    final int step = 3;
    Assertions.assertEquals(
        timestamp.toDateTime(DateTimes.inferTzFromString("America/Los_Angeles")).withPeriodAdded(Years.ONE, step).getMillis(),
        expr.eval(new Expr.ObjectBinding()
        {
          @Nullable
          @Override
          public ExpressionType getType(String name)
          {
            return null;
          }

          @Nullable
          @Override
          public Object get(String name)
          {
            if ("step".equals(name)) {
              return step;
            } else {
              throw new IAE("Invalid bindings");
            }
          }
        }).asLong()
    );
  }

  @Test
  public void testNull()
  {
    Expr expr = apply(
        ImmutableList.of(
            ExprEval.ofLong(null).toExpr(),
            ExprEval.ofString("P1M").toExpr(),
            ExprEval.of(1L).toExpr()
        )
    );

    Assertions.assertNull(expr.eval(InputBindings.nilBindings()).value());
  }
}
