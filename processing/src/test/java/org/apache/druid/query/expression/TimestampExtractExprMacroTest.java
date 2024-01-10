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
import org.apache.druid.common.config.NullHandling;
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
  static {
    NullHandling.initializeForTests();
  }

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
}
