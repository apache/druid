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

package org.apache.druid.query.expressions;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class SleepExprTest extends InitializedNullHandlingTest
{
  private final ExprMacroTable exprMacroTable = new ExprMacroTable(Collections.singletonList(new SleepExprMacro()));

  @Test
  public void testSleep()
  {
    assertExpr("sleep(1)");
    assertExpr("sleep(0.5)");
    assertExpr("sleep(null)");
    assertExpr("sleep(0)");
    assertExpr("sleep(-1)");

    assertTimeElapsed("sleep(1)", 1000);
    assertTimeElapsed("sleep(0.5)", 500);
    assertTimeElapsed("sleep(null)", 0);
    assertTimeElapsed("sleep(0)", 0);
    assertTimeElapsed("sleep(-1)", 0);
  }

  private void assertTimeElapsed(String expression, long expectedTimeElapsedMs)
  {
    final long detla = 50;
    final long before = System.currentTimeMillis();
    final Expr expr = Parser.parse(expression, exprMacroTable);
    expr.eval(InputBindings.nilBindings()).value();
    final long after = System.currentTimeMillis();
    final long elapsed = after - before;
    Assert.assertTrue(
        StringUtils.format("Expected [%s], but actual elapsed was [%s]", expectedTimeElapsedMs, elapsed),
        elapsed >= expectedTimeElapsedMs
        && elapsed < expectedTimeElapsedMs + detla
    );
  }

  private void assertExpr(final String expression)
  {
    final Expr expr = Parser.parse(expression, exprMacroTable);
    Assert.assertNull(expression, expr.eval(InputBindings.nilBindings()).value());

    final Expr exprNoFlatten = Parser.parse(expression, exprMacroTable, false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), exprMacroTable);
    Assert.assertNull(expr.stringify(), roundTrip.eval(InputBindings.nilBindings()).value());

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), exprMacroTable);
    Assert.assertNull(expr.stringify(), roundTripFlatten.eval(InputBindings.nilBindings()).value());

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
    Assert.assertArrayEquals(expr.getCacheKey(), roundTrip.getCacheKey());
    Assert.assertArrayEquals(expr.getCacheKey(), roundTripFlatten.getCacheKey());
  }
}
