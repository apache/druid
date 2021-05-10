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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class LookupExprMacroTest extends InitializedNullHandlingTest
{
  private static final Expr.ObjectBinding BINDINGS = InputBindings.withMap(
      ImmutableMap.<String, Object>builder()
          .put("x", "foo")
          .build()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testLookup()
  {
    assertExpr("lookup(x, 'lookyloo')", "xfoo");
  }

  @Test
  public void testLookupNotFound()
  {
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Lookup [lookylook] not found");
    assertExpr("lookup(x, 'lookylook')", null);
  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE);
    Assert.assertEquals(expression, expectedResult, expr.eval(BINDINGS).value());

    final Expr exprNotFlattened = Parser.parse(expression, LookupEnabledTestExprMacroTable.INSTANCE, false);
    final Expr roundTripNotFlattened =
        Parser.parse(exprNotFlattened.stringify(), LookupEnabledTestExprMacroTable.INSTANCE);
    Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTripNotFlattened.eval(BINDINGS).value());

    final Expr roundTrip = Parser.parse(expr.stringify(), LookupEnabledTestExprMacroTable.INSTANCE);
    Assert.assertEquals(exprNotFlattened.stringify(), expectedResult, roundTrip.eval(BINDINGS).value());
  }
}
