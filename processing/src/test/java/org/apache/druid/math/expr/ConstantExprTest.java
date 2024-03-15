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

package org.apache.druid.math.expr;

import org.apache.druid.math.expr.Expr.ObjectBinding;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategiesTest.NullableLongPair;
import org.apache.druid.segment.column.TypeStrategiesTest.NullableLongPairTypeStrategy;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

public class ConstantExprTest extends InitializedNullHandlingTest
{
  @Test
  public void testLongArrayExpr()
  {
    ArrayExpr arrayExpr = new ArrayExpr(ExpressionType.LONG_ARRAY, new Long[] {1L, 3L});
    checkExpr(
        arrayExpr,
        "[1, 3]",
        "ARRAY<LONG>[1, 3]",
        arrayExpr
    );
  }

  @Test
  public void testStringArrayExpr()
  {
    ArrayExpr arrayExpr = new ArrayExpr(ExpressionType.STRING_ARRAY, new String[] {"foo", "bar"});
    checkExpr(
        arrayExpr,
        "[foo, bar]",
        "ARRAY<STRING>['foo', 'bar']",
        arrayExpr
    );
  }

  @Test
  public void testBigIntegerExpr()
  {
    checkExpr(
        new BigIntegerExpr(BigInteger.valueOf(37L)),
        "37",
        "37",
        // after reparsing it will become a LongExpr
        new LongExpr(37L)
    );
  }

  @Test
  public void testComplexExpr()
  {
    TypeStrategies.registerComplex("nullablePair", new NullableLongPairTypeStrategy());
    ComplexExpr complexExpr = new ComplexExpr(
        ExpressionTypeFactory.getInstance().ofComplex("nullablePair"),
        new NullableLongPair(21L, 37L)
    );
    checkExpr(
        complexExpr,
        "Pair{lhs=21, rhs=37}",
        "complex_decode_base64('nullablePair', 'AAAAAAAAAAAVAAAAAAAAAAAl')",
        complexExpr
    );
  }

  @Test
  public void testDoubleExpr()
  {
    checkExpr(
        new DoubleExpr(11.73D),
        "11.73",
        "11.73",
        new DoubleExpr(11.73D)
    );
  }

  @Test
  public void testNullDoubleExpr()
  {
    TypeStrategies.registerComplex("nullablePair", new NullableLongPairTypeStrategy());
    checkExpr(
        new NullDoubleExpr(),
        "null",
        "null",
        // the expressions 'null' is always parsed as a StringExpr(null)
        new StringExpr(null)
    );
  }

  @Test
  public void testNullLongExpr()
  {
    checkExpr(
        new NullLongExpr(),
        "null",
        "null",
        // the expressions 'null' is always parsed as a StringExpr(null)
        new StringExpr(null)
    );
  }

  @Test
  public void testLong()
  {
    checkExpr(
        new LongExpr(11L),
        "11",
        "11",
        new LongExpr(11L)
    );
  }

  @Test
  public void testString()
  {
    checkExpr(
        new StringExpr("some"),
        "some",
        "'some'",
        new StringExpr("some")
    );
  }

  @Test
  public void testStringNull()
  {
    checkExpr(
        new StringExpr(null),
        null,
        "null",
        new StringExpr(null)
    );
  }

  private void checkExpr(
      Expr expr,
      String expectedToString,
      String expectedStringify,
      Expr expectedReparsedExpr
  )
  {
    final ObjectBinding bindings = InputBindings.nilBindings();
    if (expr.getLiteralValue() != null) {
      Assert.assertNotSame(expr.eval(bindings), expr.eval(bindings));
    }
    final Expr singleExpr = Expr.singleThreaded(expr, bindings);
    Assert.assertArrayEquals(expr.getCacheKey(), singleExpr.getCacheKey());
    Assert.assertSame(singleExpr.eval(bindings), singleExpr.eval(bindings));
    Assert.assertEquals(expectedToString, expr.toString());
    Assert.assertEquals(expectedStringify, expr.stringify());
    Assert.assertEquals(expectedToString, singleExpr.toString());
    final String stringify = singleExpr.stringify();
    final Expr reParsedExpr = Parser.parse(stringify, ExprMacroTable.nil());
    Assert.assertEquals(expectedReparsedExpr, reParsedExpr);
    Assert.assertArrayEquals(expr.getCacheKey(), expectedReparsedExpr.getCacheKey());
  }
}
