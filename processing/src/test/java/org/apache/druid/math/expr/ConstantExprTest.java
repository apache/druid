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

import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategiesTest.NullableLongPair;
import org.apache.druid.segment.column.TypeStrategiesTest.NullableLongPairTypeStrategy;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class ConstantExprTest extends InitializedNullHandlingTest
{
  @Test
  public void testLongArrayExpr()
  {
    checkExpr(
        new ArrayExpr(ExpressionType.LONG_ARRAY, new Long[] {1L, 3L}),
        true,
        "[1, 3]",
        "ARRAY<LONG>[1, 3]"
    );
  }

  @Test
  public void testStringArrayExpr()
  {
    checkExpr(
        new ArrayExpr(ExpressionType.STRING_ARRAY, new String[] {"foo", "bar"}),
        true,
        "[foo, bar]",
        "ARRAY<STRING>['foo', 'bar']"
    );
  }

  @Test
  public void testBigIntegerExpr()
  {
    checkExpr(
        new BigIntegerExpr(BigInteger.valueOf(37L)),
        true,
        "37",
        "37"
    );
  }

  @Test
  public void testComplexExpr()
  {
    TypeStrategies.registerComplex("nullablePair", new NullableLongPairTypeStrategy());
    checkExpr(
        new ComplexExpr(
            ExpressionTypeFactory.getInstance().ofComplex("nullablePair"),
            new NullableLongPair(21L, 37L)
        ),
        true,
        "Pair{lhs=21, rhs=37}",
        "complex_decode_base64('nullablePair', 'AAAAAAAAAAAVAAAAAAAAAAAl')"
    );
  }

  @Test
  public void testDoubleExpr()
  {
    checkExpr(
        new DoubleExpr(11.73D),
        true,
        "11.73",
        "11.73"
    );
  }

  @Test
  public void testNullDoubleExpr()
  {
    TypeStrategies.registerComplex("nullablePair", new NullableLongPairTypeStrategy());
    checkExpr(
        new NullDoubleExpr(),
        true,
        "null",
        "null"
    );
  }

  @Test
  public void testNullLongExpr()
  {
    checkExpr(
        new NullLongExpr(),
        true,
        "null",
        "null"
    );
  }

  @Test
  public void testLong()
  {
    checkExpr(
        new LongExpr(11L),
        true,
        "11",
        "11"
    );
  }

  @Test
  public void testString()
  {
    checkExpr(
        new StringExpr("some"),
        true,
        "some",
        "'some'"
    );
  }

  @Test
  public void test()
  {
    checkExpr(
        new StringExpr("some"),
        true,
        "some",
        "'some'"
    );
  }

  private void checkExpr(Expr expr, boolean supportsSingleThreaded, String expectedToString,
      String expectedStringify)
  {
    if (expr.getLiteralValue() != null) {
      assertNotSame(expr.eval(null), expr.eval(null));
    }
    Expr singleExpr = Expr.singleThreaded(expr);
    if (supportsSingleThreaded) {
      assertSame(singleExpr.eval(null), singleExpr.eval(null));
    } else {
      assertNotSame(singleExpr.eval(null), singleExpr.eval(null));
    }
    assertEquals(expectedToString, expr.toString());
    assertEquals(expectedStringify, expr.stringify());
    assertEquals(expectedToString, singleExpr.toString());
    assertEquals(expectedStringify, singleExpr.stringify());
  }
}
