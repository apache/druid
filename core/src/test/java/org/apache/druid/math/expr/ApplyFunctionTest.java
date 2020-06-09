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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ApplyFunctionTest extends InitializedNullHandlingTest
{
  private Expr.ObjectBinding bindings;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("x", "foo");
    builder.put("y", 2);
    builder.put("z", 3.1);
    builder.put("a", new String[] {"foo", "bar", "baz", "foobar"});
    builder.put("b", new Long[] {1L, 2L, 3L, 4L, 5L});
    builder.put("c", new Double[] {3.1, 4.2, 5.3});
    builder.put("d", new String[] {null});
    builder.put("e", new String[] {null, "foo", "bar"});
    builder.put("f", new String[0]);
    bindings = Parser.withMap(builder.build());
  }

  @Test
  public void testMap()
  {
    assertExpr("map((x) -> concat(x, 'foo'), ['foo', 'bar', 'baz', 'foobar'])", new String[] {"foofoo", "barfoo", "bazfoo", "foobarfoo"});
    assertExpr("map((x) -> concat(x, 'foo'), a)", new String[] {"foofoo", "barfoo", "bazfoo", "foobarfoo"});

    assertExpr("map((x) -> x + 1, [1, 2, 3, 4, 5])", new Long[] {2L, 3L, 4L, 5L, 6L});
    assertExpr("map((x) -> x + 1, b)", new Long[] {2L, 3L, 4L, 5L, 6L});

    assertExpr("map((c) -> c + z, [3.1, 4.2, 5.3])", new Double[]{6.2, 7.3, 8.4});
    assertExpr("map((c) -> c + z, c)", new Double[]{6.2, 7.3, 8.4});

    assertExpr("map((x) -> x + 1, map((x) -> x + 1, [1, 2, 3, 4, 5]))", new Long[] {3L, 4L, 5L, 6L, 7L});
    assertExpr("map((x) -> x + 1, map((x) -> x + 1, b))", new Long[] {3L, 4L, 5L, 6L, 7L});
    assertExpr("map(() -> 1, [1, 2, 3, 4, 5])", new Long[] {1L, 1L, 1L, 1L, 1L});
  }

  @Test
  public void testCartesianMap()
  {
    assertExpr("cartesian_map((x, y) -> concat(x, y), ['foo', 'bar', 'baz', 'foobar'], ['bar', 'baz'])", new String[] {"foobar", "foobaz", "barbar", "barbaz", "bazbar", "bazbaz", "foobarbar", "foobarbaz"});
    assertExpr("cartesian_map((x, y, z) -> concat(concat(x, y), z), ['foo', 'bar', 'baz', 'foobar'], ['bar', 'baz'], ['omg'])", new String[] {"foobaromg", "foobazomg", "barbaromg", "barbazomg", "bazbaromg", "bazbazomg", "foobarbaromg", "foobarbazomg"});
    assertExpr("cartesian_map(() -> 1, [1, 2], [1, 2, 3])", new Long[] {1L, 1L, 1L, 1L, 1L, 1L});
    assertExpr("cartesian_map((x, y) -> concat(x, y), d, d)", new String[] {null});
    assertExpr("cartesian_map((x, y) -> concat(x, y), d, f)", new String[0]);
    if (NullHandling.replaceWithDefault()) {
      assertExpr("cartesian_map((x, y) -> concat(x, y), d, e)", new String[]{null, "foo", "bar"});
      assertExpr("cartesian_map((x, y) -> concat(x, y), e, e)", new String[] {null, "foo", "bar", "foo", "foofoo", "foobar", "bar", "barfoo", "barbar"});
    } else {
      assertExpr("cartesian_map((x, y) -> concat(x, y), d, e)", new String[]{null, null, null});
      assertExpr("cartesian_map((x, y) -> concat(x, y), e, e)", new String[] {null, null, null, null, "foofoo", "foobar", null, "barfoo", "barbar"});
    }
  }

  @Test
  public void testFilter()
  {
    assertExpr("filter((x) -> strlen(x) > 3, ['foo', 'bar', 'baz', 'foobar'])", new String[] {"foobar"});
    assertExpr("filter((x) -> strlen(x) > 3, a)", new String[] {"foobar"});

    assertExpr("filter((x) -> x > 2, [1, 2, 3, 4, 5])", new Long[] {3L, 4L, 5L});
    assertExpr("filter((x) -> x > 2, b)", new Long[] {3L, 4L, 5L});
  }

  @Test
  public void testFold()
  {
    assertExpr("fold((x, y) -> x + y, [1, 1, 1, 1, 1], 0)", 5L);
    assertExpr("fold((b, acc) -> b * acc, map((b) -> b * 2, filter(b -> b > 3, b)), 1)", 80L);
    assertExpr("fold((a, acc) -> concat(a, acc), a, '')", "foobarbazbarfoo");
    assertExpr("fold((a, acc) -> array_append(acc, a), a, [])", new String[]{"foo", "bar", "baz", "foobar"});
    assertExpr("fold((a, acc) -> array_append(acc, a), b, <LONG>[])", new Long[]{1L, 2L, 3L, 4L, 5L});
  }

  @Test
  public void testCartesianFold()
  {
    assertExpr("cartesian_fold((x, y, acc) -> x + y + acc, [1, 1, 1, 1, 1], [1, 1], 0)", 20L);
  }

  @Test
  public void testAnyMatch()
  {
    assertExpr("any(x -> x > 3, [1, 2, 3, 4])", 1L);
    assertExpr("any(x -> x > 3, [1, 2, 3])", 0L);
    assertExpr("any(x -> x, map(x -> x > 3, [1, 2, 3, 4]))", 1L);
    assertExpr("any(x -> x, map(x -> x > 3, [1, 2, 3]))", 0L);
  }

  @Test
  public void testAllMatch()
  {
    assertExpr("all(x -> x > 0, [1, 2, 3, 4])", 1L);
    assertExpr("all(x -> x > 1, [1, 2, 3, 4])", 0L);
    assertExpr("all(x -> x, map(x -> x > 0, [1, 2, 3, 4]))", 1L);
    assertExpr("all(x -> x, map(x -> x > 1, [1, 2, 3, 4]))", 0L);
  }

  @Test
  public void testScoping()
  {
    assertExpr("map(b -> b + 1, b)", new Long[]{2L, 3L, 4L, 5L, 6L});
    assertExpr("fold((b, acc) -> acc + b, map(b -> b + 1, b), 0)", 20L);
    assertExpr("fold((b, acc) -> acc + b, map(b -> b + 1, b), fold((b, acc) -> acc + b, map(b -> b + 1, b), 0))", 40L);
    assertExpr("fold((b, acc) -> acc + b, map(b -> b + 1, b), 0) + fold((b, acc) -> acc + b, map(b -> b + 1, b), 0)", 40L);
    assertExpr("fold((b, acc) -> acc + b, map(b -> b + 1, b), fold((b, acc) -> acc + b, map(b -> b + 1, b), 0) + fold((b, acc) -> acc + b, map(b -> b + 1, b), 0))", 60L);
  }

  @Test
  public void testInvalidArgCount()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("lambda expression argument count does not match fold argument count");
    assertExpr("fold(() -> 1, [1, 1, 1, 1, 1], 0)", null);

    expectedException.expectMessage("lambda expression argument count does not match cartesian_fold argument count");
    assertExpr("cartesian_fold(() -> 1, [1, 1, 1, 1, 1], [1, 1], 0)", null);

    expectedException.expectMessage("lambda expression argument count does not match any argument count");
    assertExpr("any(() -> 1, [1, 2, 3, 4])", null);

    expectedException.expectMessage("lambda expression argument count does not match all argument count");
    assertExpr("all(() -> 0, [1, 2, 3, 4])", null);

  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertEquals(expression, expectedResult, expr.eval(bindings).value());

    final Expr exprNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertEquals(expr.stringify(), expectedResult, roundTrip.eval(bindings).value());

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), ExprMacroTable.nil());
    Assert.assertEquals(expr.stringify(), expectedResult, roundTripFlatten.eval(bindings).value());

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
  }

  private void assertExpr(final String expression, final Object[] expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    final Object[] result = expr.eval(bindings).asArray();
    if (expectedResult.length != 0 || result == null || result.length != 0) {
      Assert.assertArrayEquals(expression, expectedResult, result);
    }

    final Expr exprNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), ExprMacroTable.nil());
    final Object[] resultRoundTrip = roundTrip.eval(bindings).asArray();
    if (expectedResult.length != 0 || resultRoundTrip == null || resultRoundTrip.length != 0) {
      Assert.assertArrayEquals(expr.stringify(), expectedResult, resultRoundTrip);
    }

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), ExprMacroTable.nil());
    final Object[] resultRoundTripFlatten = roundTripFlatten.eval(bindings).asArray();
    if (expectedResult.length != 0 || resultRoundTripFlatten == null || resultRoundTripFlatten.length != 0) {
      Assert.assertArrayEquals(expr.stringify(), expectedResult, resultRoundTripFlatten);
    }

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
  }

  private void assertExpr(final String expression, final Double[] expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Double[] result = (Double[]) expr.eval(bindings).value();
    Assert.assertEquals(expectedResult.length, result.length);
    for (int i = 0; i < result.length; i++) {
      Assert.assertEquals(expression, expectedResult[i], result[i], 0.00001); // something is lame somewhere..
    }

    final Expr exprNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), ExprMacroTable.nil());
    Double[] resultRoundTrip = (Double[]) roundTrip.eval(bindings).value();
    Assert.assertEquals(expectedResult.length, resultRoundTrip.length);
    for (int i = 0; i < resultRoundTrip.length; i++) {
      Assert.assertEquals(expression, expectedResult[i], resultRoundTrip[i], 0.00001);
    }

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), ExprMacroTable.nil());
    Double[] resultRoundTripFlatten = (Double[]) roundTripFlatten.eval(bindings).value();
    Assert.assertEquals(expectedResult.length, resultRoundTripFlatten.length);
    for (int i = 0; i < resultRoundTripFlatten.length; i++) {
      Assert.assertEquals(expression, expectedResult[i], resultRoundTripFlatten[i], 0.00001);
    }

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
  }
}
