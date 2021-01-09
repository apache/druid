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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.Set;

public class FunctionTest extends InitializedNullHandlingTest
{
  private Expr.ObjectBinding bindings;

  @Before
  public void setup()
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
        .put("x", "foo")
        .put("y", 2)
        .put("z", 3.1)
        .put("d", 34.56D)
        .put("maxLong", Long.MAX_VALUE)
        .put("minLong", Long.MIN_VALUE)
        .put("f", 12.34F)
        .put("nan", Double.NaN)
        .put("inf", Double.POSITIVE_INFINITY)
        .put("-inf", Double.NEGATIVE_INFINITY)
        .put("o", 0)
        .put("od", 0D)
        .put("of", 0F)
        .put("a", new String[] {"foo", "bar", "baz", "foobar"})
        .put("b", new Long[] {1L, 2L, 3L, 4L, 5L})
        .put("c", new Double[] {3.1, 4.2, 5.3});
    bindings = Parser.withMap(builder.build());
  }

  @Test
  public void testCaseSimple()
  {
    assertExpr("case_simple(x,'baz','is baz','foo','is foo','is other')", "is foo");
    assertExpr("case_simple(x,'baz','is baz','bar','is bar','is other')", "is other");
    assertExpr("case_simple(y,2,'is 2',3,'is 3','is other')", "is 2");
    assertExpr("case_simple(z,2,'is 2',3,'is 3','is other')", "is other");
  }

  @Test
  public void testCaseSearched()
  {
    assertExpr("case_searched(x=='baz','is baz',x=='foo','is foo','is other')", "is foo");
    assertExpr("case_searched(x=='baz','is baz',x=='bar','is bar','is other')", "is other");
    assertExpr("case_searched(y==2,'is 2',y==3,'is 3','is other')", "is 2");
    assertExpr("case_searched(z==2,'is 2',z==3,'is 3','is other')", "is other");
  }

  @Test
  public void testConcat()
  {
    assertExpr("concat(x,' ',y)", "foo 2");
    if (NullHandling.replaceWithDefault()) {
      assertExpr("concat(x,' ',nonexistent,' ',y)", "foo  2");
    } else {
      assertArrayExpr("concat(x,' ',nonexistent,' ',y)", null);
    }

    assertExpr("concat(z)", "3.1");
    assertArrayExpr("concat()", null);
  }

  @Test
  public void testReplace()
  {
    assertExpr("replace(x,'oo','ab')", "fab");
    assertExpr("replace(x,x,'ab')", "ab");
    assertExpr("replace(x,'oo',y)", "f2");
  }

  @Test
  public void testSubstring()
  {
    assertExpr("substring(x,0,2)", "fo");
    assertExpr("substring(x,1,2)", "oo");
    assertExpr("substring(x,y,1)", "o");
    assertExpr("substring(x,0,-1)", "foo");
    assertExpr("substring(x,0,100)", "foo");
  }

  @Test
  public void testStrlen()
  {
    assertExpr("strlen(x)", 3L);
    assertExpr("strlen(nonexistent)", NullHandling.defaultLongValue());
  }

  @Test
  public void testStrpos()
  {
    assertExpr("strpos(x, 'o')", 1L);
    assertExpr("strpos(x, 'o', 0)", 1L);
    assertExpr("strpos(x, 'o', 1)", 1L);
    assertExpr("strpos(x, 'o', 2)", 2L);
    assertExpr("strpos(x, 'o', 3)", -1L);
    assertExpr("strpos(x, '')", 0L);
    assertExpr("strpos(x, 'x')", -1L);
  }

  @Test
  public void testLower()
  {
    assertExpr("lower('FOO')", "foo");
  }

  @Test
  public void testUpper()
  {
    assertExpr("upper(x)", "FOO");
  }

  @Test
  public void testIsNull()
  {
    assertExpr("isnull(null)", 1L);
    assertExpr("isnull('abc')", 0L);
  }

  @Test
  public void testIsNotNull()
  {
    assertExpr("notnull(null)", 0L);
    assertExpr("notnull('abc')", 1L);
  }

  @Test
  public void testLpad()
  {
    assertExpr("lpad(x, 5, 'ab')", "abfoo");
    assertExpr("lpad(x, 4, 'ab')", "afoo");
    assertExpr("lpad(x, 2, 'ab')", "fo");
    assertExpr("lpad(x, -1, 'ab')", NullHandling.replaceWithDefault() ? null : "");
    assertExpr("lpad(null, 5, 'ab')", null);
    assertExpr("lpad(x, 2, '')", NullHandling.replaceWithDefault() ? null : "fo");
    assertExpr("lpad(x, 6, '')", NullHandling.replaceWithDefault() ? null : "foo");
    assertExpr("lpad('', 3, '*')", NullHandling.replaceWithDefault() ? null : "***");
    assertExpr("lpad(x, 2, null)", null);
    assertExpr("lpad(a, 4, '*')", "[foo");
    assertExpr("lpad(a, 2, '*')", "[f");
    assertExpr("lpad(a, 2, '')", NullHandling.replaceWithDefault() ? null : "[f");
    assertExpr("lpad(b, 4, '*')", "[1, ");
    assertExpr("lpad(b, 2, '')", NullHandling.replaceWithDefault() ? null : "[1");
    assertExpr("lpad(b, 2, null)", null);
    assertExpr("lpad(x, 5, x)", "fofoo");
    assertExpr("lpad(x, 5, y)", "22foo");
    assertExpr("lpad(x, 5, z)", "3.foo");
    assertExpr("lpad(y, 5, x)", "foof2");
    assertExpr("lpad(z, 5, y)", "223.1");
  }

  @Test
  public void testRpad()
  {
    assertExpr("rpad(x, 5, 'ab')", "fooab");
    assertExpr("rpad(x, 4, 'ab')", "fooa");
    assertExpr("rpad(x, 2, 'ab')", "fo");
    assertExpr("rpad(x, -1, 'ab')", NullHandling.replaceWithDefault() ? null : "");
    assertExpr("rpad(null, 5, 'ab')", null);
    assertExpr("rpad(x, 2, '')", NullHandling.replaceWithDefault() ? null : "fo");
    assertExpr("rpad(x, 6, '')", NullHandling.replaceWithDefault() ? null : "foo");
    assertExpr("rpad('', 3, '*')", NullHandling.replaceWithDefault() ? null : "***");
    assertExpr("rpad(x, 2, null)", null);
    assertExpr("rpad(a, 2, '*')", "[f");
    assertExpr("rpad(a, 2, '')", NullHandling.replaceWithDefault() ? null : "[f");
    assertExpr("rpad(b, 4, '*')", "[1, ");
    assertExpr("rpad(b, 2, '')", NullHandling.replaceWithDefault() ? null : "[1");
    assertExpr("rpad(b, 2, null)", null);
    assertExpr("rpad(x, 5, x)", "foofo");
    assertExpr("rpad(x, 5, y)", "foo22");
    assertExpr("rpad(x, 5, z)", "foo3.");
    assertExpr("rpad(y, 5, x)", "2foof");
    assertExpr("rpad(z, 5, y)", "3.122");
  }

  @Test
  public void testArrayConstructor()
  {
    assertArrayExpr("array(1, 2, 3, 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array(1, 2, 3, 'bar')", new Long[]{1L, 2L, 3L, null});
    assertArrayExpr("array(1.0)", new Double[]{1.0});
    assertArrayExpr("array('foo', 'bar')", new String[]{"foo", "bar"});
  }

  @Test
  public void testArrayLength()
  {
    assertExpr("array_length([1,2,3])", 3L);
    assertExpr("array_length(a)", 4L);
  }

  @Test
  public void testArrayOffset()
  {
    assertExpr("array_offset([1, 2, 3], 2)", 3L);
    assertArrayExpr("array_offset([1, 2, 3], 3)", null);
    assertExpr("array_offset(a, 2)", "baz");
  }

  @Test
  public void testArrayOrdinal()
  {
    assertExpr("array_ordinal([1, 2, 3], 3)", 3L);
    assertArrayExpr("array_ordinal([1, 2, 3], 4)", null);
    assertExpr("array_ordinal(a, 3)", "baz");
  }

  @Test
  public void testArrayOffsetOf()
  {
    assertExpr("array_offset_of([1, 2, 3], 3)", 2L);
    assertExpr("array_offset_of([1, 2, 3], 4)", NullHandling.replaceWithDefault() ? -1L : null);
    assertExpr("array_offset_of(a, 'baz')", 2L);
  }

  @Test
  public void testArrayOrdinalOf()
  {
    assertExpr("array_ordinal_of([1, 2, 3], 3)", 3L);
    assertExpr("array_ordinal_of([1, 2, 3], 4)", NullHandling.replaceWithDefault() ? -1L : null);
    assertExpr("array_ordinal_of(a, 'baz')", 3L);
  }

  @Test
  public void testArrayContains()
  {
    assertExpr("array_contains([1, 2, 3], 2)", 1L);
    assertExpr("array_contains([1, 2, 3], 4)", 0L);
    assertExpr("array_contains([1, 2, 3], [2, 3])", 1L);
    assertExpr("array_contains([1, 2, 3], [3, 4])", 0L);
    assertExpr("array_contains(b, [3, 4])", 1L);
  }

  @Test
  public void testArrayOverlap()
  {
    assertExpr("array_overlap([1, 2, 3], [2, 4, 6])", 1L);
    assertExpr("array_overlap([1, 2, 3], [4, 5, 6])", 0L);
  }

  @Test
  public void testArrayAppend()
  {
    assertArrayExpr("array_append([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array_append([1, 2, 3], 'bar')", new Long[]{1L, 2L, 3L, null});
    assertArrayExpr("array_append([], 1)", new String[]{"1"});
    assertArrayExpr("array_append(<LONG>[], 1)", new Long[]{1L});
  }

  @Test
  public void testArrayConcat()
  {
    assertArrayExpr("array_concat([1, 2, 3], [2, 4, 6])", new Long[]{1L, 2L, 3L, 2L, 4L, 6L});
    assertArrayExpr("array_concat([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertArrayExpr("array_concat(0, [1, 2, 3])", new Long[]{0L, 1L, 2L, 3L});
    assertArrayExpr("array_concat(map(y -> y * 3, b), [1, 2, 3])", new Long[]{3L, 6L, 9L, 12L, 15L, 1L, 2L, 3L});
    assertArrayExpr("array_concat(0, 1)", new Long[]{0L, 1L});
  }

  @Test
  public void testArrayToString()
  {
    assertExpr("array_to_string([1, 2, 3], ',')", "1,2,3");
    assertExpr("array_to_string([1], '|')", "1");
    assertExpr("array_to_string(a, '|')", "foo|bar|baz|foobar");
  }

  @Test
  public void testStringToArray()
  {
    assertArrayExpr("string_to_array('1,2,3', ',')", new String[]{"1", "2", "3"});
    assertArrayExpr("string_to_array('1', ',')", new String[]{"1"});
    assertArrayExpr("string_to_array(array_to_string(a, ','), ',')", new String[]{"foo", "bar", "baz", "foobar"});
  }

  @Test
  public void testArrayCast()
  {
    assertArrayExpr("cast([1, 2, 3], 'STRING_ARRAY')", new String[]{"1", "2", "3"});
    assertArrayExpr("cast([1, 2, 3], 'DOUBLE_ARRAY')", new Double[]{1.0, 2.0, 3.0});
    assertArrayExpr("cast(c, 'LONG_ARRAY')", new Long[]{3L, 4L, 5L});
    assertArrayExpr("cast(string_to_array(array_to_string(b, ','), ','), 'LONG_ARRAY')", new Long[]{1L, 2L, 3L, 4L, 5L});
    assertArrayExpr("cast(['1.0', '2.0', '3.0'], 'LONG_ARRAY')", new Long[]{1L, 2L, 3L});
  }

  @Test
  public void testArraySlice()
  {
    assertArrayExpr("array_slice([1, 2, 3, 4], 1, 3)", new Long[] {2L, 3L});
    assertArrayExpr("array_slice([1.0, 2.1, 3.2, 4.3], 2)", new Double[] {3.2, 4.3});
    assertArrayExpr("array_slice(['a', 'b', 'c', 'd'], 4, 6)", new String[] {null, null});
    assertArrayExpr("array_slice([1, 2, 3, 4], 2, 2)", new Long[] {});
    assertArrayExpr("array_slice([1, 2, 3, 4], 5, 7)", null);
    assertArrayExpr("array_slice([1, 2, 3, 4], 2, 1)", null);
  }

  @Test
  public void testArrayPrepend()
  {
    assertArrayExpr("array_prepend(4, [1, 2, 3])", new Long[]{4L, 1L, 2L, 3L});
    assertArrayExpr("array_prepend('bar', [1, 2, 3])", new Long[]{null, 1L, 2L, 3L});
    assertArrayExpr("array_prepend(1, [])", new String[]{"1"});
    assertArrayExpr("array_prepend(1, <LONG>[])", new Long[]{1L});
    assertArrayExpr("array_prepend(1, <DOUBLE>[])", new Double[]{1.0});
  }

  @Test
  public void testRoundWithNonNumericValuesShouldReturn0()
  {
    assertExpr("round(nan)", 0D);
    assertExpr("round(nan, 5)", 0D);
    //CHECKSTYLE.OFF: Regexp
    assertExpr("round(inf)", Double.MAX_VALUE);
    assertExpr("round(inf, 4)", Double.MAX_VALUE);
    assertExpr("round(-inf)", -1 * Double.MAX_VALUE);
    assertExpr("round(-inf, 3)", -1 * Double.MAX_VALUE);
    assertExpr("round(-inf, -5)", -1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp

    // Calculations that result in non numeric numbers
    assertExpr("round(0/od)", 0D);
    assertExpr("round(od/od)", 0D);
    //CHECKSTYLE.OFF: Regexp
    assertExpr("round(1/od)", Double.MAX_VALUE);
    assertExpr("round(-1/od)", -1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp

    assertExpr("round(0/of)", 0D);
    assertExpr("round(of/of)", 0D);
    //CHECKSTYLE.OFF: Regexp
    assertExpr("round(1/of)", Double.MAX_VALUE);
    assertExpr("round(-1/of)", -1 * Double.MAX_VALUE);
    //CHECKSTYLE.ON: Regexp
  }

  @Test
  public void testRoundWithLong()
  {
    assertExpr("round(y)", 2L);
    assertExpr("round(y, 2)", 2L);
    assertExpr("round(y, -1)", 0L);
  }

  @Test
  public void testRoundWithDouble()
  {
    assertExpr("round(d)", 35D);
    assertExpr("round(d, 2)", 34.56D);
    assertExpr("round(d, y)", 34.56D);
    assertExpr("round(d, 1)", 34.6D);
    assertExpr("round(d, -1)", 30D);
  }

  @Test
  public void testRoundWithFloat()
  {
    assertExpr("round(f)", 12D);
    assertExpr("round(f, 2)", 12.34D);
    assertExpr("round(f, y)", 12.34D);
    assertExpr("round(f, 1)", 12.3D);
    assertExpr("round(f, -1)", 10D);
  }

  @Test
  public void testRoundWithExtremeNumbers()
  {
    assertExpr("round(maxLong)", BigDecimal.valueOf(Long.MAX_VALUE).setScale(0, RoundingMode.HALF_UP).longValue());
    assertExpr("round(minLong)", BigDecimal.valueOf(Long.MIN_VALUE).setScale(0, RoundingMode.HALF_UP).longValue());
    // overflow
    assertExpr("round(maxLong + 1, 1)", BigDecimal.valueOf(Long.MIN_VALUE).setScale(1, RoundingMode.HALF_UP).longValue());
    // underflow
    assertExpr("round(minLong - 1, -2)", BigDecimal.valueOf(Long.MAX_VALUE).setScale(-2, RoundingMode.HALF_UP).longValue());

    assertExpr("round(CAST(maxLong, 'DOUBLE') + 1, 1)", BigDecimal.valueOf(((double) Long.MAX_VALUE) + 1).setScale(1, RoundingMode.HALF_UP).doubleValue());
    assertExpr("round(CAST(minLong, 'DOUBLE') - 1, -2)", BigDecimal.valueOf(((double) Long.MIN_VALUE) - 1).setScale(-2, RoundingMode.HALF_UP).doubleValue());
  }

  @Test
  public void testRoundWithInvalidFirstArgument()
  {
    Set<Pair<String, String>> invalidArguments = ImmutableSet.of(
        Pair.of("b", "LONG_ARRAY"),
        Pair.of("x", "STRING"),
        Pair.of("c", "DOUBLE_ARRAY"),
        Pair.of("a", "STRING_ARRAY")

    );
    for (Pair<String, String> argAndType : invalidArguments) {
      try {
        assertExpr(String.format(Locale.ENGLISH, "round(%s)", argAndType.lhs), null);
        Assert.fail("Did not throw IllegalArgumentException");
      }
      catch (IllegalArgumentException e) {
        Assert.assertEquals(
            String.format(
                Locale.ENGLISH,
                "The first argument to the function[round] should be integer or double type but got the type: %s",
                argAndType.rhs
            ),
            e.getMessage()
        );
      }
    }
  }

  @Test
  public void testRoundWithInvalidSecondArgument()
  {
    Set<Pair<String, String>> invalidArguments = ImmutableSet.of(
        Pair.of("1.2", "DOUBLE"),
        Pair.of("x", "STRING"),
        Pair.of("a", "STRING_ARRAY"),
        Pair.of("c", "DOUBLE_ARRAY")

    );
    for (Pair<String, String> argAndType : invalidArguments) {
      try {
        assertExpr(String.format(Locale.ENGLISH, "round(d, %s)", argAndType.lhs), null);
        Assert.fail("Did not throw IllegalArgumentException");
      }
      catch (IllegalArgumentException e) {
        Assert.assertEquals(
            String.format(
                Locale.ENGLISH,
                "The second argument to the function[round] should be integer type but got the type: %s",
                argAndType.rhs
            ),
            e.getMessage()
        );
      }
    }
  }

  @Test
  public void testGreatest()
  {
    // Same types
    assertExpr("greatest(y, 0)", 2L);
    assertExpr("greatest(34.0, z, 5.0, 767.0)", 767.0);
    assertExpr("greatest('B', x, 'A')", "foo");

    // Different types
    assertExpr("greatest(-1, z, 'A')", "A");
    assertExpr("greatest(-1, z)", 3.1);
    assertExpr("greatest(1, 'A')", "A");

    // Invalid types
    try {
      assertExpr("greatest(1, ['A'])", null);
      Assert.fail("Did not throw IllegalArgumentException");
    }
    catch (IllegalArgumentException e) {
      Assert.assertEquals("Function[greatest] does not accept STRING_ARRAY types", e.getMessage());
    }

    // Null handling
    assertExpr("greatest()", null);
    assertExpr("greatest(null, null)", null);
    assertExpr("greatest(1, null, 'A')", "A");
  }

  @Test
  public void testLeast()
  {
    // Same types
    assertExpr("least(y, 0)", 0L);
    assertExpr("least(34.0, z, 5.0, 767.0)", 3.1);
    assertExpr("least('B', x, 'A')", "A");

    // Different types
    assertExpr("least(-1, z, 'A')", "-1");
    assertExpr("least(-1, z)", -1.0);
    assertExpr("least(1, 'A')", "1");

    // Invalid types
    try {
      assertExpr("least(1, [2, 3])", null);
      Assert.fail("Did not throw IllegalArgumentException");
    }
    catch (IllegalArgumentException e) {
      Assert.assertEquals("Function[least] does not accept LONG_ARRAY types", e.getMessage());
    }

    // Null handling
    assertExpr("least()", null);
    assertExpr("least(null, null)", null);
    assertExpr("least(1, null, 'A')", "1");
  }

  @Test
  public void testBitwise()
  {
    assertExpr("bitwiseAnd(3, 1)", 1L);
    assertExpr("bitwiseAnd(2, 1)", 0L);
    assertExpr("bitwiseOr(3, 1)", 3L);
    assertExpr("bitwiseOr(2, 1)", 3L);
    assertExpr("bitwiseXor(3, 1)", 2L);
    assertExpr("bitwiseXor(2, 1)", 3L);
    assertExpr("bitwiseShiftLeft(2, 1)", 4L);
    assertExpr("bitwiseShiftRight(2, 1)", 1L);
    assertExpr("bitwiseAnd(bitwiseComplement(1), 7)", 6L);
    assertExpr("bitwiseAnd('2', '1')", null);
    assertExpr("bitwiseAnd(2, '1')", 0L);

    // doubles are cast
    assertExpr("bitwiseOr(2.345, 1)", 3L);
    assertExpr("bitwiseOr(2, 1.3)", 3L);
    assertExpr("bitwiseAnd(2.345, 2.0)", 2L);

    // but can be converted to be double-like
    assertExpr(
        "bitwiseAnd(bitwiseConvertDoubleToLongBits(2.345), bitwiseConvertDoubleToLongBits(2.0))",
        4611686018427387904L
    );
    assertExpr(
        "bitwiseConvertLongBitsToDouble(bitwiseAnd(bitwiseConvertDoubleToLongBits(2.345), bitwiseConvertDoubleToLongBits(2.0)))",
        2.0
    );
    assertExpr("bitwiseConvertDoubleToLongBits(2.0)", 4611686018427387904L);
    assertExpr("bitwiseConvertDoubleToLongBits(bitwiseConvertDoubleToLongBits(2.0))", 4886405595696988160L);
    assertExpr("bitwiseConvertLongBitsToDouble(4611686018427387904)", 2.0);
    assertExpr("bitwiseConvertLongBitsToDouble(bitwiseConvertLongBitsToDouble(4611686018427387904))", 1.0E-323);
  }

  private void assertExpr(final String expression, @Nullable final Object expectedResult)
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

  private void assertArrayExpr(final String expression, @Nullable final Object[] expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, expr.eval(bindings).asArray());

    final Expr exprNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(exprNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, roundTrip.eval(bindings).asArray());

    final Expr roundTripFlatten = Parser.parse(expr.stringify(), ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, roundTripFlatten.eval(bindings).asArray());

    Assert.assertEquals(expr.stringify(), roundTrip.stringify());
    Assert.assertEquals(expr.stringify(), roundTripFlatten.stringify());
  }
}
