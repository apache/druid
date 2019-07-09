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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FunctionTest
{
  private Expr.ObjectBinding bindings;

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
      assertExpr("concat(x,' ',nonexistent,' ',y)", null);
    }

    assertExpr("concat(z)", "3.1");
    assertExpr("concat()", null);
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
    assertExpr("lpad(x, 0, 'ab')", null);
    assertExpr("lpad(x, 5, null)", null);
    assertExpr("lpad(null, 5, x)", null);
  }

  @Test
  public void testRpad()
  {
    assertExpr("rpad(x, 5, 'ab')", "fooab");
    assertExpr("rpad(x, 4, 'ab')", "fooa");
    assertExpr("rpad(x, 2, 'ab')", "fo");
    assertExpr("rpad(x, 0, 'ab')", null);
    assertExpr("rpad(x, 5, null)", null);
    assertExpr("rpad(null, 5, x)", null);
  }

  @Test
  public void testArrayConstructor()
  {
    assertExpr("array(1, 2, 3, 4)", new Long[]{1L, 2L, 3L, 4L});
    assertExpr("array(1, 2, 3, 'bar')", new Long[]{1L, 2L, 3L, null});
    assertExpr("array(1.0)", new Double[]{1.0});
    assertExpr("array('foo', 'bar')", new String[]{"foo", "bar"});
  }

  @Test
  public void testArrayLength()
  {
    assertExpr("array_length([1,2,3])", 3L);
    assertExpr("array_length(a)", 4);
  }

  @Test
  public void testArrayOffset()
  {
    assertExpr("array_offset([1, 2, 3], 2)", 3L);
    assertExpr("array_offset([1, 2, 3], 3)", null);
    assertExpr("array_offset(a, 2)", "baz");
  }

  @Test
  public void testArrayOrdinal()
  {
    assertExpr("array_ordinal([1, 2, 3], 3)", 3L);
    assertExpr("array_ordinal([1, 2, 3], 4)", null);
    assertExpr("array_ordinal(a, 3)", "baz");
  }

  @Test
  public void testArrayOffsetOf()
  {
    assertExpr("array_offset_of([1, 2, 3], 3)", 2L);
    assertExpr("array_offset_of([1, 2, 3], 4)", NullHandling.replaceWithDefault() ? -1L : null);
    assertExpr("array_offset_of(a, 'baz')", 2);
  }

  @Test
  public void testArrayOrdinalOf()
  {
    assertExpr("array_ordinal_of([1, 2, 3], 3)", 3L);
    assertExpr("array_ordinal_of([1, 2, 3], 4)", NullHandling.replaceWithDefault() ? -1L : null);
    assertExpr("array_ordinal_of(a, 'baz')", 3);
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
    assertExpr("array_append([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertExpr("array_append([1, 2, 3], 'bar')", new Long[]{1L, 2L, 3L, null});
    assertExpr("array_append([], 1)", new String[]{"1"});
    assertExpr("array_append(cast([], 'LONG_ARRAY'), 1)", new Long[]{1L});
  }

  @Test
  public void testArrayConcat()
  {
    assertExpr("array_concat([1, 2, 3], [2, 4, 6])", new Long[]{1L, 2L, 3L, 2L, 4L, 6L});
    assertExpr("array_concat([1, 2, 3], 4)", new Long[]{1L, 2L, 3L, 4L});
    assertExpr("array_concat(0, [1, 2, 3])", new Long[]{0L, 1L, 2L, 3L});
    assertExpr("array_concat(map(y -> y * 3, b), [1, 2, 3])", new Long[]{3L, 6L, 9L, 12L, 15L, 1L, 2L, 3L});
    assertExpr("array_concat(0, 1)", new Long[]{0L, 1L});
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
    assertExpr("string_to_array('1,2,3', ',')", new String[]{"1", "2", "3"});
    assertExpr("string_to_array('1', ',')", new String[]{"1"});
    assertExpr("string_to_array(array_to_string(a, ','), ',')", new String[]{"foo", "bar", "baz", "foobar"});
  }

  @Test
  public void testArrayCast()
  {
    assertExpr("cast([1, 2, 3], 'STRING_ARRAY')", new String[]{"1", "2", "3"});
    assertExpr("cast([1, 2, 3], 'DOUBLE_ARRAY')", new Double[]{1.0, 2.0, 3.0});
    assertExpr("cast(c, 'LONG_ARRAY')", new Long[]{3L, 4L, 5L});
    assertExpr("cast(string_to_array(array_to_string(b, ','), ','), 'LONG_ARRAY')", new Long[]{1L, 2L, 3L, 4L, 5L});
    assertExpr("cast(['1.0', '2.0', '3.0'], 'LONG_ARRAY')", new Long[]{1L, 2L, 3L});
  }

  @Test
  public void testArraySlice()
  {
    assertExpr("array_slice([1, 2, 3, 4], 1, 3)", new Long[] {2L, 3L});
    assertExpr("array_slice([1.0, 2.1, 3.2, 4.3], 2)", new Double[] {3.2, 4.3});
    assertExpr("array_slice(['a', 'b', 'c', 'd'], 4, 6)", new String[] {null, null});
    assertExpr("array_slice([1, 2, 3, 4], 2, 2)", new Long[] {});
    assertExpr("array_slice([1, 2, 3, 4], 5, 7)", null);
    assertExpr("array_slice([1, 2, 3, 4], 2, 1)", null);
  }

  @Test
  public void testArrayPrepend()
  {
    assertExpr("array_prepend(4, [1, 2, 3])", new Long[]{4L, 1L, 2L, 3L});
    assertExpr("array_prepend('bar', [1, 2, 3])", new Long[]{null, 1L, 2L, 3L});
    assertExpr("array_prepend(1, [])", new String[]{"1"});
    assertExpr("array_prepend(1, cast([], 'LONG_ARRAY'))", new Long[]{1L});
  }

  private void assertExpr(final String expression, final Object expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertEquals(expression, expectedResult, expr.eval(bindings).value());
  }

  private void assertExpr(final String expression, final Object[] expectedResult)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertArrayEquals(expression, expectedResult, expr.eval(bindings).asArray());
  }
}
