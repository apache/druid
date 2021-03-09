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

import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class OutputTypeTest extends InitializedNullHandlingTest
{
  private final Expr.InputBindingInspector inspector = inspectorFromMap(
      ImmutableMap.<String, ExprType>builder().put("x", ExprType.STRING)
                                              .put("x_", ExprType.STRING)
                                              .put("y", ExprType.LONG)
                                              .put("y_", ExprType.LONG)
                                              .put("z", ExprType.DOUBLE)
                                              .put("z_", ExprType.DOUBLE)
                                              .put("a", ExprType.STRING_ARRAY)
                                              .put("a_", ExprType.STRING_ARRAY)
                                              .put("b", ExprType.LONG_ARRAY)
                                              .put("b_", ExprType.LONG_ARRAY)
                                              .put("c", ExprType.DOUBLE_ARRAY)
                                              .put("c_", ExprType.DOUBLE_ARRAY)
                                              .build()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConstantsAndIdentifiers()
  {
    assertOutputType("'hello'", inspector, ExprType.STRING);
    assertOutputType("23", inspector, ExprType.LONG);
    assertOutputType("3.2", inspector, ExprType.DOUBLE);
    assertOutputType("['a', 'b']", inspector, ExprType.STRING_ARRAY);
    assertOutputType("[1,2,3]", inspector, ExprType.LONG_ARRAY);
    assertOutputType("[1.0]", inspector, ExprType.DOUBLE_ARRAY);
    assertOutputType("x", inspector, ExprType.STRING);
    assertOutputType("y", inspector, ExprType.LONG);
    assertOutputType("z", inspector, ExprType.DOUBLE);
    assertOutputType("a", inspector, ExprType.STRING_ARRAY);
    assertOutputType("b", inspector, ExprType.LONG_ARRAY);
    assertOutputType("c", inspector, ExprType.DOUBLE_ARRAY);
  }

  @Test
  public void testUnaryOperators()
  {
    assertOutputType("-1", inspector, ExprType.LONG);
    assertOutputType("-1.1", inspector, ExprType.DOUBLE);
    assertOutputType("-y", inspector, ExprType.LONG);
    assertOutputType("-z", inspector, ExprType.DOUBLE);

    assertOutputType("!'true'", inspector, ExprType.LONG);
    assertOutputType("!1", inspector, ExprType.LONG);
    assertOutputType("!1.1", inspector, ExprType.DOUBLE);
    assertOutputType("!x", inspector, ExprType.LONG);
    assertOutputType("!y", inspector, ExprType.LONG);
    assertOutputType("!z", inspector, ExprType.DOUBLE);
  }

  @Test
  public void testBinaryMathOperators()
  {
    assertOutputType("1+1", inspector, ExprType.LONG);
    assertOutputType("1-1", inspector, ExprType.LONG);
    assertOutputType("1*1", inspector, ExprType.LONG);
    assertOutputType("1/1", inspector, ExprType.LONG);
    assertOutputType("1^1", inspector, ExprType.LONG);
    assertOutputType("1%1", inspector, ExprType.LONG);

    assertOutputType("y+y_", inspector, ExprType.LONG);
    assertOutputType("y-y_", inspector, ExprType.LONG);
    assertOutputType("y*y_", inspector, ExprType.LONG);
    assertOutputType("y/y_", inspector, ExprType.LONG);
    assertOutputType("y^y_", inspector, ExprType.LONG);
    assertOutputType("y%y_", inspector, ExprType.LONG);

    assertOutputType("y+z", inspector, ExprType.DOUBLE);
    assertOutputType("y-z", inspector, ExprType.DOUBLE);
    assertOutputType("y*z", inspector, ExprType.DOUBLE);
    assertOutputType("y/z", inspector, ExprType.DOUBLE);
    assertOutputType("y^z", inspector, ExprType.DOUBLE);
    assertOutputType("y%z", inspector, ExprType.DOUBLE);

    assertOutputType("z+z_", inspector, ExprType.DOUBLE);
    assertOutputType("z-z_", inspector, ExprType.DOUBLE);
    assertOutputType("z*z_", inspector, ExprType.DOUBLE);
    assertOutputType("z/z_", inspector, ExprType.DOUBLE);
    assertOutputType("z^z_", inspector, ExprType.DOUBLE);
    assertOutputType("z%z_", inspector, ExprType.DOUBLE);

    assertOutputType("y>y_", inspector, ExprType.LONG);
    assertOutputType("y_<y", inspector, ExprType.LONG);
    assertOutputType("y_<=y", inspector, ExprType.LONG);
    assertOutputType("y_>=y", inspector, ExprType.LONG);
    assertOutputType("y_==y", inspector, ExprType.LONG);
    assertOutputType("y_!=y", inspector, ExprType.LONG);
    assertOutputType("y_ && y", inspector, ExprType.LONG);
    assertOutputType("y_ || y", inspector, ExprType.LONG);

    assertOutputType("z>y_", inspector, ExprType.DOUBLE);
    assertOutputType("z<y", inspector, ExprType.DOUBLE);
    assertOutputType("z<=y", inspector, ExprType.DOUBLE);
    assertOutputType("y>=z", inspector, ExprType.DOUBLE);
    assertOutputType("z==y", inspector, ExprType.DOUBLE);
    assertOutputType("z!=y", inspector, ExprType.DOUBLE);
    assertOutputType("z && y", inspector, ExprType.DOUBLE);
    assertOutputType("y || z", inspector, ExprType.DOUBLE);

    assertOutputType("z>z_", inspector, ExprType.DOUBLE);
    assertOutputType("z<z_", inspector, ExprType.DOUBLE);
    assertOutputType("z<=z_", inspector, ExprType.DOUBLE);
    assertOutputType("z_>=z", inspector, ExprType.DOUBLE);
    assertOutputType("z==z_", inspector, ExprType.DOUBLE);
    assertOutputType("z!=z_", inspector, ExprType.DOUBLE);
    assertOutputType("z && z_", inspector, ExprType.DOUBLE);
    assertOutputType("z_ || z", inspector, ExprType.DOUBLE);

    assertOutputType("1*(2 + 3.0)", inspector, ExprType.DOUBLE);
  }

  @Test
  public void testUnivariateMathFunctions()
  {
    assertOutputType("pi()", inspector, ExprType.DOUBLE);
    assertOutputType("abs(x)", inspector, ExprType.STRING);
    assertOutputType("abs(y)", inspector, ExprType.LONG);
    assertOutputType("abs(z)", inspector, ExprType.DOUBLE);
    assertOutputType("cos(y)", inspector, ExprType.DOUBLE);
    assertOutputType("cos(z)", inspector, ExprType.DOUBLE);
  }

  @Test
  public void testBivariateMathFunctions()
  {
    assertOutputType("div(y,y_)", inspector, ExprType.LONG);
    assertOutputType("div(y,z_)", inspector, ExprType.LONG);
    assertOutputType("div(z,z_)", inspector, ExprType.LONG);

    assertOutputType("max(y,y_)", inspector, ExprType.LONG);
    assertOutputType("max(y,z_)", inspector, ExprType.DOUBLE);
    assertOutputType("max(z,z_)", inspector, ExprType.DOUBLE);

    assertOutputType("hypot(y,y_)", inspector, ExprType.DOUBLE);
    assertOutputType("hypot(y,z_)", inspector, ExprType.DOUBLE);
    assertOutputType("hypot(z,z_)", inspector, ExprType.DOUBLE);
  }

  @Test
  public void testConditionalFunctions()
  {
    assertOutputType("if(y, 'foo', 'bar')", inspector, ExprType.STRING);
    assertOutputType("if(y,2,3)", inspector, ExprType.LONG);
    assertOutputType("if(y,2,3.0)", inspector, ExprType.DOUBLE);

    assertOutputType(
        "case_simple(x,'baz','is baz','foo','is foo','is other')",
        inspector,
        ExprType.STRING
    );
    assertOutputType(
        "case_simple(y,2,2,3,3,4)",
        inspector,
        ExprType.LONG
    );
    assertOutputType(
        "case_simple(z,2.0,2.0,3.0,3.0,4.0)",
        inspector,
        ExprType.DOUBLE
    );

    assertOutputType(
        "case_simple(y,2,2,3,3.0,4)",
        inspector,
        ExprType.DOUBLE
    );
    assertOutputType(
        "case_simple(z,2.0,2.0,3.0,3.0,null)",
        inspector,
        ExprType.DOUBLE
    );

    assertOutputType(
        "case_searched(x=='baz','is baz',x=='foo','is foo','is other')",
        inspector,
        ExprType.STRING
    );
    assertOutputType(
        "case_searched(y==1,1,y==2,2,0)",
        inspector,
        ExprType.LONG
    );
    assertOutputType(
        "case_searched(z==1.0,1.0,z==2.0,2.0,0.0)",
        inspector,
        ExprType.DOUBLE
    );
    assertOutputType(
        "case_searched(y==1,1,y==2,2.0,0)",
        inspector,
        ExprType.DOUBLE
    );
    assertOutputType(
        "case_searched(z==1.0,1,z==2.0,2,null)",
        inspector,
        ExprType.LONG
    );
    assertOutputType(
        "case_searched(z==1.0,1.0,z==2.0,2.0,null)",
        inspector,
        ExprType.DOUBLE
    );

    assertOutputType("nvl(x, 'foo')", inspector, ExprType.STRING);
    assertOutputType("nvl(y, 1)", inspector, ExprType.LONG);
    assertOutputType("nvl(y, 1.1)", inspector, ExprType.DOUBLE);
    assertOutputType("nvl(z, 2.0)", inspector, ExprType.DOUBLE);
    assertOutputType("nvl(y, 2.0)", inspector, ExprType.DOUBLE);
    assertOutputType("isnull(x)", inspector, ExprType.LONG);
    assertOutputType("isnull(y)", inspector, ExprType.LONG);
    assertOutputType("isnull(z)", inspector, ExprType.LONG);
    assertOutputType("notnull(x)", inspector, ExprType.LONG);
    assertOutputType("notnull(y)", inspector, ExprType.LONG);
    assertOutputType("notnull(z)", inspector, ExprType.LONG);
  }

  @Test
  public void testStringFunctions()
  {
    assertOutputType("concat(x, 'foo')", inspector, ExprType.STRING);
    assertOutputType("concat(y, 'foo')", inspector, ExprType.STRING);
    assertOutputType("concat(z, 'foo')", inspector, ExprType.STRING);

    assertOutputType("strlen(x)", inspector, ExprType.LONG);
    assertOutputType("format('%s', x)", inspector, ExprType.STRING);
    assertOutputType("format('%s', y)", inspector, ExprType.STRING);
    assertOutputType("format('%s', z)", inspector, ExprType.STRING);
    assertOutputType("strpos(x, x_)", inspector, ExprType.LONG);
    assertOutputType("strpos(x, y)", inspector, ExprType.LONG);
    assertOutputType("strpos(x, z)", inspector, ExprType.LONG);
    assertOutputType("substring(x, 1, 2)", inspector, ExprType.STRING);
    assertOutputType("left(x, 1)", inspector, ExprType.STRING);
    assertOutputType("right(x, 1)", inspector, ExprType.STRING);
    assertOutputType("replace(x, 'foo', '')", inspector, ExprType.STRING);
    assertOutputType("lower(x)", inspector, ExprType.STRING);
    assertOutputType("upper(x)", inspector, ExprType.STRING);
    assertOutputType("reverse(x)", inspector, ExprType.STRING);
    assertOutputType("repeat(x, 4)", inspector, ExprType.STRING);
  }

  @Test
  public void testArrayFunctions()
  {
    assertOutputType("array(1, 2, 3)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array(1, 2, 3.0)", inspector, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_length(a)", inspector, ExprType.LONG);
    assertOutputType("array_length(b)", inspector, ExprType.LONG);
    assertOutputType("array_length(c)", inspector, ExprType.LONG);

    assertOutputType("string_to_array(x, ',')", inspector, ExprType.STRING_ARRAY);

    assertOutputType("array_to_string(a, ',')", inspector, ExprType.STRING);
    assertOutputType("array_to_string(b, ',')", inspector, ExprType.STRING);
    assertOutputType("array_to_string(c, ',')", inspector, ExprType.STRING);

    assertOutputType("array_offset(a, 1)", inspector, ExprType.STRING);
    assertOutputType("array_offset(b, 1)", inspector, ExprType.LONG);
    assertOutputType("array_offset(c, 1)", inspector, ExprType.DOUBLE);

    assertOutputType("array_ordinal(a, 1)", inspector, ExprType.STRING);
    assertOutputType("array_ordinal(b, 1)", inspector, ExprType.LONG);
    assertOutputType("array_ordinal(c, 1)", inspector, ExprType.DOUBLE);

    assertOutputType("array_offset_of(a, 'a')", inspector, ExprType.LONG);
    assertOutputType("array_offset_of(b, 1)", inspector, ExprType.LONG);
    assertOutputType("array_offset_of(c, 1.0)", inspector, ExprType.LONG);

    assertOutputType("array_ordinal_of(a, 'a')", inspector, ExprType.LONG);
    assertOutputType("array_ordinal_of(b, 1)", inspector, ExprType.LONG);
    assertOutputType("array_ordinal_of(c, 1.0)", inspector, ExprType.LONG);

    assertOutputType("array_append(x, x_)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_append(a, x_)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_append(y, y_)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_append(b, y_)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_append(z, z_)", inspector, ExprType.DOUBLE_ARRAY);
    assertOutputType("array_append(c, z_)", inspector, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_concat(x, a)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_concat(a, a)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_concat(y, b)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_concat(b, b)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_concat(z, c)", inspector, ExprType.DOUBLE_ARRAY);
    assertOutputType("array_concat(c, c)", inspector, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_contains(a, 'a')", inspector, ExprType.LONG);
    assertOutputType("array_contains(b, 1)", inspector, ExprType.LONG);
    assertOutputType("array_contains(c, 2.0)", inspector, ExprType.LONG);

    assertOutputType("array_overlap(a, a)", inspector, ExprType.LONG);
    assertOutputType("array_overlap(b, b)", inspector, ExprType.LONG);
    assertOutputType("array_overlap(c, c)", inspector, ExprType.LONG);

    assertOutputType("array_slice(a, 1, 2)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_slice(b, 1, 2)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_slice(c, 1, 2)", inspector, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_prepend(x, a)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_prepend(x, x_)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("array_prepend(y, b)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_prepend(y, y_)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("array_prepend(z, c)", inspector, ExprType.DOUBLE_ARRAY);
    assertOutputType("array_prepend(z, z_)", inspector, ExprType.DOUBLE_ARRAY);
  }

  @Test
  public void testReduceFunctions()
  {
    assertOutputType("greatest('B', x, 'A')", inspector, ExprType.STRING);
    assertOutputType("greatest(y, 0)", inspector, ExprType.LONG);
    assertOutputType("greatest(34.0, z, 5.0, 767.0)", inspector, ExprType.DOUBLE);

    assertOutputType("least('B', x, 'A')", inspector, ExprType.STRING);
    assertOutputType("least(y, 0)", inspector, ExprType.LONG);
    assertOutputType("least(34.0, z, 5.0, 767.0)", inspector, ExprType.DOUBLE);
  }

  @Test
  public void testApplyFunctions()
  {
    assertOutputType("map((x) -> concat(x, 'foo'), x)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("map((x) -> x + x, y)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("map((x) -> x + x, z)", inspector, ExprType.DOUBLE_ARRAY);
    assertOutputType("map((x) -> concat(x, 'foo'), a)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("map((x) -> x + x, b)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("map((x) -> x + x, c)", inspector, ExprType.DOUBLE_ARRAY);
    assertOutputType(
        "cartesian_map((x, y) -> concat(x, y), ['foo', 'bar', 'baz', 'foobar'], ['bar', 'baz'])",
        inspector,
        ExprType.STRING_ARRAY
    );
    assertOutputType("fold((x, acc) -> x + acc, y, 0)", inspector, ExprType.LONG);
    assertOutputType("fold((x, acc) -> x + acc, y, y)", inspector, ExprType.LONG);
    assertOutputType("fold((x, acc) -> x + acc, y, 1.0)", inspector, ExprType.DOUBLE);
    assertOutputType("fold((x, acc) -> x + acc, y, z)", inspector, ExprType.DOUBLE);

    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, 0)", inspector, ExprType.LONG);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, y)", inspector, ExprType.LONG);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, 1.0)", inspector, ExprType.DOUBLE);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, z)", inspector, ExprType.DOUBLE);

    assertOutputType("filter((x) -> x == 'foo', a)", inspector, ExprType.STRING_ARRAY);
    assertOutputType("filter((x) -> x > 1, b)", inspector, ExprType.LONG_ARRAY);
    assertOutputType("filter((x) -> x > 1, c)", inspector, ExprType.DOUBLE_ARRAY);

    assertOutputType("any((x) -> x == 'foo', a)", inspector, ExprType.LONG);
    assertOutputType("any((x) -> x > 1, b)", inspector, ExprType.LONG);
    assertOutputType("any((x) -> x > 1.2, c)", inspector, ExprType.LONG);

    assertOutputType("all((x) -> x == 'foo', a)", inspector, ExprType.LONG);
    assertOutputType("all((x) -> x > 1, b)", inspector, ExprType.LONG);
    assertOutputType("all((x) -> x > 1.2, c)", inspector, ExprType.LONG);
  }


  @Test
  public void testEvalAutoConversion()
  {
    final ExprEval<?> nullStringEval = ExprEval.of(null);
    final ExprEval<?> stringEval = ExprEval.of("wat");
    final ExprEval<?> longEval = ExprEval.of(1L);
    final ExprEval<?> doubleEval = ExprEval.of(1.0);
    final ExprEval<?> arrayEval = ExprEval.ofLongArray(new Long[]{1L, 2L, 3L});

    // only long stays long
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.autoDetect(longEval, longEval));
    // only string stays string
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.autoDetect(nullStringEval, nullStringEval));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.autoDetect(stringEval, stringEval));
    // if only 1 argument is a string, preserve the other type
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.autoDetect(nullStringEval, longEval));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.autoDetect(longEval, nullStringEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(doubleEval, nullStringEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(nullStringEval, doubleEval));
    // for operators, doubles is the catch all
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(longEval, doubleEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(doubleEval, longEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(doubleEval, doubleEval));
    // ... even when non-null strings are used with non-double types
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(longEval, stringEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(doubleEval, stringEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(stringEval, doubleEval));
    // arrays are not a good idea to use with this method
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(arrayEval, nullStringEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(arrayEval, doubleEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(arrayEval, longEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(nullStringEval, arrayEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(doubleEval, arrayEval));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.autoDetect(longEval, arrayEval));
  }

  @Test
  public void testOperatorAutoConversion()
  {
    // nulls output other
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.operator(ExprType.LONG, null));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.operator(null, ExprType.LONG));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.DOUBLE, null));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(null, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.operator(ExprType.STRING, null));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.operator(null, ExprType.STRING));
    // only long stays long
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.operator(ExprType.LONG, ExprType.LONG));
    // only string stays string
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.operator(ExprType.STRING, ExprType.STRING));
    // for operators, doubles is the catch all
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.LONG, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.DOUBLE, ExprType.LONG));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.DOUBLE, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.DOUBLE, ExprType.STRING));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.STRING, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.STRING, ExprType.LONG));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.operator(ExprType.LONG, ExprType.STRING));
    // unless it is an array, and those have to be the same
    Assert.assertEquals(ExprType.LONG_ARRAY, ExprTypeConversion.operator(ExprType.LONG_ARRAY, ExprType.LONG_ARRAY));
    Assert.assertEquals(
        ExprType.DOUBLE_ARRAY,
        ExprTypeConversion.operator(ExprType.DOUBLE_ARRAY, ExprType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExprType.STRING_ARRAY,
        ExprTypeConversion.operator(ExprType.STRING_ARRAY, ExprType.STRING_ARRAY)
    );
  }

  @Test
  public void testFunctionAutoConversion()
  {
    // nulls output other
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.function(ExprType.LONG, null));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.function(null, ExprType.LONG));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.function(ExprType.DOUBLE, null));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.function(null, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(ExprType.STRING, null));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(null, ExprType.STRING));
    // only long stays long
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.function(ExprType.LONG, ExprType.LONG));
    // any double makes all doubles
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.function(ExprType.LONG, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.function(ExprType.DOUBLE, ExprType.LONG));
    Assert.assertEquals(ExprType.DOUBLE, ExprTypeConversion.function(ExprType.DOUBLE, ExprType.DOUBLE));
    // any string makes become string
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(ExprType.LONG, ExprType.STRING));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(ExprType.STRING, ExprType.LONG));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(ExprType.DOUBLE, ExprType.STRING));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(ExprType.STRING, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.function(ExprType.STRING, ExprType.STRING));
    // unless it is an array, and those have to be the same
    Assert.assertEquals(ExprType.LONG_ARRAY, ExprTypeConversion.function(ExprType.LONG_ARRAY, ExprType.LONG_ARRAY));
    Assert.assertEquals(
        ExprType.DOUBLE_ARRAY,
        ExprTypeConversion.function(ExprType.DOUBLE_ARRAY, ExprType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExprType.STRING_ARRAY,
        ExprTypeConversion.function(ExprType.STRING_ARRAY, ExprType.STRING_ARRAY)
    );
  }

  @Test
  public void testIntegerFunctionAutoConversion()
  {
    // nulls output other
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(ExprType.LONG, null));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(null, ExprType.LONG));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(ExprType.DOUBLE, null));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(null, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(ExprType.STRING, null));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(null, ExprType.STRING));
    // all numbers are longs
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(ExprType.LONG, ExprType.LONG));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(ExprType.LONG, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(ExprType.DOUBLE, ExprType.LONG));
    Assert.assertEquals(ExprType.LONG, ExprTypeConversion.integerMathFunction(ExprType.DOUBLE, ExprType.DOUBLE));
    // any string makes become string
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(ExprType.LONG, ExprType.STRING));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(ExprType.STRING, ExprType.LONG));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(ExprType.DOUBLE, ExprType.STRING));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(ExprType.STRING, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.STRING, ExprTypeConversion.integerMathFunction(ExprType.STRING, ExprType.STRING));
    // unless it is an array
    Assert.assertEquals(ExprType.LONG_ARRAY, ExprTypeConversion.integerMathFunction(ExprType.LONG_ARRAY, ExprType.LONG_ARRAY));
    Assert.assertEquals(
        ExprType.DOUBLE_ARRAY,
        ExprTypeConversion.integerMathFunction(ExprType.DOUBLE_ARRAY, ExprType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExprType.STRING_ARRAY,
        ExprTypeConversion.integerMathFunction(ExprType.STRING_ARRAY, ExprType.STRING_ARRAY)
    );
  }

  @Test
  public void testAutoConversionArrayMismatchArrays()
  {
    expectedException.expect(IAE.class);
    ExprTypeConversion.function(ExprType.DOUBLE_ARRAY, ExprType.LONG_ARRAY);
  }

  @Test
  public void testAutoConversionArrayMismatchArrayScalar()
  {
    expectedException.expect(IAE.class);
    ExprTypeConversion.function(ExprType.DOUBLE_ARRAY, ExprType.LONG);
  }

  @Test
  public void testAutoConversionArrayMismatchScalarArray()
  {
    expectedException.expect(IAE.class);
    ExprTypeConversion.function(ExprType.DOUBLE, ExprType.LONG_ARRAY);
  }

  private void assertOutputType(String expression, Expr.InputBindingInspector inspector, ExprType outputType)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil(), false);
    Assert.assertEquals(outputType, expr.getOutputType(inspector));
  }

  Expr.InputBindingInspector inspectorFromMap(Map<String, ExprType> types)
  {
    return types::get;
  }
}
