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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class OutputTypeTest extends InitializedNullHandlingTest
{
  private final Expr.InputBindingTypes inputTypes = inputTypesFromMap(
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
    assertOutputType("'hello'", inputTypes, ExprType.STRING);
    assertOutputType("23", inputTypes, ExprType.LONG);
    assertOutputType("3.2", inputTypes, ExprType.DOUBLE);
    assertOutputType("['a', 'b']", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("[1,2,3]", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("[1.0]", inputTypes, ExprType.DOUBLE_ARRAY);
    assertOutputType("x", inputTypes, ExprType.STRING);
    assertOutputType("y", inputTypes, ExprType.LONG);
    assertOutputType("z", inputTypes, ExprType.DOUBLE);
    assertOutputType("a", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("b", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("c", inputTypes, ExprType.DOUBLE_ARRAY);
  }

  @Test
  public void testUnaryOperators()
  {
    assertOutputType("-1", inputTypes, ExprType.LONG);
    assertOutputType("-1.1", inputTypes, ExprType.DOUBLE);
    assertOutputType("-y", inputTypes, ExprType.LONG);
    assertOutputType("-z", inputTypes, ExprType.DOUBLE);

    assertOutputType("!'true'", inputTypes, ExprType.LONG);
    assertOutputType("!1", inputTypes, ExprType.LONG);
    assertOutputType("!1.1", inputTypes, ExprType.DOUBLE);
    assertOutputType("!x", inputTypes, ExprType.LONG);
    assertOutputType("!y", inputTypes, ExprType.LONG);
    assertOutputType("!z", inputTypes, ExprType.DOUBLE);
  }

  @Test
  public void testBinaryMathOperators()
  {
    assertOutputType("1+1", inputTypes, ExprType.LONG);
    assertOutputType("1-1", inputTypes, ExprType.LONG);
    assertOutputType("1*1", inputTypes, ExprType.LONG);
    assertOutputType("1/1", inputTypes, ExprType.LONG);
    assertOutputType("1^1", inputTypes, ExprType.LONG);
    assertOutputType("1%1", inputTypes, ExprType.LONG);

    assertOutputType("y+y_", inputTypes, ExprType.LONG);
    assertOutputType("y-y_", inputTypes, ExprType.LONG);
    assertOutputType("y*y_", inputTypes, ExprType.LONG);
    assertOutputType("y/y_", inputTypes, ExprType.LONG);
    assertOutputType("y^y_", inputTypes, ExprType.LONG);
    assertOutputType("y%y_", inputTypes, ExprType.LONG);

    assertOutputType("y+z", inputTypes, ExprType.DOUBLE);
    assertOutputType("y-z", inputTypes, ExprType.DOUBLE);
    assertOutputType("y*z", inputTypes, ExprType.DOUBLE);
    assertOutputType("y/z", inputTypes, ExprType.DOUBLE);
    assertOutputType("y^z", inputTypes, ExprType.DOUBLE);
    assertOutputType("y%z", inputTypes, ExprType.DOUBLE);

    assertOutputType("z+z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z-z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z*z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z/z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z^z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z%z_", inputTypes, ExprType.DOUBLE);

    assertOutputType("y>y_", inputTypes, ExprType.LONG);
    assertOutputType("y_<y", inputTypes, ExprType.LONG);
    assertOutputType("y_<=y", inputTypes, ExprType.LONG);
    assertOutputType("y_>=y", inputTypes, ExprType.LONG);
    assertOutputType("y_==y", inputTypes, ExprType.LONG);
    assertOutputType("y_!=y", inputTypes, ExprType.LONG);
    assertOutputType("y_ && y", inputTypes, ExprType.LONG);
    assertOutputType("y_ || y", inputTypes, ExprType.LONG);

    assertOutputType("z>y_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z<y", inputTypes, ExprType.DOUBLE);
    assertOutputType("z<=y", inputTypes, ExprType.DOUBLE);
    assertOutputType("y>=z", inputTypes, ExprType.DOUBLE);
    assertOutputType("z==y", inputTypes, ExprType.DOUBLE);
    assertOutputType("z!=y", inputTypes, ExprType.DOUBLE);
    assertOutputType("z && y", inputTypes, ExprType.DOUBLE);
    assertOutputType("y || z", inputTypes, ExprType.DOUBLE);

    assertOutputType("z>z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z<z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z<=z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z_>=z", inputTypes, ExprType.DOUBLE);
    assertOutputType("z==z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z!=z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z && z_", inputTypes, ExprType.DOUBLE);
    assertOutputType("z_ || z", inputTypes, ExprType.DOUBLE);

    assertOutputType("1*(2 + 3.0)", inputTypes, ExprType.DOUBLE);
  }

  @Test
  public void testUnivariateMathFunctions()
  {
    assertOutputType("pi()", inputTypes, ExprType.DOUBLE);
    assertOutputType("abs(x)", inputTypes, ExprType.STRING);
    assertOutputType("abs(y)", inputTypes, ExprType.LONG);
    assertOutputType("abs(z)", inputTypes, ExprType.DOUBLE);
    assertOutputType("cos(y)", inputTypes, ExprType.DOUBLE);
    assertOutputType("cos(z)", inputTypes, ExprType.DOUBLE);
  }

  @Test
  public void testBivariateMathFunctions()
  {
    assertOutputType("div(y,y_)", inputTypes, ExprType.LONG);
    assertOutputType("div(y,z_)", inputTypes, ExprType.DOUBLE);
    assertOutputType("div(z,z_)", inputTypes, ExprType.DOUBLE);

    assertOutputType("max(y,y_)", inputTypes, ExprType.LONG);
    assertOutputType("max(y,z_)", inputTypes, ExprType.DOUBLE);
    assertOutputType("max(z,z_)", inputTypes, ExprType.DOUBLE);

    assertOutputType("hypot(y,y_)", inputTypes, ExprType.DOUBLE);
    assertOutputType("hypot(y,z_)", inputTypes, ExprType.DOUBLE);
    assertOutputType("hypot(z,z_)", inputTypes, ExprType.DOUBLE);
  }

  @Test
  public void testConditionalFunctions()
  {
    assertOutputType("if(y, 'foo', 'bar')", inputTypes, ExprType.STRING);
    assertOutputType("if(y,2,3)", inputTypes, ExprType.LONG);
    assertOutputType("if(y,2,3.0)", inputTypes, ExprType.DOUBLE);

    assertOutputType(
        "case_simple(x,'baz','is baz','foo','is foo','is other')",
        inputTypes,
        ExprType.STRING
    );
    assertOutputType(
        "case_simple(y,2,2,3,3,4)",
        inputTypes,
        ExprType.LONG
    );
    assertOutputType(
        "case_simple(z,2.0,2.0,3.0,3.0,4.0)",
        inputTypes,
        ExprType.DOUBLE
    );

    assertOutputType(
        "case_searched(x=='baz','is baz',x=='foo','is foo','is other')",
        inputTypes,
        ExprType.STRING
    );
    assertOutputType(
        "case_searched(y==1,1,y==2,2,0)",
        inputTypes,
        ExprType.LONG
    );
    assertOutputType(
        "case_searched(z==1.0,1.0,z==2.0,2.0,0.0)",
        inputTypes,
        ExprType.DOUBLE
    );

    assertOutputType("nvl(x, 'foo')", inputTypes, ExprType.STRING);
    assertOutputType("nvl(y, 1)", inputTypes, ExprType.LONG);
    assertOutputType("nvl(z, 2.0)", inputTypes, ExprType.DOUBLE);
    assertOutputType("isnull(x)", inputTypes, ExprType.LONG);
    assertOutputType("isnull(y)", inputTypes, ExprType.LONG);
    assertOutputType("isnull(z)", inputTypes, ExprType.LONG);
    assertOutputType("notnull(x)", inputTypes, ExprType.LONG);
    assertOutputType("notnull(y)", inputTypes, ExprType.LONG);
    assertOutputType("notnull(z)", inputTypes, ExprType.LONG);
  }

  @Test
  public void testStringFunctions()
  {
    assertOutputType("concat(x, 'foo')", inputTypes, ExprType.STRING);
    assertOutputType("concat(y, 'foo')", inputTypes, ExprType.STRING);
    assertOutputType("concat(z, 'foo')", inputTypes, ExprType.STRING);

    assertOutputType("strlen(x)", inputTypes, ExprType.LONG);
    assertOutputType("format('%s', x)", inputTypes, ExprType.STRING);
    assertOutputType("format('%s', y)", inputTypes, ExprType.STRING);
    assertOutputType("format('%s', z)", inputTypes, ExprType.STRING);
    assertOutputType("strpos(x, x_)", inputTypes, ExprType.LONG);
    assertOutputType("strpos(x, y)", inputTypes, ExprType.LONG);
    assertOutputType("strpos(x, z)", inputTypes, ExprType.LONG);
    assertOutputType("substring(x, 1, 2)", inputTypes, ExprType.STRING);
    assertOutputType("left(x, 1)", inputTypes, ExprType.STRING);
    assertOutputType("right(x, 1)", inputTypes, ExprType.STRING);
    assertOutputType("replace(x, 'foo', '')", inputTypes, ExprType.STRING);
    assertOutputType("lower(x)", inputTypes, ExprType.STRING);
    assertOutputType("upper(x)", inputTypes, ExprType.STRING);
    assertOutputType("reverse(x)", inputTypes, ExprType.STRING);
    assertOutputType("repeat(x, 4)", inputTypes, ExprType.STRING);
  }

  @Test
  public void testArrayFunctions()
  {
    assertOutputType("array(1, 2, 3)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array(1, 2, 3.0)", inputTypes, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_length(a)", inputTypes, ExprType.LONG);
    assertOutputType("array_length(b)", inputTypes, ExprType.LONG);
    assertOutputType("array_length(c)", inputTypes, ExprType.LONG);

    assertOutputType("string_to_array(x, ',')", inputTypes, ExprType.STRING_ARRAY);

    assertOutputType("array_to_string(a, ',')", inputTypes, ExprType.STRING);
    assertOutputType("array_to_string(b, ',')", inputTypes, ExprType.STRING);
    assertOutputType("array_to_string(c, ',')", inputTypes, ExprType.STRING);

    assertOutputType("array_offset(a, 1)", inputTypes, ExprType.STRING);
    assertOutputType("array_offset(b, 1)", inputTypes, ExprType.LONG);
    assertOutputType("array_offset(c, 1)", inputTypes, ExprType.DOUBLE);

    assertOutputType("array_ordinal(a, 1)", inputTypes, ExprType.STRING);
    assertOutputType("array_ordinal(b, 1)", inputTypes, ExprType.LONG);
    assertOutputType("array_ordinal(c, 1)", inputTypes, ExprType.DOUBLE);

    assertOutputType("array_offset_of(a, 'a')", inputTypes, ExprType.LONG);
    assertOutputType("array_offset_of(b, 1)", inputTypes, ExprType.LONG);
    assertOutputType("array_offset_of(c, 1.0)", inputTypes, ExprType.LONG);

    assertOutputType("array_ordinal_of(a, 'a')", inputTypes, ExprType.LONG);
    assertOutputType("array_ordinal_of(b, 1)", inputTypes, ExprType.LONG);
    assertOutputType("array_ordinal_of(c, 1.0)", inputTypes, ExprType.LONG);

    assertOutputType("array_append(x, x_)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_append(a, x_)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_append(y, y_)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_append(b, y_)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_append(z, z_)", inputTypes, ExprType.DOUBLE_ARRAY);
    assertOutputType("array_append(c, z_)", inputTypes, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_concat(x, a)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_concat(a, a)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_concat(y, b)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_concat(b, b)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_concat(z, c)", inputTypes, ExprType.DOUBLE_ARRAY);
    assertOutputType("array_concat(c, c)", inputTypes, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_contains(a, 'a')", inputTypes, ExprType.LONG);
    assertOutputType("array_contains(b, 1)", inputTypes, ExprType.LONG);
    assertOutputType("array_contains(c, 2.0)", inputTypes, ExprType.LONG);

    assertOutputType("array_overlap(a, a)", inputTypes, ExprType.LONG);
    assertOutputType("array_overlap(b, b)", inputTypes, ExprType.LONG);
    assertOutputType("array_overlap(c, c)", inputTypes, ExprType.LONG);

    assertOutputType("array_slice(a, 1, 2)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_slice(b, 1, 2)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_slice(c, 1, 2)", inputTypes, ExprType.DOUBLE_ARRAY);

    assertOutputType("array_prepend(x, a)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_prepend(x, x_)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("array_prepend(y, b)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_prepend(y, y_)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("array_prepend(z, c)", inputTypes, ExprType.DOUBLE_ARRAY);
    assertOutputType("array_prepend(z, z_)", inputTypes, ExprType.DOUBLE_ARRAY);
  }

  @Test
  public void testReduceFunctions()
  {
    assertOutputType("greatest('B', x, 'A')", inputTypes, ExprType.STRING);
    assertOutputType("greatest(y, 0)", inputTypes, ExprType.LONG);
    assertOutputType("greatest(34.0, z, 5.0, 767.0)", inputTypes, ExprType.DOUBLE);

    assertOutputType("least('B', x, 'A')", inputTypes, ExprType.STRING);
    assertOutputType("least(y, 0)", inputTypes, ExprType.LONG);
    assertOutputType("least(34.0, z, 5.0, 767.0)", inputTypes, ExprType.DOUBLE);
  }

  @Test
  public void testApplyFunctions()
  {
    assertOutputType("map((x) -> concat(x, 'foo'), x)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("map((x) -> x + x, y)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("map((x) -> x + x, z)", inputTypes, ExprType.DOUBLE_ARRAY);
    assertOutputType("map((x) -> concat(x, 'foo'), a)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("map((x) -> x + x, b)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("map((x) -> x + x, c)", inputTypes, ExprType.DOUBLE_ARRAY);
    assertOutputType(
        "cartesian_map((x, y) -> concat(x, y), ['foo', 'bar', 'baz', 'foobar'], ['bar', 'baz'])",
        inputTypes,
        ExprType.STRING_ARRAY
    );
    assertOutputType("fold((x, acc) -> x + acc, y, 0)", inputTypes, ExprType.LONG);
    assertOutputType("fold((x, acc) -> x + acc, y, y)", inputTypes, ExprType.LONG);
    assertOutputType("fold((x, acc) -> x + acc, y, 1.0)", inputTypes, ExprType.DOUBLE);
    assertOutputType("fold((x, acc) -> x + acc, y, z)", inputTypes, ExprType.DOUBLE);

    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, 0)", inputTypes, ExprType.LONG);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, y)", inputTypes, ExprType.LONG);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, 1.0)", inputTypes, ExprType.DOUBLE);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, z)", inputTypes, ExprType.DOUBLE);

    assertOutputType("filter((x) -> x == 'foo', a)", inputTypes, ExprType.STRING_ARRAY);
    assertOutputType("filter((x) -> x > 1, b)", inputTypes, ExprType.LONG_ARRAY);
    assertOutputType("filter((x) -> x > 1, c)", inputTypes, ExprType.DOUBLE_ARRAY);

    assertOutputType("any((x) -> x == 'foo', a)", inputTypes, ExprType.LONG);
    assertOutputType("any((x) -> x > 1, b)", inputTypes, ExprType.LONG);
    assertOutputType("any((x) -> x > 1.2, c)", inputTypes, ExprType.LONG);

    assertOutputType("all((x) -> x == 'foo', a)", inputTypes, ExprType.LONG);
    assertOutputType("all((x) -> x > 1, b)", inputTypes, ExprType.LONG);
    assertOutputType("all((x) -> x > 1.2, c)", inputTypes, ExprType.LONG);
  }

  @Test
  public void testAutoConversion()
  {
    // nulls output nulls
    Assert.assertNull(ExprType.autoTypeConversion(ExprType.LONG, null));
    Assert.assertNull(ExprType.autoTypeConversion(null, ExprType.LONG));
    Assert.assertNull(ExprType.autoTypeConversion(ExprType.DOUBLE, null));
    Assert.assertNull(ExprType.autoTypeConversion(null, ExprType.DOUBLE));
    Assert.assertNull(ExprType.autoTypeConversion(ExprType.STRING, null));
    Assert.assertNull(ExprType.autoTypeConversion(null, ExprType.STRING));
    // only long stays long
    Assert.assertEquals(ExprType.LONG, ExprType.autoTypeConversion(ExprType.LONG, ExprType.LONG));
    // any double makes all doubles
    Assert.assertEquals(ExprType.DOUBLE, ExprType.autoTypeConversion(ExprType.LONG, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.DOUBLE, ExprType.autoTypeConversion(ExprType.DOUBLE, ExprType.LONG));
    Assert.assertEquals(ExprType.DOUBLE, ExprType.autoTypeConversion(ExprType.DOUBLE, ExprType.DOUBLE));
    // any string makes become string
    Assert.assertEquals(ExprType.STRING, ExprType.autoTypeConversion(ExprType.LONG, ExprType.STRING));
    Assert.assertEquals(ExprType.STRING, ExprType.autoTypeConversion(ExprType.STRING, ExprType.LONG));
    Assert.assertEquals(ExprType.STRING, ExprType.autoTypeConversion(ExprType.DOUBLE, ExprType.STRING));
    Assert.assertEquals(ExprType.STRING, ExprType.autoTypeConversion(ExprType.STRING, ExprType.DOUBLE));
    Assert.assertEquals(ExprType.STRING, ExprType.autoTypeConversion(ExprType.STRING, ExprType.STRING));
    // unless it is an array, and those have to be the same
    Assert.assertEquals(ExprType.LONG_ARRAY, ExprType.autoTypeConversion(ExprType.LONG_ARRAY, ExprType.LONG_ARRAY));
    Assert.assertEquals(
        ExprType.DOUBLE_ARRAY,
        ExprType.autoTypeConversion(ExprType.DOUBLE_ARRAY, ExprType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExprType.STRING_ARRAY,
        ExprType.autoTypeConversion(ExprType.STRING_ARRAY, ExprType.STRING_ARRAY)
    );
  }

  @Test
  public void testAutoConversionArrayMismatchArrays()
  {
    expectedException.expect(IAE.class);
    ExprType.autoTypeConversion(ExprType.DOUBLE_ARRAY, ExprType.LONG_ARRAY);
  }

  @Test
  public void testAutoConversionArrayMismatchArrayScalar()
  {
    expectedException.expect(IAE.class);
    ExprType.autoTypeConversion(ExprType.DOUBLE_ARRAY, ExprType.LONG);
  }

  @Test
  public void testAutoConversionArrayMismatchScalarArray()
  {
    expectedException.expect(IAE.class);
    ExprType.autoTypeConversion(ExprType.STRING, ExprType.LONG_ARRAY);
  }

  private void assertOutputType(String expression, Expr.InputBindingTypes inputTypes, ExprType outputType)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil(), false);
    Assert.assertEquals(outputType, expr.getOutputType(inputTypes));
  }

  Expr.InputBindingTypes inputTypesFromMap(Map<String, ExprType> types)
  {
    return types::get;
  }
}
