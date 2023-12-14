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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class OutputTypeTest extends InitializedNullHandlingTest
{
  private final Expr.InputBindingInspector inspector = inspectorFromMap(
      ImmutableMap.<String, ExpressionType>builder().put("x", ExpressionType.STRING)
                  .put("x_", ExpressionType.STRING)
                  .put("y", ExpressionType.LONG)
                  .put("y_", ExpressionType.LONG)
                  .put("z", ExpressionType.DOUBLE)
                  .put("z_", ExpressionType.DOUBLE)
                  .put("a", ExpressionType.STRING_ARRAY)
                  .put("a_", ExpressionType.STRING_ARRAY)
                  .put("b", ExpressionType.LONG_ARRAY)
                  .put("b_", ExpressionType.LONG_ARRAY)
                  .put("c", ExpressionType.DOUBLE_ARRAY)
                  .put("c_", ExpressionType.DOUBLE_ARRAY)
                  .build()
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConstantsAndIdentifiers()
  {
    assertOutputType("'hello'", inspector, ExpressionType.STRING);
    assertOutputType("23", inspector, ExpressionType.LONG);
    assertOutputType("3.2", inspector, ExpressionType.DOUBLE);
    assertOutputType("['a', 'b']", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("[1,2,3]", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("[1.0]", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType("x", inspector, ExpressionType.STRING);
    assertOutputType("y", inspector, ExpressionType.LONG);
    assertOutputType("z", inspector, ExpressionType.DOUBLE);
    assertOutputType("a", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("b", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("c", inspector, ExpressionType.DOUBLE_ARRAY);
  }

  @Test
  public void testUnaryOperators()
  {
    assertOutputType("-1", inspector, ExpressionType.LONG);
    assertOutputType("-1.1", inspector, ExpressionType.DOUBLE);
    assertOutputType("-y", inspector, ExpressionType.LONG);
    assertOutputType("-z", inspector, ExpressionType.DOUBLE);

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      assertOutputType("!'true'", inspector, ExpressionType.LONG);
      assertOutputType("!1", inspector, ExpressionType.LONG);
      assertOutputType("!x", inspector, ExpressionType.LONG);
      assertOutputType("!y", inspector, ExpressionType.LONG);
      assertOutputType("!1.1", inspector, ExpressionType.LONG);
      assertOutputType("!z", inspector, ExpressionType.LONG);
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      assertOutputType("!1.1", inspector, ExpressionType.DOUBLE);
      assertOutputType("!z", inspector, ExpressionType.DOUBLE);
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testBinaryMathOperators()
  {
    assertOutputType("1+1", inspector, ExpressionType.LONG);
    assertOutputType("1-1", inspector, ExpressionType.LONG);
    assertOutputType("1*1", inspector, ExpressionType.LONG);
    assertOutputType("1/1", inspector, ExpressionType.LONG);
    assertOutputType("1^1", inspector, ExpressionType.LONG);
    assertOutputType("1%1", inspector, ExpressionType.LONG);

    assertOutputType("y+y_", inspector, ExpressionType.LONG);
    assertOutputType("y-y_", inspector, ExpressionType.LONG);
    assertOutputType("y*y_", inspector, ExpressionType.LONG);
    assertOutputType("y/y_", inspector, ExpressionType.LONG);
    assertOutputType("y^y_", inspector, ExpressionType.LONG);
    assertOutputType("y%y_", inspector, ExpressionType.LONG);

    assertOutputType("y+z", inspector, ExpressionType.DOUBLE);
    assertOutputType("y-z", inspector, ExpressionType.DOUBLE);
    assertOutputType("y*z", inspector, ExpressionType.DOUBLE);
    assertOutputType("y/z", inspector, ExpressionType.DOUBLE);
    assertOutputType("y^z", inspector, ExpressionType.DOUBLE);
    assertOutputType("y%z", inspector, ExpressionType.DOUBLE);

    assertOutputType("z+z_", inspector, ExpressionType.DOUBLE);
    assertOutputType("z-z_", inspector, ExpressionType.DOUBLE);
    assertOutputType("z*z_", inspector, ExpressionType.DOUBLE);
    assertOutputType("z/z_", inspector, ExpressionType.DOUBLE);
    assertOutputType("z^z_", inspector, ExpressionType.DOUBLE);
    assertOutputType("z%z_", inspector, ExpressionType.DOUBLE);

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      assertOutputType("y>y_", inspector, ExpressionType.LONG);
      assertOutputType("y_<y", inspector, ExpressionType.LONG);
      assertOutputType("y_<=y", inspector, ExpressionType.LONG);
      assertOutputType("y_>=y", inspector, ExpressionType.LONG);
      assertOutputType("y_==y", inspector, ExpressionType.LONG);
      assertOutputType("y_!=y", inspector, ExpressionType.LONG);
      assertOutputType("y_ && y", inspector, ExpressionType.LONG);
      assertOutputType("y_ || y", inspector, ExpressionType.LONG);

      assertOutputType("z>y_", inspector, ExpressionType.LONG);
      assertOutputType("z<y", inspector, ExpressionType.LONG);
      assertOutputType("z<=y", inspector, ExpressionType.LONG);
      assertOutputType("y>=z", inspector, ExpressionType.LONG);
      assertOutputType("z==y", inspector, ExpressionType.LONG);
      assertOutputType("z!=y", inspector, ExpressionType.LONG);
      assertOutputType("z && y", inspector, ExpressionType.LONG);
      assertOutputType("y || z", inspector, ExpressionType.LONG);

      assertOutputType("z>z_", inspector, ExpressionType.LONG);
      assertOutputType("z<z_", inspector, ExpressionType.LONG);
      assertOutputType("z<=z_", inspector, ExpressionType.LONG);
      assertOutputType("z_>=z", inspector, ExpressionType.LONG);
      assertOutputType("z==z_", inspector, ExpressionType.LONG);
      assertOutputType("z!=z_", inspector, ExpressionType.LONG);
      assertOutputType("z && z_", inspector, ExpressionType.LONG);
      assertOutputType("z_ || z", inspector, ExpressionType.LONG);
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      assertOutputType("z>y_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z<y", inspector, ExpressionType.DOUBLE);
      assertOutputType("z<=y", inspector, ExpressionType.DOUBLE);
      assertOutputType("y>=z", inspector, ExpressionType.DOUBLE);
      assertOutputType("z==y", inspector, ExpressionType.DOUBLE);
      assertOutputType("z!=y", inspector, ExpressionType.DOUBLE);
      assertOutputType("z && y", inspector, ExpressionType.DOUBLE);
      assertOutputType("y || z", inspector, ExpressionType.DOUBLE);

      assertOutputType("z>z_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z<z_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z<=z_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z_>=z", inspector, ExpressionType.DOUBLE);
      assertOutputType("z==z_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z!=z_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z && z_", inspector, ExpressionType.DOUBLE);
      assertOutputType("z_ || z", inspector, ExpressionType.DOUBLE);
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
    assertOutputType("1*(2 + 3.0)", inspector, ExpressionType.DOUBLE);
  }

  @Test
  public void testUnivariateMathFunctions()
  {
    assertOutputType("pi()", inspector, ExpressionType.DOUBLE);
    assertOutputType("abs(x)", inspector, ExpressionType.STRING);
    assertOutputType("abs(y)", inspector, ExpressionType.LONG);
    assertOutputType("abs(z)", inspector, ExpressionType.DOUBLE);
    assertOutputType("cos(y)", inspector, ExpressionType.DOUBLE);
    assertOutputType("cos(z)", inspector, ExpressionType.DOUBLE);
  }

  @Test
  public void testBivariateMathFunctions()
  {
    assertOutputType("div(y,y_)", inspector, ExpressionType.LONG);
    assertOutputType("div(y,z_)", inspector, ExpressionType.LONG);
    assertOutputType("div(z,z_)", inspector, ExpressionType.LONG);

    assertOutputType("max(y,y_)", inspector, ExpressionType.LONG);
    assertOutputType("max(y,z_)", inspector, ExpressionType.DOUBLE);
    assertOutputType("max(z,z_)", inspector, ExpressionType.DOUBLE);

    assertOutputType("hypot(y,y_)", inspector, ExpressionType.DOUBLE);
    assertOutputType("hypot(y,z_)", inspector, ExpressionType.DOUBLE);
    assertOutputType("hypot(z,z_)", inspector, ExpressionType.DOUBLE);

    assertOutputType("safe_divide(y,y_)", inspector, ExpressionType.LONG);
    assertOutputType("safe_divide(y,z_)", inspector, ExpressionType.DOUBLE);
    assertOutputType("safe_divide(z,z_)", inspector, ExpressionType.DOUBLE);

  }

  @Test
  public void testConditionalFunctions()
  {
    assertOutputType("if(y, 'foo', 'bar')", inspector, ExpressionType.STRING);
    assertOutputType("if(y,2,3)", inspector, ExpressionType.LONG);
    assertOutputType("if(y,2,3.0)", inspector, ExpressionType.DOUBLE);

    assertOutputType(
        "case_simple(x,'baz','is baz','foo','is foo','is other')",
        inspector,
        ExpressionType.STRING
    );
    assertOutputType(
        "case_simple(y,2,2,3,3,4)",
        inspector,
        ExpressionType.LONG
    );
    assertOutputType(
        "case_simple(z,2.0,2.0,3.0,3.0,4.0)",
        inspector,
        ExpressionType.DOUBLE
    );

    assertOutputType(
        "case_simple(y,2,2,3,3.0,4)",
        inspector,
        ExpressionType.DOUBLE
    );
    assertOutputType(
        "case_simple(z,2.0,2.0,3.0,3.0,null)",
        inspector,
        ExpressionType.DOUBLE
    );

    assertOutputType(
        "case_searched(x=='baz','is baz',x=='foo','is foo','is other')",
        inspector,
        ExpressionType.STRING
    );
    assertOutputType(
        "case_searched(y==1,1,y==2,2,0)",
        inspector,
        ExpressionType.LONG
    );
    assertOutputType(
        "case_searched(z==1.0,1.0,z==2.0,2.0,0.0)",
        inspector,
        ExpressionType.DOUBLE
    );
    assertOutputType(
        "case_searched(y==1,1,y==2,2.0,0)",
        inspector,
        ExpressionType.DOUBLE
    );
    assertOutputType(
        "case_searched(z==1.0,1,z==2.0,2,null)",
        inspector,
        ExpressionType.LONG
    );
    assertOutputType(
        "case_searched(z==1.0,1.0,z==2.0,2.0,null)",
        inspector,
        ExpressionType.DOUBLE
    );

    assertOutputType("nvl(x, 'foo')", inspector, ExpressionType.STRING);
    assertOutputType("nvl(y, 1)", inspector, ExpressionType.LONG);
    assertOutputType("nvl(y, 1.1)", inspector, ExpressionType.DOUBLE);
    assertOutputType("nvl(z, 2.0)", inspector, ExpressionType.DOUBLE);
    assertOutputType("nvl(y, 2.0)", inspector, ExpressionType.DOUBLE);
    assertOutputType("isnull(x)", inspector, ExpressionType.LONG);
    assertOutputType("isnull(y)", inspector, ExpressionType.LONG);
    assertOutputType("isnull(z)", inspector, ExpressionType.LONG);
    assertOutputType("notnull(x)", inspector, ExpressionType.LONG);
    assertOutputType("notnull(y)", inspector, ExpressionType.LONG);
    assertOutputType("notnull(z)", inspector, ExpressionType.LONG);
  }

  @Test
  public void testStringFunctions()
  {
    assertOutputType("concat(x, 'foo')", inspector, ExpressionType.STRING);
    assertOutputType("concat(y, 'foo')", inspector, ExpressionType.STRING);
    assertOutputType("concat(z, 'foo')", inspector, ExpressionType.STRING);

    assertOutputType("strlen(x)", inspector, ExpressionType.LONG);
    assertOutputType("format('%s', x)", inspector, ExpressionType.STRING);
    assertOutputType("format('%s', y)", inspector, ExpressionType.STRING);
    assertOutputType("format('%s', z)", inspector, ExpressionType.STRING);
    assertOutputType("strpos(x, x_)", inspector, ExpressionType.LONG);
    assertOutputType("strpos(x, y)", inspector, ExpressionType.LONG);
    assertOutputType("strpos(x, z)", inspector, ExpressionType.LONG);
    assertOutputType("substring(x, 1, 2)", inspector, ExpressionType.STRING);
    assertOutputType("left(x, 1)", inspector, ExpressionType.STRING);
    assertOutputType("right(x, 1)", inspector, ExpressionType.STRING);
    assertOutputType("replace(x, 'foo', '')", inspector, ExpressionType.STRING);
    assertOutputType("lower(x)", inspector, ExpressionType.STRING);
    assertOutputType("upper(x)", inspector, ExpressionType.STRING);
    assertOutputType("reverse(x)", inspector, ExpressionType.STRING);
    assertOutputType("repeat(x, 4)", inspector, ExpressionType.STRING);
  }

  @Test
  public void testArrayFunctions()
  {
    assertOutputType("array(1, 2, 3)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array(1, 2, 3.0)", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType(
        "array(a, b)",
        inspector,
        ExpressionTypeFactory.getInstance().ofArray(ExpressionType.STRING_ARRAY)
    );

    assertOutputType("array_length(a)", inspector, ExpressionType.LONG);
    assertOutputType("array_length(b)", inspector, ExpressionType.LONG);
    assertOutputType("array_length(c)", inspector, ExpressionType.LONG);

    assertOutputType("string_to_array(x, ',')", inspector, ExpressionType.STRING_ARRAY);

    assertOutputType("array_to_string(a, ',')", inspector, ExpressionType.STRING);
    assertOutputType("array_to_string(b, ',')", inspector, ExpressionType.STRING);
    assertOutputType("array_to_string(c, ',')", inspector, ExpressionType.STRING);

    assertOutputType("array_offset(a, 1)", inspector, ExpressionType.STRING);
    assertOutputType("array_offset(b, 1)", inspector, ExpressionType.LONG);
    assertOutputType("array_offset(c, 1)", inspector, ExpressionType.DOUBLE);

    assertOutputType("array_ordinal(a, 1)", inspector, ExpressionType.STRING);
    assertOutputType("array_ordinal(b, 1)", inspector, ExpressionType.LONG);
    assertOutputType("array_ordinal(c, 1)", inspector, ExpressionType.DOUBLE);

    assertOutputType("array_offset_of(a, 'a')", inspector, ExpressionType.LONG);
    assertOutputType("array_offset_of(b, 1)", inspector, ExpressionType.LONG);
    assertOutputType("array_offset_of(c, 1.0)", inspector, ExpressionType.LONG);

    assertOutputType("array_ordinal_of(a, 'a')", inspector, ExpressionType.LONG);
    assertOutputType("array_ordinal_of(b, 1)", inspector, ExpressionType.LONG);
    assertOutputType("array_ordinal_of(c, 1.0)", inspector, ExpressionType.LONG);

    assertOutputType("array_append(x, x_)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_append(a, x_)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_append(y, y_)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_append(b, y_)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_append(z, z_)", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType("array_append(c, z_)", inspector, ExpressionType.DOUBLE_ARRAY);

    assertOutputType("array_concat(x, a)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_concat(a, a)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_concat(y, b)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_concat(b, b)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_concat(z, c)", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType("array_concat(c, c)", inspector, ExpressionType.DOUBLE_ARRAY);

    assertOutputType("array_contains(a, 'a')", inspector, ExpressionType.LONG);
    assertOutputType("array_contains(b, 1)", inspector, ExpressionType.LONG);
    assertOutputType("array_contains(c, 2.0)", inspector, ExpressionType.LONG);

    assertOutputType("array_overlap(a, a)", inspector, ExpressionType.LONG);
    assertOutputType("array_overlap(b, b)", inspector, ExpressionType.LONG);
    assertOutputType("array_overlap(c, c)", inspector, ExpressionType.LONG);

    assertOutputType("array_slice(a, 1, 2)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_slice(b, 1, 2)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_slice(c, 1, 2)", inspector, ExpressionType.DOUBLE_ARRAY);

    assertOutputType("array_prepend(x, a)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_prepend(x, x_)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("array_prepend(y, b)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_prepend(y, y_)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("array_prepend(z, c)", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType("array_prepend(z, z_)", inspector, ExpressionType.DOUBLE_ARRAY);
  }

  @Test
  public void testReduceFunctions()
  {
    assertOutputType("greatest('B', x, 'A')", inspector, ExpressionType.STRING);
    assertOutputType("greatest(y, 0)", inspector, ExpressionType.LONG);
    assertOutputType("greatest(34.0, z, 5.0, 767.0)", inspector, ExpressionType.DOUBLE);

    assertOutputType("least('B', x, 'A')", inspector, ExpressionType.STRING);
    assertOutputType("least(y, 0)", inspector, ExpressionType.LONG);
    assertOutputType("least(34.0, z, 5.0, 767.0)", inspector, ExpressionType.DOUBLE);
  }

  @Test
  public void testApplyFunctions()
  {
    assertOutputType("map((x) -> concat(x, 'foo'), x)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("map((x) -> x + x, y)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("map((x) -> x + x, z)", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType("map((x) -> concat(x, 'foo'), a)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("map((x) -> x + x, b)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("map((x) -> x + x, c)", inspector, ExpressionType.DOUBLE_ARRAY);
    assertOutputType(
        "cartesian_map((x, y) -> concat(x, y), ['foo', 'bar', 'baz', 'foobar'], ['bar', 'baz'])",
        inspector,
        ExpressionType.STRING_ARRAY
    );
    assertOutputType("fold((x, acc) -> x + acc, y, 0)", inspector, ExpressionType.LONG);
    assertOutputType("fold((x, acc) -> x + acc, y, y)", inspector, ExpressionType.LONG);
    assertOutputType("fold((x, acc) -> x + acc, y, 1.0)", inspector, ExpressionType.DOUBLE);
    assertOutputType("fold((x, acc) -> x + acc, y, z)", inspector, ExpressionType.DOUBLE);

    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, 0)", inspector, ExpressionType.LONG);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, y)", inspector, ExpressionType.LONG);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, 1.0)", inspector, ExpressionType.DOUBLE);
    assertOutputType("cartesian_fold((x, y, acc) -> x + y + acc, y, z, z)", inspector, ExpressionType.DOUBLE);

    assertOutputType("filter((x) -> x == 'foo', a)", inspector, ExpressionType.STRING_ARRAY);
    assertOutputType("filter((x) -> x > 1, b)", inspector, ExpressionType.LONG_ARRAY);
    assertOutputType("filter((x) -> x > 1, c)", inspector, ExpressionType.DOUBLE_ARRAY);

    assertOutputType("any((x) -> x == 'foo', a)", inspector, ExpressionType.LONG);
    assertOutputType("any((x) -> x > 1, b)", inspector, ExpressionType.LONG);
    assertOutputType("any((x) -> x > 1.2, c)", inspector, ExpressionType.LONG);

    assertOutputType("all((x) -> x == 'foo', a)", inspector, ExpressionType.LONG);
    assertOutputType("all((x) -> x > 1, b)", inspector, ExpressionType.LONG);
    assertOutputType("all((x) -> x > 1.2, c)", inspector, ExpressionType.LONG);
  }


  @Test
  public void testEvalAutoConversion()
  {
    final ExprEval<?> nullStringEval = ExprEval.of(null);
    final ExprEval<?> stringEval = ExprEval.of("wat");
    final ExprEval<?> longEval = ExprEval.of(1L);
    final ExprEval<?> doubleEval = ExprEval.of(1.0);
    final ExprEval<?> arrayEval = ExprEval.ofLongArray(new Long[]{1L, 2L, 3L});
    final ExprEval<?> complexEval = ExprEval.ofComplex(ExpressionType.UNKNOWN_COMPLEX, new Object());
    final ExprEval<?> complexEval2 = ExprEval.ofComplex(new ExpressionType(ExprType.COMPLEX, null, null), new Object());

    // only long stays long
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.autoDetect(longEval, longEval));
    // only string stays string
    Assert.assertEquals(ExpressionType.STRING, ExpressionTypeConversion.autoDetect(nullStringEval, nullStringEval));
    Assert.assertEquals(ExpressionType.STRING, ExpressionTypeConversion.autoDetect(stringEval, stringEval));
    // if only 1 argument is a string, preserve the other type
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.autoDetect(nullStringEval, longEval));
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.autoDetect(longEval, nullStringEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(doubleEval, nullStringEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(nullStringEval, doubleEval));
    // for operators, doubles is the catch all
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(longEval, doubleEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(doubleEval, longEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(doubleEval, doubleEval));
    // ... even when non-null strings are used with non-double types
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(longEval, stringEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(doubleEval, stringEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(stringEval, doubleEval));
    // arrays are not a good idea to use with this method
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(arrayEval, nullStringEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(arrayEval, doubleEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(arrayEval, longEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(nullStringEval, arrayEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(doubleEval, arrayEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(longEval, arrayEval));

    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(longEval, complexEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(doubleEval, complexEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(arrayEval, complexEval));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.autoDetect(complexEval, complexEval));
    Assert.assertEquals(
        ExpressionTypeConversion.autoDetect(complexEval, complexEval),
        ExpressionTypeConversion.autoDetect(complexEval2, complexEval)
    );
  }

  @Test
  public void testOperatorAutoConversion()
  {
    // nulls output other
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.operator(ExpressionType.LONG, null));
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.operator(null, ExpressionType.LONG));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.operator(ExpressionType.DOUBLE, null));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.operator(null, ExpressionType.DOUBLE));
    Assert.assertEquals(ExpressionType.STRING, ExpressionTypeConversion.operator(ExpressionType.STRING, null));
    Assert.assertEquals(ExpressionType.STRING, ExpressionTypeConversion.operator(null, ExpressionType.STRING));
    // only long stays long
    Assert.assertEquals(
        ExpressionType.LONG,
        ExpressionTypeConversion.operator(ExpressionType.LONG, ExpressionType.LONG)
    );
    // only string stays string
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.operator(ExpressionType.STRING, ExpressionType.STRING)
    );
    // for operators, doubles is the catch all
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.LONG, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.DOUBLE, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.DOUBLE, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.DOUBLE, ExpressionType.STRING)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.STRING, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.STRING, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.operator(ExpressionType.LONG, ExpressionType.STRING)
    );
    // unless it is an array, and those have to be the same
    Assert.assertEquals(
        ExpressionType.LONG_ARRAY,
        ExpressionTypeConversion.operator(ExpressionType.LONG_ARRAY, ExpressionType.LONG_ARRAY)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE_ARRAY,
        ExpressionTypeConversion.operator(ExpressionType.DOUBLE_ARRAY, ExpressionType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExpressionType.STRING_ARRAY,
        ExpressionTypeConversion.operator(ExpressionType.STRING_ARRAY, ExpressionType.STRING_ARRAY)
    );

    ExpressionType nested = ExpressionType.fromColumnType(ColumnType.NESTED_DATA);
    Assert.assertEquals(
        nested,
        ExpressionTypeConversion.operator(nested, nested)
    );
    Assert.assertEquals(
        nested,
        ExpressionTypeConversion.operator(nested, ExpressionType.UNKNOWN_COMPLEX)
    );
    Assert.assertEquals(
        nested,
        ExpressionTypeConversion.operator(ExpressionType.UNKNOWN_COMPLEX, nested)
    );
  }

  @Test
  public void testFunctionAutoConversion()
  {
    // nulls output other
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.function(ExpressionType.LONG, null));
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.function(null, ExpressionType.LONG));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.function(ExpressionType.DOUBLE, null));
    Assert.assertEquals(ExpressionType.DOUBLE, ExpressionTypeConversion.function(null, ExpressionType.DOUBLE));
    Assert.assertEquals(ExpressionType.STRING, ExpressionTypeConversion.function(ExpressionType.STRING, null));
    Assert.assertEquals(ExpressionType.STRING, ExpressionTypeConversion.function(null, ExpressionType.STRING));
    // only long stays long
    Assert.assertEquals(
        ExpressionType.LONG,
        ExpressionTypeConversion.function(ExpressionType.LONG, ExpressionType.LONG)
    );
    // any double makes all doubles
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.function(ExpressionType.LONG, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.function(ExpressionType.DOUBLE, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE,
        ExpressionTypeConversion.function(ExpressionType.DOUBLE, ExpressionType.DOUBLE)
    );
    // any string makes become string
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.function(ExpressionType.LONG, ExpressionType.STRING)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.function(ExpressionType.STRING, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.function(ExpressionType.DOUBLE, ExpressionType.STRING)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.function(ExpressionType.STRING, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.function(ExpressionType.STRING, ExpressionType.STRING)
    );
    // unless it is an array, and those have to be the same
    Assert.assertEquals(
        ExpressionType.LONG_ARRAY,
        ExpressionTypeConversion.function(ExpressionType.LONG_ARRAY, ExpressionType.LONG_ARRAY)
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE_ARRAY,
        ExpressionTypeConversion.function(ExpressionType.DOUBLE_ARRAY, ExpressionType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExpressionType.STRING_ARRAY,
        ExpressionTypeConversion.function(ExpressionType.STRING_ARRAY, ExpressionType.STRING_ARRAY)
    );
    ExpressionType nested = ExpressionType.fromColumnType(ColumnType.NESTED_DATA);
    Assert.assertEquals(
        nested,
        ExpressionTypeConversion.function(nested, nested)
    );
    Assert.assertEquals(
        nested,
        ExpressionTypeConversion.function(nested, ExpressionType.UNKNOWN_COMPLEX)
    );
    Assert.assertEquals(
        nested,
        ExpressionTypeConversion.function(ExpressionType.UNKNOWN_COMPLEX, nested)
    );
  }

  @Test
  public void testIntegerFunctionAutoConversion()
  {
    // nulls output other
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.integerMathFunction(ExpressionType.LONG, null));
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.integerMathFunction(null, ExpressionType.LONG));
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.integerMathFunction(ExpressionType.DOUBLE, null));
    Assert.assertEquals(ExpressionType.LONG, ExpressionTypeConversion.integerMathFunction(null, ExpressionType.DOUBLE));
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.STRING, null)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(null, ExpressionType.STRING)
    );
    // all numbers are longs
    Assert.assertEquals(
        ExpressionType.LONG,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.LONG, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.LONG,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.LONG, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.LONG,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.DOUBLE, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.LONG,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.DOUBLE, ExpressionType.DOUBLE)
    );
    // any string makes become string
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.LONG, ExpressionType.STRING)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.STRING, ExpressionType.LONG)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.DOUBLE, ExpressionType.STRING)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.STRING, ExpressionType.DOUBLE)
    );
    Assert.assertEquals(
        ExpressionType.STRING,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.STRING, ExpressionType.STRING)
    );
    // unless it is an array
    Assert.assertEquals(
        ExpressionType.LONG_ARRAY,
        ExpressionTypeConversion.integerMathFunction(
            ExpressionType.LONG_ARRAY,
            ExpressionType.LONG_ARRAY
        )
    );
    Assert.assertEquals(
        ExpressionType.DOUBLE_ARRAY,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.DOUBLE_ARRAY, ExpressionType.DOUBLE_ARRAY)
    );
    Assert.assertEquals(
        ExpressionType.STRING_ARRAY,
        ExpressionTypeConversion.integerMathFunction(ExpressionType.STRING_ARRAY, ExpressionType.STRING_ARRAY)
    );
  }

  @Test
  public void testAutoConversionArrayMismatchArrays()
  {
    expectedException.expect(IAE.class);
    ExpressionTypeConversion.function(ExpressionType.DOUBLE_ARRAY, ExpressionType.LONG_ARRAY);
  }

  @Test
  public void testAutoConversionArrayMismatchArrayScalar()
  {
    expectedException.expect(IAE.class);
    ExpressionTypeConversion.function(ExpressionType.DOUBLE_ARRAY, ExpressionType.LONG);
  }

  @Test
  public void testAutoConversionArrayMismatchScalarArray()
  {
    expectedException.expect(IAE.class);
    ExpressionTypeConversion.function(ExpressionType.DOUBLE, ExpressionType.LONG_ARRAY);
  }

  private void assertOutputType(String expression, Expr.InputBindingInspector inspector, ExpressionType outputType)
  {
    final Expr expr = Parser.parse(expression, ExprMacroTable.nil(), false);
    Assert.assertEquals(outputType, expr.getOutputType(inspector));
  }

  Expr.InputBindingInspector inspectorFromMap(Map<String, ExpressionType> types)
  {
    return key -> types.get(key);
  }
}
