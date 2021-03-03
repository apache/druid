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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ParserTest extends InitializedNullHandlingTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  VectorExprSanityTest.SettableVectorInputBinding emptyBinding = new VectorExprSanityTest.SettableVectorInputBinding(8);

  @Test
  public void testSimple()
  {
    String actual = Parser.parse("1", ExprMacroTable.nil()).toString();
    String expected = "1";
    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testParseConstants()
  {
    validateLiteral("null", ExprType.STRING, null);
    validateLiteral("'hello'", ExprType.STRING, "hello");
    validateLiteral("'hello \\uD83E\\uDD18'", ExprType.STRING, "hello \uD83E\uDD18");
    validateLiteral("1", ExprType.LONG, 1L);
    validateLiteral("1.", ExprType.DOUBLE, 1.0, false);
    validateLiteral("1.234", ExprType.DOUBLE, 1.234);
    validateLiteral("1e10", ExprType.DOUBLE, 1.0E10, false);
    validateLiteral("1e-10", ExprType.DOUBLE, 1.0E-10, false);
    validateLiteral("1E10", ExprType.DOUBLE, 1.0E10, false);
    validateLiteral("1E-10", ExprType.DOUBLE, 1.0E-10, false);
    validateLiteral("1.E10", ExprType.DOUBLE, 1.0E10, false);
    validateLiteral("1.E-10", ExprType.DOUBLE, 1.0E-10, false);
    validateLiteral("1.e10", ExprType.DOUBLE, 1.0E10, false);
    validateLiteral("1.e-10", ExprType.DOUBLE, 1.0E-10, false);
    validateLiteral("1.1e10", ExprType.DOUBLE, 1.1E10, false);
    validateLiteral("1.1e-10", ExprType.DOUBLE, 1.1E-10, false);
    validateLiteral("1.1E10", ExprType.DOUBLE, 1.1E10);
    validateLiteral("1.1E-10", ExprType.DOUBLE, 1.1E-10);
    validateLiteral("Infinity", ExprType.DOUBLE, Double.POSITIVE_INFINITY);
    validateLiteral("NaN", ExprType.DOUBLE, Double.NaN);
  }

  @Test
  public void testSimpleUnaryOps1()
  {
    String actual = Parser.parse("-x", ExprMacroTable.nil()).toString();
    String expected = "-x";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("!x", ExprMacroTable.nil()).toString();
    expected = "!x";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleUnaryOps2()
  {
    validateFlatten("-1", "-1", "-1");
    validateFlatten("--1", "--1", "1");
    validateFlatten("-1+2", "(+ -1 2)", "1");
    validateFlatten("-1*2", "(* -1 2)", "-2");
    validateFlatten("-1^2", "(^ -1 2)", "1");
  }

  @Test
  public void testSimpleLogicalOps1()
  {
    validateParser("x>y", "(> x y)", ImmutableList.of("x", "y"));
    validateParser("x<y", "(< x y)", ImmutableList.of("x", "y"));
    validateParser("x<=y", "(<= x y)", ImmutableList.of("x", "y"));
    validateParser("x>=y", "(>= x y)", ImmutableList.of("x", "y"));
    validateParser("x==y", "(== x y)", ImmutableList.of("x", "y"));
    validateParser("x!=y", "(!= x y)", ImmutableList.of("x", "y"));
    validateParser("x && y", "(&& x y)", ImmutableList.of("x", "y"));
    validateParser("x || y", "(|| x y)", ImmutableList.of("x", "y"));

  }

  @Test
  public void testSimpleAdditivityOp1()
  {
    validateParser("x+y", "(+ x y)", ImmutableList.of("x", "y"));
    validateParser("x-y", "(- x y)", ImmutableList.of("x", "y"));
  }

  @Test
  public void testSimpleAdditivityOp2()
  {
    validateParser("x+y+z", "(+ (+ x y) z)", ImmutableList.of("x", "y", "z"));
    validateParser("x+y-z", "(- (+ x y) z)", ImmutableList.of("x", "y", "z"));
    validateParser("x-y+z", "(+ (- x y) z)", ImmutableList.of("x", "y", "z"));
    validateParser("x-y-z", "(- (- x y) z)", ImmutableList.of("x", "y", "z"));

    validateParser("x-y-x", "(- (- x y) x)", ImmutableList.of("x", "y"), ImmutableSet.of("x", "x_0", "y"));
  }

  @Test
  public void testSimpleMultiplicativeOp1()
  {
    validateParser("x*y", "(* x y)", ImmutableList.of("x", "y"));
    validateParser("x/y", "(/ x y)", ImmutableList.of("x", "y"));
    validateParser("x%y", "(% x y)", ImmutableList.of("x", "y"));
  }

  @Test
  public void testSimpleMultiplicativeOp2()
  {
    validateFlatten("1*2*3", "(* (* 1 2) 3)", "6");
    validateFlatten("1*2/3", "(/ (* 1 2) 3)", "0");
    validateFlatten("1/2*3", "(* (/ 1 2) 3)", "0");
    validateFlatten("1/2/3", "(/ (/ 1 2) 3)", "0");

    validateFlatten("1.0*2*3", "(* (* 1.0 2) 3)", "6.0");
    validateFlatten("1.0*2/3", "(/ (* 1.0 2) 3)", "0.6666666666666666");
    validateFlatten("1.0/2*3", "(* (/ 1.0 2) 3)", "1.5");
    validateFlatten("1.0/2/3", "(/ (/ 1.0 2) 3)", "0.16666666666666666");

    // partial
    validateFlatten("1.0*2*x", "(* (* 1.0 2) x)", "(* 2.0 x)");
    validateFlatten("1.0*2/x", "(/ (* 1.0 2) x)", "(/ 2.0 x)");
    validateFlatten("1.0/2*x", "(* (/ 1.0 2) x)", "(* 0.5 x)");
    validateFlatten("1.0/2/x", "(/ (/ 1.0 2) x)", "(/ 0.5 x)");

    // not working yet
    validateFlatten("1.0*x*3", "(* (* 1.0 x) 3)", "(* (* 1.0 x) 3)");
  }

  @Test
  public void testSimpleCarrot1()
  {
    validateFlatten("1^2", "(^ 1 2)", "1");
  }

  @Test
  public void testSimpleCarrot2()
  {
    validateFlatten("1^2^3", "(^ 1 (^ 2 3))", "1");
  }

  @Test
  public void testMixed()
  {
    validateFlatten("1+2*3", "(+ 1 (* 2 3))", "7");
    validateFlatten("1+(2*3)", "(+ 1 (* 2 3))", "7");
    validateFlatten("(1+2)*3", "(* (+ 1 2) 3)", "9");

    validateFlatten("1*2+3", "(+ (* 1 2) 3)", "5");
    validateFlatten("(1*2)+3", "(+ (* 1 2) 3)", "5");
    validateFlatten("1*(2+3)", "(* 1 (+ 2 3))", "5");

    validateFlatten("1+2^3", "(+ 1 (^ 2 3))", "9");
    validateFlatten("1+(2^3)", "(+ 1 (^ 2 3))", "9");
    validateFlatten("(1+2)^3", "(^ (+ 1 2) 3)", "27");

    validateFlatten("1^2+3", "(+ (^ 1 2) 3)", "4");
    validateFlatten("(1^2)+3", "(+ (^ 1 2) 3)", "4");
    validateFlatten("1^(2+3)", "(^ 1 (+ 2 3))", "1");

    validateFlatten("1^2*3+4", "(+ (* (^ 1 2) 3) 4)", "7");
    validateFlatten("-1^2*-3+-4", "(+ (* (^ -1 2) -3) -4)", "-7");

    validateFlatten("max(3, 4)", "(max [3, 4])", "4");
    validateFlatten("min(1, max(3, 4))", "(min [1, (max [3, 4])])", "1");
  }

  @Test
  public void testIdentifiers()
  {
    validateParser("foo", "foo", ImmutableList.of("foo"), ImmutableSet.of());
    validateParser("\"foo\"", "foo", ImmutableList.of("foo"), ImmutableSet.of());
    validateParser("\"foo bar\"", "foo bar", ImmutableList.of("foo bar"), ImmutableSet.of());
    validateParser("\"foo\\\"bar\"", "foo\"bar", ImmutableList.of("foo\"bar"), ImmutableSet.of());
  }

  @Test
  public void testLiterals()
  {
    validateConstantExpression("\'foo\'", "foo");
    validateConstantExpression("\'foo bar\'", "foo bar");
    validateConstantExpression("\'föo bar\'", "föo bar");
    validateConstantExpression("\'f\\u0040o bar\'", "f@o bar");
    validateConstantExpression("\'f\\u000Ao \\'b\\\\\\\"ar\'", "f\no 'b\\\"ar");
  }

  @Test
  public void testLiteralArraysHomogeneousElements()
  {
    validateConstantExpression("[1.0, 2.345]", new Double[]{1.0, 2.345});
    validateConstantExpression("[1, 3]", new Long[]{1L, 3L});
    validateConstantExpression("['hello', 'world']", new String[]{"hello", "world"});
  }

  @Test
  public void testLiteralArraysHomogeneousOrNullElements()
  {
    validateConstantExpression("[1.0, null, 2.345]", new Double[]{1.0, null, 2.345});
    validateConstantExpression("[null, 1, 3]", new Long[]{null, 1L, 3L});
    validateConstantExpression("['hello', 'world', null]", new String[]{"hello", "world", null});
  }

  @Test
  public void testLiteralArraysEmptyAndAllNullImplicitAreString()
  {
    validateConstantExpression("[]", new String[0]);
    validateConstantExpression("[null, null, null]", new String[]{null, null, null});
  }

  @Test
  public void testLiteralArraysImplicitTypedNumericMixed()
  {
    // implicit typed numeric arrays with mixed elements are doubles
    validateConstantExpression("[1, null, 2000.0]", new Double[]{1.0, null, 2000.0});
    validateConstantExpression("[1.0, null, 2000]", new Double[]{1.0, null, 2000.0});
  }

  @Test
  public void testLiteralArraysExplicitTypedEmpties()
  {
    validateConstantExpression("<STRING>[]", new String[0]);
    validateConstantExpression("<DOUBLE>[]", new Double[0]);
    validateConstantExpression("<LONG>[]", new Long[0]);
  }

  @Test
  public void testLiteralArraysExplicitAllNull()
  {
    validateConstantExpression("<DOUBLE>[null, null, null]", new Double[]{null, null, null});
    validateConstantExpression("<LONG>[null, null, null]", new Long[]{null, null, null});
    validateConstantExpression("<STRING>[null, null, null]", new String[]{null, null, null});
  }

  @Test
  public void testLiteralArraysExplicitTypes()
  {
    validateConstantExpression("<DOUBLE>[1.0, null, 2000.0]", new Double[]{1.0, null, 2000.0});
    validateConstantExpression("<LONG>[3, null, 4]", new Long[]{3L, null, 4L});
    validateConstantExpression("<STRING>['foo', 'bar', 'baz']", new String[]{"foo", "bar", "baz"});
  }

  @Test
  public void testLiteralArraysExplicitTypesMixedElements()
  {
    // explicit typed numeric arrays mixed numeric types should coerce to the correct explicit type
    validateConstantExpression("<DOUBLE>[3, null, 4, 2.345]", new Double[]{3.0, null, 4.0, 2.345});
    validateConstantExpression("<LONG>[1.0, null, 2000.0]", new Long[]{1L, null, 2000L});

    // explicit typed string arrays should accept any literal and convert to string
    validateConstantExpression("<STRING>['1', null, 2000, 1.1]", new String[]{"1", null, "2000", "1.1"});
  }

  @Test
  public void testLiteralArrayImplicitStringParseException()
  {
    // implicit typed string array cannot handle literals thate are not null or string
    expectedException.expect(RE.class);
    expectedException.expectMessage("Failed to parse array: element 2000 is not a string");
    validateConstantExpression("['1', null, 2000, 1.1]", new String[]{"1", null, "2000", "1.1"});
  }

  @Test
  public void testLiteralArraysExplicitLongParseException()
  {
    // explicit typed long arrays only handle numeric types
    expectedException.expect(RE.class);
    expectedException.expectMessage("Failed to parse array element '2000' as a long");
    validateConstantExpression("<LONG>[1, null, '2000']", new Long[]{1L, null, 2000L});
  }

  @Test
  public void testLiteralArraysExplicitDoubleParseException()
  {
    // explicit typed double arrays only handle numeric types
    expectedException.expect(RE.class);
    expectedException.expectMessage("Failed to parse array element '2000.0' as a double");
    validateConstantExpression("<DOUBLE>[1.0, null, '2000.0']", new Double[]{1.0, null, 2000.0});
  }

  @Test
  public void testFunctions()
  {
    validateParser("sqrt(x)", "(sqrt [x])", ImmutableList.of("x"));
    validateParser("if(cond,then,else)", "(if [cond, then, else])", ImmutableList.of("cond", "else", "then"));
    validateParser("cast(x, 'STRING')", "(cast [x, STRING])", ImmutableList.of("x"));
    validateParser("cast(x, 'LONG')", "(cast [x, LONG])", ImmutableList.of("x"));
    validateParser("cast(x, 'DOUBLE')", "(cast [x, DOUBLE])", ImmutableList.of("x"));
    validateParser(
        "cast(x, 'STRING_ARRAY')",
        "(cast [x, STRING_ARRAY])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "cast(x, 'LONG_ARRAY')",
        "(cast [x, LONG_ARRAY])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "cast(x, 'DOUBLE_ARRAY')",
        "(cast [x, DOUBLE_ARRAY])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "array_length(x)",
        "(array_length [x])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "array_concat(x, y)",
        "(array_concat [x, y])",
        ImmutableList.of("x", "y"),
        ImmutableSet.of(),
        ImmutableSet.of("x", "y")
    );
    validateParser(
        "array_append(x, y)",
        "(array_append [x, y])",
        ImmutableList.of("x", "y"),
        ImmutableSet.of("y"),
        ImmutableSet.of("x")
    );

    validateFlatten("sqrt(4)", "(sqrt [4])", "2.0");
    validateFlatten("array_concat([1, 2], [3, 4])", "(array_concat [[1, 2], [3, 4]])", "[1, 2, 3, 4]");
  }

  @Test
  public void testApplyFunctions()
  {
    validateParser(
        "map(() -> 1, x)",
        "(map ([] -> 1), [x])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "map((x) -> x + 1, x)",
        "(map ([x] -> (+ x 1)), [x])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "x + map((x) -> x + 1, y)",
        "(+ x (map ([x] -> (+ x 1)), [y]))",
        ImmutableList.of("x", "y"),
        ImmutableSet.of("x"),
        ImmutableSet.of("y")
    );
    validateParser(
        "x + map((x) -> x + 1, x)",
        "(+ x (map ([x] -> (+ x 1)), [x]))",
        ImmutableList.of("x"),
        ImmutableSet.of("x"),
        ImmutableSet.of("x_0")
    );
    validateParser(
        "map((x) -> concat(x, y), z)",
        "(map ([x] -> (concat [x, y])), [z])",
        ImmutableList.of("y", "z"),
        ImmutableSet.of("y"),
        ImmutableSet.of("z")
    );
    // 'y' is accumulator, and currently unknown
    validateParser(
        "fold((x, acc) -> acc + x, x, y)",
        "(fold ([x, acc] -> (+ acc x)), [x, y])",
        ImmutableList.of("x", "y"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );

    validateParser(
        "fold((x, acc) -> acc + x, map((x) -> x + 1, x), y)",
        "(fold ([x, acc] -> (+ acc x)), [(map ([x] -> (+ x 1)), [x]), y])",
        ImmutableList.of("x", "y"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );
    validateParser(
        "array_append(z, fold((x, acc) -> acc + x, map((x) -> x + 1, x), y))",
        "(array_append [z, (fold ([x, acc] -> (+ acc x)), [(map ([x] -> (+ x 1)), [x]), y])])",
        ImmutableList.of("x", "y", "z"),
        ImmutableSet.of(),
        ImmutableSet.of("x", "z")
    );
    validateParser(
        "map(z -> z + 1, array_append(z, fold((x, acc) -> acc + x, map((x) -> x + 1, x), y)))",
        "(map ([z] -> (+ z 1)), [(array_append [z, (fold ([x, acc] -> (+ acc x)), [(map ([x] -> (+ x 1)), [x]), y])])])",
        ImmutableList.of("x", "y", "z"),
        ImmutableSet.of(),
        ImmutableSet.of("x", "z")
    );

    validateParser(
        "array_append(map(z -> z + 1, array_append(z, fold((x, acc) -> acc + x, map((x) -> x + 1, x), y))), a)",
        "(array_append [(map ([z] -> (+ z 1)), [(array_append [z, (fold ([x, acc] -> (+ acc x)), [(map ([x] -> (+ x 1)), [x]), y])])]), a])",
        ImmutableList.of("x", "y", "a", "z"),
        ImmutableSet.of("a"),
        ImmutableSet.of("x", "z")
    );

    validateFlatten("map((x) -> x + 1, [1, 2, 3, 4])", "(map ([x] -> (+ x 1)), [[1, 2, 3, 4]])", "[2, 3, 4, 5]");
    validateFlatten(
        "map((x) -> x + z, [1, 2, 3, 4])",
        "(map ([x] -> (+ x z)), [[1, 2, 3, 4]])",
        "(map ([x] -> (+ x z)), [[1, 2, 3, 4]])"
    );
  }

  @Test
  public void testApplyUnapplied()
  {
    validateApplyUnapplied("x + 1", "(+ x 1)", "(+ x 1)", ImmutableList.of());
    validateApplyUnapplied("x + 1", "(+ x 1)", "(+ x 1)", ImmutableList.of("z"));
    validateApplyUnapplied("x + y", "(+ x y)", "(map ([x] -> (+ x y)), [x])", ImmutableList.of("x"));
    validateApplyUnapplied(
        "x + y",
        "(+ x y)",
        "(cartesian_map ([x, y] -> (+ x y)), [x, y])",
        ImmutableList.of("x", "y")
    );

    validateApplyUnapplied(
        "map(x -> x + y, x)",
        "(map ([x] -> (+ x y)), [x])",
        "(cartesian_map ([x, y] -> (+ x y)), [x, y])",
        ImmutableList.of("y")
    );
    validateApplyUnapplied(
        "map(x -> x + 1, x + 1)",
        "(map ([x] -> (+ x 1)), [(+ x 1)])",
        "(map ([x] -> (+ x 1)), [(map ([x] -> (+ x 1)), [x])])",
        ImmutableList.of("x")
    );
    validateApplyUnapplied(
        "fold((x, acc) -> acc + x + y, x, 0)",
        "(fold ([x, acc] -> (+ (+ acc x) y)), [x, 0])",
        "(cartesian_fold ([x, y, acc] -> (+ (+ acc x) y)), [x, y, 0])",
        ImmutableList.of("y")
    );
    validateApplyUnapplied(
        "z + fold((x, acc) -> acc + x + y, x, 0)",
        "(+ z (fold ([x, acc] -> (+ (+ acc x) y)), [x, 0]))",
        "(+ z (cartesian_fold ([x, y, acc] -> (+ (+ acc x) y)), [x, y, 0]))",
        ImmutableList.of("y")
    );
    validateApplyUnapplied(
        "z + fold((x, acc) -> acc + x + y, x, 0)",
        "(+ z (fold ([x, acc] -> (+ (+ acc x) y)), [x, 0]))",
        "(map ([z] -> (+ z (cartesian_fold ([x, y, acc] -> (+ (+ acc x) y)), [x, y, 0]))), [z])",
        ImmutableList.of("y", "z")
    );
    validateApplyUnapplied(
        "array_to_string(concat(x, 'hello'), ',')",
        "(array_to_string [(concat [x, hello]), ,])",
        "(array_to_string [(map ([x] -> (concat [x, hello])), [x]), ,])",
        ImmutableList.of("x", "y")
    );
    validateApplyUnapplied(
        "cast(x, 'LONG')",
        "(cast [x, LONG])",
        "(map ([x] -> (cast [x, LONG])), [x])",
        ImmutableList.of("x")
    );
    validateApplyUnapplied(
        "cartesian_map((x,y) -> x + y, x, y)",
        "(cartesian_map ([x, y] -> (+ x y)), [x, y])",
        "(cartesian_map ([x, y] -> (+ x y)), [x, y])",
        ImmutableList.of("y")
    );
    validateApplyUnapplied(
        "cast(x, 'LONG_ARRAY')",
        "(cast [x, LONG_ARRAY])",
        "(cast [x, LONG_ARRAY])",
        ImmutableList.of("x")
    );

    validateApplyUnapplied(
        "case_searched((x == 'b'),'b',(x == 'g'),'g','Other')",
        "(case_searched [(== x b), b, (== x g), g, Other])",
        "(map ([x] -> (case_searched [(== x b), b, (== x g), g, Other])), [x])",
        ImmutableList.of("x")
    );
  }

  @Test
  public void testUniquify()
  {
    validateParser("x-x", "(- x x)", ImmutableList.of("x"), ImmutableSet.of("x", "x_0"));
    validateParser(
        "x - x + x",
        "(+ (- x x) x)",
        ImmutableList.of("x"),
        ImmutableSet.of("x", "x_0", "x_1")
    );

    validateParser(
        "map((x) -> x + x, x)",
        "(map ([x] -> (+ x x)), [x])",
        ImmutableList.of("x"),
        ImmutableSet.of(),
        ImmutableSet.of("x")
    );

    validateApplyUnapplied(
        "x + x",
        "(+ x x)",
        "(map ([x] -> (+ x x)), [x])",
        ImmutableList.of("x")
    );

    validateApplyUnapplied(
        "x + x + x",
        "(+ (+ x x) x)",
        "(map ([x] -> (+ (+ x x) x)), [x])",
        ImmutableList.of("x")
    );

    // heh
    validateApplyUnapplied(
        "x + x + x + y + y + y + y + z + z + z",
        "(+ (+ (+ (+ (+ (+ (+ (+ (+ x x) x) y) y) y) y) z) z) z)",
        "(cartesian_map ([x, y, z] -> (+ (+ (+ (+ (+ (+ (+ (+ (+ x x) x) y) y) y) y) z) z) z)), [x, y, z])",
        ImmutableList.of("x", "y", "z")
    );
  }

  private void validateLiteral(String expr, ExprType type, Object expected)
  {
    validateLiteral(expr, type, expected, true);
  }

  private void validateLiteral(String expr, ExprType type, Object expected, boolean roundTrip)
  {
    Expr parsed = Parser.parse(expr, ExprMacroTable.nil(), false);
    Expr parsedFlat = Parser.parse(expr, ExprMacroTable.nil(), true);
    Assert.assertTrue(parsed.isLiteral());
    Assert.assertTrue(parsedFlat.isLiteral());
    Assert.assertEquals(type, parsed.getOutputType(emptyBinding));
    Assert.assertEquals(type, parsedFlat.getOutputType(emptyBinding));
    Assert.assertEquals(expected, parsed.getLiteralValue());
    Assert.assertEquals(expected, parsedFlat.getLiteralValue());
    if (roundTrip) {
      Assert.assertEquals(expr, parsed.stringify());
      Assert.assertEquals(expr, parsedFlat.stringify());
    }
  }

  private void validateFlatten(String expression, String withoutFlatten, String withFlatten)
  {
    Expr notFlat = Parser.parse(expression, ExprMacroTable.nil(), false);
    Expr flat = Parser.parse(expression, ExprMacroTable.nil(), true);
    Assert.assertEquals(expression, withoutFlatten, notFlat.toString());
    Assert.assertEquals(expression, withFlatten, flat.toString());

    Expr notFlatRoundTrip = Parser.parse(notFlat.stringify(), ExprMacroTable.nil(), false);
    Expr flatRoundTrip = Parser.parse(flat.stringify(), ExprMacroTable.nil(), true);
    Assert.assertEquals(expression, withoutFlatten, notFlatRoundTrip.toString());
    Assert.assertEquals(expression, withFlatten, flatRoundTrip.toString());
    Assert.assertEquals(notFlat.stringify(), notFlatRoundTrip.stringify());
    Assert.assertEquals(flat.stringify(), flatRoundTrip.stringify());
  }

  private void validateParser(String expression, String expected, List<String> identifiers)
  {
    validateParser(expression, expected, identifiers, ImmutableSet.copyOf(identifiers), Collections.emptySet());
  }

  private void validateParser(String expression, String expected, List<String> identifiers, Set<String> scalars)
  {
    validateParser(expression, expected, identifiers, scalars, Collections.emptySet());
  }

  private void validateParser(
      String expression,
      String expected,
      List<String> identifiers,
      Set<String> scalars,
      Set<String> arrays
  )
  {
    final Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    final Expr.BindingAnalysis deets = parsed.analyzeInputs();
    Assert.assertEquals(expression, expected, parsed.toString());
    Assert.assertEquals(expression, identifiers, deets.getRequiredBindingsList());
    Assert.assertEquals(expression, scalars, deets.getScalarVariables());
    Assert.assertEquals(expression, arrays, deets.getArrayVariables());

    final Expr parsedNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr roundTrip = Parser.parse(parsedNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertEquals(parsed.stringify(), roundTrip.stringify());
    final Expr.BindingAnalysis roundTripDeets = roundTrip.analyzeInputs();
    Assert.assertEquals(expression, identifiers, roundTripDeets.getRequiredBindingsList());
    Assert.assertEquals(expression, scalars, roundTripDeets.getScalarVariables());
    Assert.assertEquals(expression, arrays, roundTripDeets.getArrayVariables());
  }

  private void validateApplyUnapplied(
      String expression,
      String unapplied,
      String applied,
      List<String> identifiers
  )
  {
    final Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    Expr.BindingAnalysis deets = parsed.analyzeInputs();
    Parser.validateExpr(parsed, deets);
    final Expr transformed = Parser.applyUnappliedBindings(parsed, deets, identifiers);
    Assert.assertEquals(expression, unapplied, parsed.toString());
    Assert.assertEquals(applied, applied, transformed.toString());

    final Expr parsedNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    final Expr parsedRoundTrip = Parser.parse(parsedNoFlatten.stringify(), ExprMacroTable.nil());
    Expr.BindingAnalysis roundTripDeets = parsedRoundTrip.analyzeInputs();
    Parser.validateExpr(parsedRoundTrip, roundTripDeets);
    final Expr transformedRoundTrip = Parser.applyUnappliedBindings(parsedRoundTrip, roundTripDeets, identifiers);
    Assert.assertEquals(expression, unapplied, parsedRoundTrip.toString());
    Assert.assertEquals(applied, applied, transformedRoundTrip.toString());

    Assert.assertEquals(parsed.stringify(), parsedRoundTrip.stringify());
    Assert.assertEquals(transformed.stringify(), transformedRoundTrip.stringify());
  }

  private void validateConstantExpression(String expression, Object expected)
  {
    Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertEquals(
        expression,
        expected,
        parsed.eval(Parser.withMap(ImmutableMap.of())).value()
    );

    final Expr parsedNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    Expr parsedRoundTrip = Parser.parse(parsedNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertEquals(
        expression,
        expected,
        parsedRoundTrip.eval(Parser.withMap(ImmutableMap.of())).value()
    );
    Assert.assertEquals(parsed.stringify(), parsedRoundTrip.stringify());
  }

  private void validateConstantExpression(String expression, Object[] expected)
  {
    Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    Object evaluated = parsed.eval(Parser.withMap(ImmutableMap.of())).value();
    Assert.assertArrayEquals(
        expression,
        expected,
        (Object[]) evaluated
    );

    Assert.assertEquals(expected.getClass(), evaluated.getClass());
    final Expr parsedNoFlatten = Parser.parse(expression, ExprMacroTable.nil(), false);
    Expr roundTrip = Parser.parse(parsedNoFlatten.stringify(), ExprMacroTable.nil());
    Assert.assertArrayEquals(
        expression,
        expected,
        (Object[]) roundTrip.eval(Parser.withMap(ImmutableMap.of())).value()
    );
    Assert.assertEquals(parsed.stringify(), roundTrip.stringify());
  }
}
