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
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class ParserTest
{
  @Test
  public void testSimple()
  {
    String actual = Parser.parse("1", ExprMacroTable.nil()).toString();
    String expected = "1";
    Assert.assertEquals(expected, actual);
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
  public void testLiteralArrays()
  {
    validateConstantExpression("[1.0, 2.345]", new Double[]{1.0, 2.345});
    validateConstantExpression("[1, 3]", new Long[]{1L, 3L});
    validateConstantExpression("[\'hello\', \'world\']", new String[]{"hello", "world"});
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
        "(cartesian_map ([x, x_0] -> (+ x x_0)), [x, x])",
        ImmutableList.of("x")
    );

    validateApplyUnapplied(
        "x + x + x",
        "(+ (+ x x) x)",
        "(cartesian_map ([x, x_0, x_1] -> (+ (+ x x_0) x_1)), [x, x, x])",
        ImmutableList.of("x")
    );

    // heh
    validateApplyUnapplied(
        "x + x + x + y + y + y + y + z + z + z",
        "(+ (+ (+ (+ (+ (+ (+ (+ (+ x x) x) y) y) y) y) z) z) z)",
        "(cartesian_map ([x, x_0, x_1, y, y_2, y_3, y_4, z, z_5, z_6] -> (+ (+ (+ (+ (+ (+ (+ (+ (+ x x_0) x_1) y) y_2) y_3) y_4) z) z_5) z_6)), [x, x, x, y, y, y, y, z, z, z])",
        ImmutableList.of("x", "y", "z")
    );
  }


  private void validateFlatten(String expression, String withoutFlatten, String withFlatten)
  {
    Assert.assertEquals(expression, withoutFlatten, Parser.parse(expression, ExprMacroTable.nil(), false).toString());
    Assert.assertEquals(expression, withFlatten, Parser.parse(expression, ExprMacroTable.nil(), true).toString());
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
    final Expr.BindingDetails deets = parsed.analyzeInputs();
    Assert.assertEquals(expression, expected, parsed.toString());
    Assert.assertEquals(expression, identifiers, deets.getRequiredBindingsList());
    Assert.assertEquals(expression, scalars, deets.getScalarVariables());
    Assert.assertEquals(expression, arrays, deets.getArrayVariables());
  }

  private void validateApplyUnapplied(
      String expression,
      String unapplied,
      String applied,
      List<String> identifiers
  )
  {
    final Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    Expr.BindingDetails deets = parsed.analyzeInputs();
    Parser.validateExpr(parsed, deets);
    final Expr transformed = Parser.applyUnappliedBindings(parsed, deets, identifiers);
    Assert.assertEquals(expression, unapplied, parsed.toString());
    Assert.assertEquals(applied, applied, transformed.toString());
  }

  private void validateConstantExpression(String expression, Object expected)
  {
    Assert.assertEquals(
        expression,
        expected,
        Parser.parse(expression, ExprMacroTable.nil()).eval(Parser.withMap(ImmutableMap.of())).value()
    );
  }

  private void validateConstantExpression(String expression, Object[] expected)
  {
    Assert.assertArrayEquals(
        expression,
        expected,
        (Object[]) Parser.parse(expression, ExprMacroTable.nil()).eval(Parser.withMap(ImmutableMap.of())).value()
    );
  }
}
