/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.math.expr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
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

    validateParser("x-y-x", "(- (- x y) x)", ImmutableList.of("x", "y"));
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
    validateParser("foo", "foo", ImmutableList.of("foo"));
    validateParser("\"foo\"", "foo", ImmutableList.of("foo"));
    validateParser("\"foo bar\"", "foo bar", ImmutableList.of("foo bar"));
    validateParser("\"foo\\\"bar\"", "foo\"bar", ImmutableList.of("foo\"bar"));
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
  public void testFunctions()
  {
    validateParser("sqrt(x)", "(sqrt [x])", ImmutableList.of("x"));
    validateParser("if(cond,then,else)", "(if [cond, then, else])", ImmutableList.of("cond", "then", "else"));
  }

  private void validateFlatten(String expression, String withoutFlatten, String withFlatten)
  {
    Assert.assertEquals(expression, withoutFlatten, Parser.parse(expression, ExprMacroTable.nil(), false).toString());
    Assert.assertEquals(expression, withFlatten, Parser.parse(expression, ExprMacroTable.nil(), true).toString());
  }

  private void validateParser(String expression, String expected, List<String> identifiers)
  {
    final Expr parsed = Parser.parse(expression, ExprMacroTable.nil());
    Assert.assertEquals(expression, expected, parsed.toString());
    Assert.assertEquals(expression, identifiers, Parser.findRequiredBindings(parsed));
  }

  private void validateConstantExpression(String expression, Object expected)
  {
    Assert.assertEquals(
        expression,
        expected,
        Parser.parse(expression, ExprMacroTable.nil()).eval(Parser.withMap(ImmutableMap.of())).value()
    );
  }
}
