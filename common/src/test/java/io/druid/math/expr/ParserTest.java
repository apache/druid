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

import org.junit.Assert;
import org.junit.Test;

/**
 */
public class ParserTest
{
  @Test
  public void testSimple()
  {
    String actual = Parser.parse("1").toString();
    String expected = "1";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleUnaryOps1()
  {
    String actual = Parser.parse("-x").toString();
    String expected = "-x";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("!x").toString();
    expected = "!x";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleUnaryOps2()
  {
    validate("-1", "-1", "-1");
    validate("--1", "--1", "1");
    validate("-1+2", "(+ -1 2)", "1");
    validate("-1*2", "(* -1 2)", "-2");
    validate("-1^2", "(^ -1 2)", "1");
  }

  private void validateParser(String expression, String expected, String identifiers)
  {
    Assert.assertEquals(expected, Parser.parse(expression).toString());
    Assert.assertEquals(identifiers, Parser.findRequiredBindings(expression).toString());
  }

  @Test
  public void testSimpleLogicalOps1()
  {
    validateParser("x>y", "(> x y)", "[x, y]");
    validateParser("x<y", "(< x y)", "[x, y]");
    validateParser("x<=y", "(<= x y)", "[x, y]");
    validateParser("x>=y", "(>= x y)", "[x, y]");
    validateParser("x==y", "(== x y)", "[x, y]");
    validateParser("x!=y", "(!= x y)", "[x, y]");
    validateParser("x && y", "(&& x y)", "[x, y]");
    validateParser("x || y", "(|| x y)", "[x, y]");
  }

  @Test
  public void testSimpleAdditivityOp1()
  {
    validateParser("x+y", "(+ x y)", "[x, y]");
    validateParser("x-y", "(- x y)", "[x, y]");
  }

  @Test
  public void testSimpleAdditivityOp2()
  {
    validateParser("x+y+z", "(+ (+ x y) z)", "[x, y, z]");
    validateParser("x+y-z", "(- (+ x y) z)", "[x, y, z]");
    validateParser("x-y+z", "(+ (- x y) z)", "[x, y, z]");
    validateParser("x-y-z", "(- (- x y) z)", "[x, y, z]");

    validateParser("x-y-x", "(- (- x y) x)", "[x, y]");
  }

  @Test
  public void testSimpleMultiplicativeOp1()
  {
    validateParser("x*y", "(* x y)", "[x, y]");
    validateParser("x/y", "(/ x y)", "[x, y]");
    validateParser("x%y", "(% x y)", "[x, y]");
  }

  @Test
  public void testSimpleMultiplicativeOp2()
  {
    validate("1*2*3", "(* (* 1 2) 3)", "6");
    validate("1*2/3", "(/ (* 1 2) 3)", "0");
    validate("1/2*3", "(* (/ 1 2) 3)", "0");
    validate("1/2/3", "(/ (/ 1 2) 3)", "0");

    validate("1.0*2*3", "(* (* 1.0 2) 3)", "6.0");
    validate("1.0*2/3", "(/ (* 1.0 2) 3)", "0.6666666666666666");
    validate("1.0/2*3", "(* (/ 1.0 2) 3)", "1.5");
    validate("1.0/2/3", "(/ (/ 1.0 2) 3)", "0.16666666666666666");

    // partial
    validate("1.0*2*x", "(* (* 1.0 2) x)", "(* 2.0 x)");
    validate("1.0*2/x", "(/ (* 1.0 2) x)", "(/ 2.0 x)");
    validate("1.0/2*x", "(* (/ 1.0 2) x)", "(* 0.5 x)");
    validate("1.0/2/x", "(/ (/ 1.0 2) x)", "(/ 0.5 x)");

    // not working yet
    validate("1.0*x*3", "(* (* 1.0 x) 3)", "(* (* 1.0 x) 3)");
  }

  @Test
  public void testSimpleCarrot1()
  {
    validate("1^2", "(^ 1 2)", "1");
  }

  @Test
  public void testSimpleCarrot2()
  {
    validate("1^2^3", "(^ 1 (^ 2 3))", "1");
  }

  @Test
  public void testMixed()
  {
    validate("1+2*3", "(+ 1 (* 2 3))", "7");
    validate("1+(2*3)", "(+ 1 (* 2 3))", "7");
    validate("(1+2)*3", "(* (+ 1 2) 3)", "9");

    validate("1*2+3", "(+ (* 1 2) 3)", "5");
    validate("(1*2)+3", "(+ (* 1 2) 3)", "5");
    validate("1*(2+3)", "(* 1 (+ 2 3))", "5");

    validate("1+2^3", "(+ 1 (^ 2 3))", "9");
    validate("1+(2^3)", "(+ 1 (^ 2 3))", "9");
    validate("(1+2)^3", "(^ (+ 1 2) 3)", "27");

    validate("1^2+3", "(+ (^ 1 2) 3)", "4");
    validate("(1^2)+3", "(+ (^ 1 2) 3)", "4");
    validate("1^(2+3)", "(^ 1 (+ 2 3))", "1");

    validate("1^2*3+4", "(+ (* (^ 1 2) 3) 4)", "7");
    validate("-1^2*-3+-4", "(+ (* (^ -1 2) -3) -4)", "-7");

    validate("max(3, 4)", "(max [3, 4])", "4");
    validate("min(1, max(3, 4))", "(min [1, (max [3, 4])])", "1");
  }

  private void validate(String expression, String withoutFlatten, String withFlatten)
  {
    Assert.assertEquals(withoutFlatten, Parser.parse(expression, false).toString());
    Assert.assertEquals(withFlatten, Parser.parse(expression, true).toString());
  }

  @Test
  public void testFunctions()
  {
    validateParser("sqrt(x)", "(sqrt [x])", "[x]");
    validateParser("if(cond,then,else)", "(if [cond, then, else])", "[cond, then, else]");
  }
}
