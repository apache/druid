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
    String actual = Parser.parse("-1").toString();
    String expected = "-1";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("--1").toString();
    expected = "--1";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("-1+2").toString();
    expected = "(+ -1 2)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("-1*2").toString();
    expected = "(* -1 2)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("-1^2").toString();
    expected = "(^ -1 2)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleLogicalOps1()
  {
    String actual = Parser.parse("x>y").toString();
    String expected = "(> x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x<y").toString();
    expected = "(< x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x<=y").toString();
    expected = "(<= x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x>=y").toString();
    expected = "(>= x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x==y").toString();
    expected = "(== x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x!=y").toString();
    expected = "(!= x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x && y").toString();
    expected = "(&& x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x || y").toString();
    expected = "(|| x y)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleAdditivityOp1()
  {
    String actual = Parser.parse("x+y").toString();
    String expected = "(+ x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x-y").toString();
    expected = "(- x y)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleAdditivityOp2()
  {
    String actual = Parser.parse("x+y+z").toString();
    String expected = "(+ (+ x y) z)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x+y-z").toString();
    expected = "(- (+ x y) z)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x-y+z").toString();
    expected = "(+ (- x y) z)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x-y-z").toString();
    expected = "(- (- x y) z)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleMultiplicativeOp1()
  {
    String actual = Parser.parse("x*y").toString();
    String expected = "(* x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x/y").toString();
    expected = "(/ x y)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("x%y").toString();
    expected = "(% x y)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleMultiplicativeOp2()
  {
    String actual = Parser.parse("1*2*3").toString();
    String expected = "(* (* 1 2) 3)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1*2/3").toString();
    expected = "(/ (* 1 2) 3)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1/2*3").toString();
    expected = "(* (/ 1 2) 3)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1/2/3").toString();
    expected = "(/ (/ 1 2) 3)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleCarrot1()
  {
    String actual = Parser.parse("1^2").toString();
    String expected = "(^ 1 2)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testSimpleCarrot2()
  {
    String actual = Parser.parse("1^2^3").toString();
    String expected = "(^ 1 (^ 2 3))";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testMixed()
  {
    String actual = Parser.parse("1+2*3").toString();
    String expected = "(+ 1 (* 2 3))";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1+(2*3)").toString();
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("(1+2)*3").toString();
    expected = "(* (+ 1 2) 3)";
    Assert.assertEquals(expected, actual);


    actual = Parser.parse("1*2+3").toString();
    expected = "(+ (* 1 2) 3)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("(1*2)+3").toString();
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1*(2+3)").toString();
    expected = "(* 1 (+ 2 3))";
    Assert.assertEquals(expected, actual);


    actual = Parser.parse("1+2^3").toString();
    expected = "(+ 1 (^ 2 3))";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1+(2^3)").toString();
    expected = "(+ 1 (^ 2 3))";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("(1+2)^3").toString();
    expected = "(^ (+ 1 2) 3)";
    Assert.assertEquals(expected, actual);


    actual = Parser.parse("1^2+3").toString();
    expected = "(+ (^ 1 2) 3)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("(1^2)+3").toString();
    expected = "(+ (^ 1 2) 3)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("1^(2+3)").toString();
    expected = "(^ 1 (+ 2 3))";
    Assert.assertEquals(expected, actual);


    actual = Parser.parse("1^2*3+4").toString();
    expected = "(+ (* (^ 1 2) 3) 4)";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("-1^-2*-3+-4").toString();
    expected = "(+ (* (^ -1 -2) -3) -4)";
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testFunctions()
  {
    String actual = Parser.parse("sqrt(x)").toString();
    String expected = "(sqrt [x])";
    Assert.assertEquals(expected, actual);

    actual = Parser.parse("if(cond,then,else)").toString();
    expected = "(if [cond, then, else])";
    Assert.assertEquals(expected, actual);
  }
}
