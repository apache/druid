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
public class LexerTest
{
  @Test
  public void testAllTokens()
  {
    testAllTokensAsserts("abcd<<=>>===!!=+-*/%^(),123 01.23");
    testAllTokensAsserts("  abcd <  <=  > >=  == !    !=+   -*/  %^ ()   ,  123  01.23  ");
  }

  private void testAllTokensAsserts(String input)
  {
    Lexer lexer = new Lexer(input);
    Assert.assertEquals(new Token(Token.IDENTIFIER, "abcd"), lexer.consume());
    Assert.assertEquals(new Token(Token.LT, "<"), lexer.consume());
    Assert.assertEquals(new Token(Token.LEQ, "<="), lexer.consume());
    Assert.assertEquals(new Token(Token.GT, ">"), lexer.consume());
    Assert.assertEquals(new Token(Token.GEQ, ">="), lexer.consume());
    Assert.assertEquals(new Token(Token.EQ, "=="), lexer.consume());
    Assert.assertEquals(new Token(Token.NOT, "!"), lexer.consume());
    Assert.assertEquals(new Token(Token.NEQ, "!="), lexer.consume());
    Assert.assertEquals(new Token(Token.PLUS, "+"), lexer.consume());
    Assert.assertEquals(new Token(Token.MINUS, "-"), lexer.consume());
    Assert.assertEquals(new Token(Token.MUL, "*"), lexer.consume());
    Assert.assertEquals(new Token(Token.DIV, "/"), lexer.consume());
    Assert.assertEquals(new Token(Token.MODULO, "%"), lexer.consume());
    Assert.assertEquals(new Token(Token.CARROT, "^"), lexer.consume());
    Assert.assertEquals(new Token(Token.LPAREN, "("), lexer.consume());
    Assert.assertEquals(new Token(Token.RPAREN, ")"), lexer.consume());
    Assert.assertEquals(new Token(Token.COMMA, ","), lexer.consume());
    Assert.assertEquals(new Token(Token.LONG, "123"), lexer.consume());
    Assert.assertEquals(new Token(Token.DOUBLE, "01.23"), lexer.consume());
    Assert.assertEquals(new Token(Token.EOF, ""), lexer.consume());
    Assert.assertEquals(new Token(Token.EOF, ""), lexer.consume());
  }
}
