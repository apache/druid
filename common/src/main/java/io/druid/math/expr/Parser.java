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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Parser
{
  static final Map<String, Function> func = new HashMap<>();

  static {
    func.put("sqrt", new SqrtFunc());
    func.put("if", new ConditionFunc());
  }

  private final Lexer lexer;

  private Parser(Lexer lexer)
  {
    this.lexer = lexer;
  }

  public static Expr parse(String input)
  {
    return new Parser(new Lexer(input)).parseExpr(0);
  }

  private Expr parseExpr(int p)
  {
    Expr result = new SimpleExpr(parseAtom());

    Token t = lexer.peek();
    while (
        t.getType() < Token.NUM_BINARY_OPERATORS
        &&
        Token.PRECEDENCE[t.getType()] >= p
        ) {
      lexer.consume();
      result = new BinExpr(
          t,
          result,
          parseExpr(Token.R_PRECEDENCE[t.getType()])
      );

      t = lexer.peek();
    }

    return result;
  }

  private Atom parseAtom()
  {
    Token t = lexer.peek();

    switch(t.getType()) {
      case Token.IDENTIFIER:
        lexer.consume();
        String id = t.getMatch();

        if (func.containsKey(id)) {
          expect(Token.LPAREN);
          List<Expr> args = new ArrayList<>();
          t = lexer.peek();
          while(t.getType() != Token.RPAREN) {
            args.add(parseExpr(0));
            t = lexer.peek();
            if (t.getType() == Token.COMMA) {
              lexer.consume();
            }
          }
          expect(Token.RPAREN);
          return new FunctionAtom(id, args);
        }

        return new IdentifierAtom(t);
      case Token.LONG:
        lexer.consume();
        return new LongValueAtom(Long.valueOf(t.getMatch()));
      case Token.DOUBLE:
        lexer.consume();
        return new DoubleValueAtom(Double.valueOf(t.getMatch()));
      case Token.MINUS:
        lexer.consume();
        return new UnaryMinusExprAtom(parseExpr(Token.UNARY_MINUS_PRECEDENCE));
      case Token.NOT:
        lexer.consume();
        return new UnaryNotExprAtom(parseExpr(Token.UNARY_NOT_PRECEDENCE));
      case Token.LPAREN:
        lexer.consume();
        Expr expression = parseExpr(0);
        if(lexer.consume().getType() == Token.RPAREN) {
          return new NestedExprAtom(expression);
        }
      default:
        throw new RuntimeException("Invalid token found " + t + " in input " + lexer);
    }
  }

  private void expect(int type)
  {
    Token t = lexer.consume();
    if(t.getType() != type) {
      throw new RuntimeException("Invalid token found " + t + " in input " + lexer);
    }
  }
}
