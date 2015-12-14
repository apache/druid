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

/**
 */
class Token
{
  static final int AND = 0;
  static final int OR = 1;

  static final int LT = 2;
  static final int LEQ = 3;
  static final int GT = 4;
  static final int GEQ = 5;
  static final int EQ = 6;
  static final int NEQ = 7;

  static final int PLUS = 8;
  static final int MINUS = 9;

  static final int MUL = 10;
  static final int DIV = 11;
  static final int MODULO = 12;

  static final int CARROT = 13;
  static final int NUM_BINARY_OPERATORS = 14;

  static final int LPAREN = 51;
  static final int RPAREN = 52;
  static final int COMMA = 53;
  static final int NOT = 54;
  static final int IDENTIFIER = 55;
  static final int LONG = 56;
  static final int DOUBLE = 57;

  static final int EOF = 100;

  static final int[] PRECEDENCE;
  static final int[] R_PRECEDENCE;
  static final int UNARY_MINUS_PRECEDENCE;
  static final int UNARY_NOT_PRECEDENCE;

  static {
    PRECEDENCE = new int[NUM_BINARY_OPERATORS];
    R_PRECEDENCE = new int[NUM_BINARY_OPERATORS];
    //R_RECEDENCE = (op is left-associative) ? PRECEDENCE + 1 : PRECEDENCE

    int precedenceCounter = 0;

    PRECEDENCE[AND] = precedenceCounter;
    R_PRECEDENCE[AND] = PRECEDENCE[AND] + 1;

    PRECEDENCE[OR] = precedenceCounter;
    R_PRECEDENCE[OR] = PRECEDENCE[OR] + 1;

    precedenceCounter++;

    PRECEDENCE[EQ] = precedenceCounter;
    R_PRECEDENCE[EQ] = PRECEDENCE[EQ] + 1;

    PRECEDENCE[NEQ] = precedenceCounter;
    R_PRECEDENCE[NEQ] = PRECEDENCE[NEQ] + 1;

    PRECEDENCE[LT] = precedenceCounter;
    R_PRECEDENCE[LT] = PRECEDENCE[LT] + 1;

    PRECEDENCE[LEQ] = precedenceCounter;
    R_PRECEDENCE[LEQ] = PRECEDENCE[LEQ] + 1;

    PRECEDENCE[GT] = precedenceCounter;
    R_PRECEDENCE[GT] = PRECEDENCE[GT] + 1;

    PRECEDENCE[GEQ] = precedenceCounter;
    R_PRECEDENCE[GEQ] = PRECEDENCE[GEQ] + 1;

    precedenceCounter++;

    PRECEDENCE[PLUS] = precedenceCounter;
    R_PRECEDENCE[PLUS] = PRECEDENCE[PLUS] + 1;

    PRECEDENCE[MINUS] = precedenceCounter;
    R_PRECEDENCE[MINUS] = PRECEDENCE[MINUS] + 1;

    precedenceCounter++;

    PRECEDENCE[MUL] = precedenceCounter;
    R_PRECEDENCE[MUL] = PRECEDENCE[MUL] + 1;

    PRECEDENCE[DIV] = precedenceCounter;
    R_PRECEDENCE[DIV] = PRECEDENCE[DIV] + 1;

    PRECEDENCE[MODULO] = precedenceCounter;
    R_PRECEDENCE[MODULO] = PRECEDENCE[MODULO] + 1;

    precedenceCounter++;

    PRECEDENCE[CARROT] = precedenceCounter;
    R_PRECEDENCE[CARROT] = PRECEDENCE[CARROT];

    precedenceCounter++;

    UNARY_MINUS_PRECEDENCE = precedenceCounter;
    UNARY_NOT_PRECEDENCE = precedenceCounter;
  }

  private final int type;
  private final String match;

  Token(int type)
  {
    this(type, "");
  }

  Token(int type, String match)
  {
    this.type = type;
    this.match = match;
  }

  int getType()
  {
    return type;
  }

  String getMatch()
  {
    return match;
  }

  @Override
  public String toString()
  {
    return match;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Token token = (Token) o;

    if (type != token.type) {
      return false;
    }
    return !(match != null ? !match.equals(token.match) : token.match != null);

  }

  @Override
  public int hashCode()
  {
    int result = type;
    result = 31 * result + (match != null ? match.hashCode() : 0);
    return result;
  }
}
