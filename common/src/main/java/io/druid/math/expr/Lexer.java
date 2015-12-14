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

class Lexer
{
  private String input;
  private int currPos;

  private Token next;

  Lexer(String input)
  {
    this.input = input;
    currPos = 0;

    next = nextToken();
  }

  Token peek()
  {
    return next;
  }

  Token consume()
  {
    Token old = next;
    next = nextToken();
    return old;
  }

  private Token nextToken()
  {
    if (currPos >= input.length()) {
      return new Token(Token.EOF);
    }

    char c = input.charAt(currPos);
    while(c == ' ') {
      currPos++;
      if (currPos >= input.length()) {
        return new Token(Token.EOF);
      }
      c = input.charAt(currPos);
    }

    switch(c)
    {
      case '<':
        currPos++;
        if (currPos < input.length() && input.charAt(currPos) == '=') {
          currPos++;
          return new Token(Token.LEQ, "<=");
        } else {
          return new Token(Token.LT, "<");
        }
      case '>':
        currPos++;
        if (currPos < input.length() && input.charAt(currPos) == '=') {
          currPos++;
          return new Token(Token.GEQ, ">=");
        } else {
          return new Token(Token.GT, ">");
        }
      case '=':
        currPos++;
        if (currPos < input.length() && input.charAt(currPos) == '=') {
          currPos++;
          return new Token(Token.EQ, "==");
        } else {
          throw new IllegalArgumentException("unknown operator '='");
        }
      case '!':
        currPos++;
        if (currPos < input.length() && input.charAt(currPos) == '=') {
          currPos++;
          return new Token(Token.NEQ, "!=");
        } else {
          return new Token(Token.NOT, "!");
        }

      case '+':
        currPos++;
        return new Token(Token.PLUS, "+");
      case '-':
        currPos++;
        return new Token(Token.MINUS, "-");

      case '*':
        currPos++;
        return new Token(Token.MUL, "*");
      case '/':
        currPos++;
        return new Token(Token.DIV, "/");
      case '%':
        currPos++;
        return new Token(Token.MODULO, "%");

      case '^':
        currPos++;
        return new Token(Token.CARROT, "^");

      case '(':
        currPos++;
        return new Token(Token.LPAREN, "(");
      case ')':
        currPos++;
        return new Token(Token.RPAREN, ")");
      case ',':
        currPos++;
        return new Token(Token.COMMA, ",");

      default:
        if (isNumberStartingChar(c)) {
          return parseNumber();
        } else if (isIdentifierStartingChar(c)){
          return parseIdentifierOrKeyword();
        } else {
          throw new RuntimeException("Illegal expression " + toString());
        }
    }
  }

  private boolean isNumberStartingChar(char c)
  {
    return c >= '0' && c <= '9';
  }

  private boolean isIdentifierStartingChar(char c)
  {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           c == '$' ||
           c == '_';
  }

  private boolean isIdentifierChar(char c)
  {
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') ||
           c == '$' ||
           c == '_';
  }

  private Token parseIdentifierOrKeyword()
  {
    StringBuilder sb = new StringBuilder();
    char c = input.charAt(currPos++);
    while (isIdentifierChar(c)) {
      sb.append(c);

      if (currPos < input.length()) {
        c = input.charAt(currPos++);
      } else {
        currPos++;
        break;
      }
    };

    currPos--;

    String str = sb.toString();
    if(str.equals("and")) {
      return new Token(Token.AND, str);
    } else if(str.equals("or")) {
      return new Token(Token.OR, str);
    } else {
      return new Token(Token.IDENTIFIER, sb.toString());
    }
  }

  // Numbers
  // long : [0-9]+
  // double : [0-9]+.[0-9]+
  private Token parseNumber()
  {
    boolean isLong = true;
    StringBuilder sb = new StringBuilder();
    char c = input.charAt(currPos++);

    while (
        ('0' <= c && c <= '9') || c == '.'
        ) {
      if (c == '.') {
        isLong = false;
      }

      sb.append(c);

      if (currPos < input.length()) {
        c = input.charAt(currPos++);
      } else {
        currPos++;
        break;
      }
    };

    currPos--;
    if (isLong) {
      return new Token(Token.LONG, sb.toString());
    } else {
      return new Token(Token.DOUBLE, sb.toString());
    }
  }

  @Override
  public String toString()
  {
    return "at " + currPos + ", " + input;
  }
}
