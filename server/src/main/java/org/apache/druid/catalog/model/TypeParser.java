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

package org.apache.druid.catalog.model;

import org.apache.druid.catalog.model.MeasureTypes.MeasureType;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Parser for the SQL type format used in the catalog.
 * <ul>
 * <li>{@code null} - A null type means "use whatever Druid wants."</li>
 * <li>Scalar - {@code VARCHAR}, {@code BIGINT}, etc.</li>
 * <li>{@code TIMESTAMP('<grain'>} - truncated timestamp</li>
 * <li>{@code fn(<type>)} - aggregate measure such as @{code COUNT},
 *     {@code COUNT()} or {@code SUM(BIGINT)}.</li>
 * </ul>
 */
public class TypeParser
{
  public static final ParsedType ANY_TYPE = new ParsedType(null, ParsedType.Kind.ANY, null, null);
  public static final ParsedType TIME_TYPE = new ParsedType(null, ParsedType.Kind.TIME, null, null);

  public static class ParsedType
  {
    public enum Kind
    {
      ANY,
      TIME,
      DIMENSION,
      MEASURE
    }

    private final String type;
    private final Kind kind;
    private final String timeGrain;
    private final MeasureType measureType;

    public ParsedType(String type, Kind kind, String timeGrain, MeasureType measureType)
    {
      this.type = type;
      this.kind = kind;
      this.timeGrain = timeGrain;
      this.measureType = measureType;
    }

    public String type()
    {
      return type;
    }

    public Kind kind()
    {
      return kind;
    }

    public String timeGrain()
    {
      return timeGrain;
    }

    public MeasureType measure()
    {
      return measureType;
    }

    @Override
    public String toString()
    {
      StringBuilder buf = new StringBuilder("ParsedType{type=")
          .append(type)
          .append(", kind=")
          .append(kind.name());
      if (timeGrain != null) {
        buf.append(", time grain=").append(timeGrain);
      }
      if (measureType != null) {
        buf.append(", measure type=").append(measureType);
      }
      return buf.append("}").toString();
    }
  }

  private static class Token
  {
    private enum Kind
    {
      SYMBOL,
      OPEN,
      STRING,
      COMMA,
      CLOSE
    }

    private final Kind kind;
    private final String value;

    public Token(Kind kind, String value)
    {
      this.kind = kind;
      this.value = value;
    }
  }

  private String input;
  private int posn;

  private TypeParser(String type)
  {
    this.input = type;
  }

  public static ParsedType parse(String type)
  {
    if (type == null) {
      return ANY_TYPE;
    }
    return new TypeParser(type).parse();
  }

  private ParsedType parse()
  {
    Token token = parseToken();
    if (token == null) {
      return ANY_TYPE;
    }
    if (token.kind != Token.Kind.SYMBOL) {
      throw new IAE("Invalid type name");
    }
    final String baseName = StringUtils.toUpperCase(token.value);
    boolean isTime = Columns.isTimestamp(baseName);
    token = parseToken();
    if (token == null) {
      if (isTime) {
        return new ParsedType(baseName, ParsedType.Kind.TIME, null, null);
      } else if (Columns.isScalar(baseName)) {
        return new ParsedType(baseName, ParsedType.Kind.DIMENSION, null, null);
      }
      return analyzeAggregate(baseName, Collections.emptyList());
    }
    if (token.kind != Token.Kind.OPEN) {
      throw new IAE("Invalid type name");
    }
    if (!isTime && Columns.isScalar(baseName)) {
      throw new IAE("Invalid type name");
    }
    List<Token> args = new ArrayList<>();
    token = parseToken();
    if (token == null) {
      throw new IAE("Invalid type name");
    }
    if (token.kind != Token.Kind.CLOSE) {
      if (token.kind != Token.Kind.SYMBOL && token.kind != Token.Kind.STRING) {
        throw new IAE("Invalid type name");
      }
      args.add(token);
      while (true) {
        token = parseToken();
        if (token == null) {
          throw new IAE("Invalid type name");
        }
        if (token.kind == Token.Kind.CLOSE) {
          break;
        }
        if (token.kind != Token.Kind.COMMA) {
          throw new IAE("Invalid type name");
        }
        token = parseToken();
        if (token.kind != Token.Kind.SYMBOL && token.kind != Token.Kind.STRING) {
          throw new IAE("Invalid type name");
        }
        args.add(token);
      }
    }
    token = parseToken();
    if (token != null) {
      throw new IAE("Invalid type name");
    }
    if (isTime) {
      return analyzeTimestamp(args);
    } else {
      return analyzeAggregate(baseName, args);
    }
  }

  private ParsedType analyzeTimestamp(List<Token> args)
  {
    if (args.isEmpty()) {
      // Odd: TIMESTAMP(), but OK...
      return new ParsedType(input, ParsedType.Kind.TIME, null, null);
    }
    if (args.size() > 1 || args.get(0).kind != Token.Kind.STRING) {
      throw new IAE("Invalid type name");
    }
    String gran = args.get(0).value;
    CatalogUtils.validateGranularity(gran);
    return new ParsedType(input, ParsedType.Kind.TIME, gran, null);
  }

  private ParsedType analyzeAggregate(String baseName, List<Token> args)
  {
    return new ParsedType(
        input,
        ParsedType.Kind.MEASURE,
        null,
        MeasureTypes.parse(
            input,
            baseName,
            args.stream().map(arg -> arg.value).collect(Collectors.toList())
        )
    );
  }

  private Token parseToken()
  {
    char c = nextChar();
    while (c == ' ') {
      c = nextChar();
    }
    switch (c) {
      case 0:
        return null;
      case '(':
        return new Token(Token.Kind.OPEN, Character.toString(c));
      case ')':
        return new Token(Token.Kind.CLOSE, Character.toString(c));
      case ',':
        return new Token(Token.Kind.COMMA, Character.toString(c));
      case '\'':
        return parseString();
    }
    if (!Character.isAlphabetic(c)) {
      throw new IAE("Invalid type name");
    }
    return parseSymbol(c);
  }

  private char nextChar()
  {
    if (posn == input.length()) {
      return 0;
    }
    return input.charAt(posn++);
  }

  private Token parseString()
  {
    StringBuilder buf = new StringBuilder();
    while (true) {
      char c = nextChar();
      if (c == 0) {
        throw new IAE("Invalid type name");
      }
      if (c == '\'') {
        return new Token(Token.Kind.STRING, buf.toString());
      }
      buf.append(c);
    }
  }

  private Token parseSymbol(char c)
  {
    StringBuilder buf = new StringBuilder().append(c);
    while (true) {
      c = nextChar();
      if (c == 0 || !Character.isAlphabetic(c)) {
        if (c != 0) {
          posn--;
        }
        return new Token(Token.Kind.SYMBOL, buf.toString());
      }
      buf.append(c);
    }
  }
}
