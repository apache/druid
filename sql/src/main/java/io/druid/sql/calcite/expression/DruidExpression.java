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

package io.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.segment.column.ValueType;
import io.druid.segment.virtual.ExpressionVirtualColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represents three kinds of expression-like concepts that native Druid queries support:
 *
 * (1) SimpleExtractions, which are direct column access, possibly with an extractionFn
 * (2) native Druid expressions.
 */
public class DruidExpression
{
  // Must be sorted
  private static final char[] SAFE_CHARS = " ,._-;:(){}[]<>!@#$%^&*`~?/".toCharArray();

  static {
    Arrays.sort(SAFE_CHARS);
  }

  private final SimpleExtraction simpleExtraction;
  private final String expression;

  private DruidExpression(final SimpleExtraction simpleExtraction, final String expression)
  {
    this.simpleExtraction = simpleExtraction;
    this.expression = Preconditions.checkNotNull(expression);
  }

  public static DruidExpression of(final SimpleExtraction simpleExtraction, final String expression)
  {
    return new DruidExpression(simpleExtraction, expression);
  }

  public static DruidExpression fromColumn(final String column)
  {
    return new DruidExpression(SimpleExtraction.of(column, null), String.format("\"%s\"", escape(column)));
  }

  public static DruidExpression fromExpression(final String expression)
  {
    return new DruidExpression(null, expression);
  }

  public static DruidExpression fromFunctionCall(final String functionName, final List<DruidExpression> args)
  {
    return new DruidExpression(null, functionCall(functionName, args));
  }

  public static String numberLiteral(final Number n)
  {
    return n == null ? nullLiteral() : n.toString();
  }

  public static String stringLiteral(final String s)
  {
    return s == null ? nullLiteral() : "'" + escape(s) + "'";
  }

  public static String nullLiteral()
  {
    return "''";
  }

  public static String functionCall(final String functionName, final List<DruidExpression> args)
  {
    Preconditions.checkNotNull(functionName, "functionName");
    Preconditions.checkNotNull(args, "args");

    final StringBuilder builder = new StringBuilder(functionName);
    builder.append("(");

    for (int i = 0; i < args.size(); i++) {
      final DruidExpression arg = Preconditions.checkNotNull(args.get(i), "arg #%s", i);
      builder.append(arg.getExpression());
      if (i < args.size() - 1) {
        builder.append(",");
      }
    }

    builder.append(")");

    return builder.toString();
  }

  public static String functionCall(final String functionName, final DruidExpression... args)
  {
    return functionCall(functionName, Arrays.asList(args));
  }

  private static String escape(final String s)
  {
    final StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (Character.isLetterOrDigit(c) || Arrays.binarySearch(SAFE_CHARS, c) >= 0) {
        escaped.append(c);
      } else {
        escaped.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }
    return escaped.toString();
  }

  public String getExpression()
  {
    return expression;
  }

  public boolean isDirectColumnAccess()
  {
    return simpleExtraction != null && simpleExtraction.getExtractionFn() == null;
  }

  public String getDirectColumn()
  {
    return Preconditions.checkNotNull(simpleExtraction.getColumn());
  }

  public boolean isSimpleExtraction()
  {
    return simpleExtraction != null;
  }

  public Expr parse(final ExprMacroTable macroTable)
  {
    return Parser.parse(expression, macroTable);
  }

  public SimpleExtraction getSimpleExtraction()
  {
    return Preconditions.checkNotNull(simpleExtraction);
  }

  public ExpressionVirtualColumn toVirtualColumn(
      final String name,
      final ValueType outputType,
      final ExprMacroTable macroTable
  )
  {
    return new ExpressionVirtualColumn(name, expression, outputType, macroTable);
  }

  public DruidExpression map(
      final Function<SimpleExtraction, SimpleExtraction> extractionMap,
      final Function<String, String> expressionMap
  )
  {
    return new DruidExpression(
        simpleExtraction == null ? null : extractionMap.apply(simpleExtraction),
        expressionMap.apply(expression)
    );
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DruidExpression that = (DruidExpression) o;
    return Objects.equals(simpleExtraction, that.simpleExtraction) &&
           Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(simpleExtraction, expression);
  }

  @Override
  public String toString()
  {
    return "DruidExpression{" +
           "simpleExtraction=" + simpleExtraction +
           ", expression='" + expression + '\'' +
           '}';
  }
}
