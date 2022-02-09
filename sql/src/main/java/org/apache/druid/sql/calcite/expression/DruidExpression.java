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

package org.apache.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represents two kinds of expression-like concepts that native Druid queries support:
 *
 * (1) SimpleExtractions, which are direct column access, possibly with an extractionFn
 * (2) native Druid expressions and virtual columns
 *
 * When added to {@link org.apache.druid.sql.calcite.rel.VirtualColumnRegistry} whenever used by projections, filters,
 * aggregators, or other query components, these will be converted into native virtual columns using
 * {@link #toVirtualColumn(String, ColumnType, ExprMacroTable)}
 *
 * Approximate expression structure is retained in the {@link #arguments}, which when fed into the
 * {@link ExpressionBuilder} that all {@link DruidExpression} must be created with will produce the final String
 * expression (which will be later parsed into {@link Expr} during native processing).
 *
 * This allows using the {@link DruidExpressionShuttle} to examine this expression "tree" and potentially rewrite some
 * or all of the tree as it visits nodes, and the {@link #nodeType} property provides high level classification of
 * the types of expression which a node produces.
 */
public class DruidExpression
{
  public enum NodeType
  {
    /**
     * constant value
     */
    LITERAL,
    /**
     * Identifier for a direct physical or virtual column access (column name or virtual column name)
     */
    IDENTIFIER,
    /**
     * Standard native druid expression, which can compute a string that can be parsed into {@link Expr}, or used
     * as an {@link ExpressionVirtualColumn}
     */
    EXPRESSION,
    /**
     * Expression backed by a specialized {@link VirtualColumn}, which might provide more optimized evaluation than
     * is possible with the standard
     */
    SPECIALIZED
  }

  // Must be sorted
  private static final char[] SAFE_CHARS = " ,._-;:(){}[]<>!@#$%^&*`~?/".toCharArray();
  private static final VirtualColumnBuilder DEFAULT_VIRTUAL_COLUMN_BUILDER = new ExpressionVirtualColumnBuilder();

  static {
    Arrays.sort(SAFE_CHARS);
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
    return "null";
  }

  public static ExpressionBuilder functionCall(final String functionName)
  {
    Preconditions.checkNotNull(functionName, "functionName");

    return args -> {
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
    };
  }

  /**
   * @deprecated use {@link #functionCall(String)} instead
   */
  @Deprecated
  public static String functionCall(final String functionName, final List<DruidExpression> args)
  {
    return functionCall(functionName).buildExpression(args);
  }

  /**
   * @deprecated use {@link #functionCall(String)} instead
   */
  @Deprecated
  public static String functionCall(final String functionName, final DruidExpression... args)
  {
    return functionCall(functionName).buildExpression(Arrays.asList(args));
  }

  public static DruidExpression ofLiteral(
      @Nullable final ColumnType columnType,
      final String literal
  )
  {
    return new DruidExpression(
        NodeType.LITERAL,
        columnType,
        null,
        new LiteralExpressionBuilder(literal),
        Collections.emptyList(),
        null
    );
  }

  public static DruidExpression ofStringLiteral(final String s)
  {
    return ofLiteral(ColumnType.STRING, stringLiteral(s));
  }

  public static DruidExpression ofColumn(
      @Nullable final ColumnType columnType,
      final String column,
      final SimpleExtraction simpleExtraction
  )
  {
    return new DruidExpression(
        NodeType.IDENTIFIER,
        columnType,
        simpleExtraction,
        new IdentifierExpressionBuilder(column),
        Collections.emptyList(),
        null
    );
  }

  public static DruidExpression ofColumn(final ColumnType columnType, final String column)
  {
    return ofColumn(columnType, column, SimpleExtraction.of(column, null));
  }

  public static DruidExpression ofFunctionCall(
      final ColumnType columnType,
      final String functionName,
      final List<DruidExpression> args
  )
  {
    return new DruidExpression(NodeType.EXPRESSION, columnType, null, functionCall(functionName), args, null);
  }

  public static DruidExpression ofVirtualColumn(
      final ColumnType type,
      final ExpressionBuilder expressionBuilder,
      final List<DruidExpression> arguments,
      final VirtualColumnBuilder virtualColumnBuilder
  )
  {
    return new DruidExpression(NodeType.SPECIALIZED, type, null, expressionBuilder, arguments, virtualColumnBuilder);
  }

  public static DruidExpression ofExpression(
      @Nullable final ColumnType columnType,
      final ExpressionBuilder expressionBuilder,
      final List<DruidExpression> arguments
  )
  {
    return new DruidExpression(NodeType.EXPRESSION, columnType, null, expressionBuilder, arguments, null);
  }

  public static DruidExpression ofExpression(
      @Nullable final ColumnType columnType,
      final SimpleExtraction simpleExtraction,
      final ExpressionBuilder expressionBuilder,
      final List<DruidExpression> arguments
  )
  {
    return new DruidExpression(NodeType.EXPRESSION, columnType, simpleExtraction, expressionBuilder, arguments, null);
  }

  /**
   * @deprecated use {@link #ofExpression(ColumnType, SimpleExtraction, ExpressionBuilder, List)} instead to participate
   * in virtual column and expression optimization
   */
  @Deprecated
  public static DruidExpression of(final SimpleExtraction simpleExtraction, final String expression)
  {
    return new DruidExpression(
        NodeType.EXPRESSION,
        null,
        simpleExtraction,
        new LiteralExpressionBuilder(expression),
        Collections.emptyList(),
        null
    );
  }

  /**
   * @deprecated use {@link #ofColumn(ColumnType, String)} or {@link #ofColumn(ColumnType, String, SimpleExtraction)}
   * instead
   */
  @Deprecated
  public static DruidExpression fromColumn(final String column)
  {
    return new DruidExpression(
        NodeType.EXPRESSION,
        null,
        SimpleExtraction.of(column, null),
        new IdentifierExpressionBuilder(column),
        Collections.emptyList(),
        null
    );
  }

  /**
   * @deprecated use {@link #ofExpression(ColumnType, ExpressionBuilder, List)} instead to participate in virtual
   * column and expression optimization
   */
  @Deprecated
  public static DruidExpression fromExpression(final String expression)
  {
    return new DruidExpression(
        NodeType.EXPRESSION,
        null,
        null,
        new LiteralExpressionBuilder(expression),
        Collections.emptyList(),
        null
    );
  }

  /**
   * @deprecated use {@link #ofFunctionCall(ColumnType, String, List)} instead to participate in virtual column and
   * expression optimization
   */
  @Deprecated
  public static DruidExpression fromFunctionCall(final String functionName, final List<DruidExpression> args)
  {
    return new DruidExpression(
        NodeType.EXPRESSION,
        null,
        null,
        new LiteralExpressionBuilder(functionCall(functionName, args)),
        Collections.emptyList(),
        null
    );
  }

  private final NodeType nodeType;
  @Nullable
  private final ColumnType druidType;
  private final List<DruidExpression> arguments;
  @Nullable
  private final SimpleExtraction simpleExtraction;
  private final ExpressionBuilder expressionBuilder;
  private final VirtualColumnBuilder virtualColumnBuilder;

  private final Supplier<String> expression;

  private DruidExpression(
      final NodeType nodeType,
      @Nullable final ColumnType druidType,
      @Nullable final SimpleExtraction simpleExtraction,
      final ExpressionBuilder expressionBuilder,
      final List<DruidExpression> arguments,
      @Nullable final VirtualColumnBuilder virtualColumnBuilder
  )
  {
    this.nodeType = nodeType;
    this.druidType = druidType;
    this.simpleExtraction = simpleExtraction;
    this.expressionBuilder = Preconditions.checkNotNull(expressionBuilder);
    this.arguments = arguments;
    this.virtualColumnBuilder = virtualColumnBuilder != null ? virtualColumnBuilder : DEFAULT_VIRTUAL_COLUMN_BUILDER;
    this.expression = Suppliers.memoize(() -> this.expressionBuilder.buildExpression(this.arguments));
  }

  public String getExpression()
  {
    return expression.get();
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

  public SimpleExtraction getSimpleExtraction()
  {
    return Preconditions.checkNotNull(simpleExtraction);
  }

  public List<DruidExpression> getArguments()
  {
    return arguments;
  }

  public Expr parse(final ExprMacroTable macroTable)
  {
    return Parser.parse(expression.get(), macroTable);
  }

  public VirtualColumn toVirtualColumn(
      final String name,
      final ColumnType outputType,
      final ExprMacroTable macroTable
  )
  {
    return virtualColumnBuilder.build(name, outputType, expression.get(), macroTable);
  }

  public NodeType getType()
  {
    return nodeType;
  }

  public ColumnType getDruidType()
  {
    return druidType;
  }

  public DruidExpression map(
      final Function<SimpleExtraction, SimpleExtraction> extractionMap,
      final Function<String, String> expressionMap
  )
  {
    return new DruidExpression(
        nodeType,
        druidType,
        simpleExtraction == null ? null : extractionMap.apply(simpleExtraction),
        (args) -> expressionMap.apply(expressionBuilder.buildExpression(args)),
        arguments,
        virtualColumnBuilder
    );
  }

  public DruidExpression withArguments(List<DruidExpression> newArgs)
  {
    return new DruidExpression(
        nodeType,
        druidType,
        simpleExtraction,
        expressionBuilder,
        newArgs,
        virtualColumnBuilder
    );
  }

  public DruidExpression visit(DruidExpressionShuttle shuttle)
  {
    return new DruidExpression(
        nodeType,
        druidType,
        simpleExtraction,
        expressionBuilder,
        shuttle.visitAll(arguments),
        virtualColumnBuilder
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
           Objects.equals(nodeType, that.nodeType) &&
           Objects.equals(druidType, that.druidType) &&
           Objects.equals(arguments, that.arguments) &&
           Objects.equals(expression.get(), that.expression.get());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(nodeType, druidType, simpleExtraction, arguments, expression.get());
  }

  @Override
  public String toString()
  {
    return "DruidExpression{" +
           "type=" + (druidType != null ? druidType.asTypeString() : nullLiteral()) +
           ", simpleExtraction=" + simpleExtraction +
           ", expression='" + expression.get() + '\'' +
           ", arguments=" + arguments +
           '}';
  }

  @FunctionalInterface
  public interface DruidExpressionShuttle
  {
    DruidExpression visit(DruidExpression expression);

    default List<DruidExpression> visitAll(List<DruidExpression> expressions)
    {
      List<DruidExpression> list = new ArrayList<>(expressions.size());
      for (DruidExpression expr : expressions) {
        list.add(visit(expr));
      }
      return list;
    }
  }

  @FunctionalInterface
  public interface DruidExpressionBuilder
  {
    DruidExpression buildExpression(List<DruidExpression> arguments);
  }

  @FunctionalInterface
  public interface ExpressionBuilder
  {
    String buildExpression(List<DruidExpression> arguments);
  }

  @FunctionalInterface
  public interface VirtualColumnBuilder
  {
    VirtualColumn build(String name, ColumnType outputType, String expression, ExprMacroTable macroTable);
  }

  /**
   * Direct reference to a physical or virtual column
   */
  public static class IdentifierExpressionBuilder implements ExpressionBuilder
  {
    private final String identifier;

    public IdentifierExpressionBuilder(String identifier)
    {
      this.identifier = escape(identifier);
    }

    @Override
    public String buildExpression(List<DruidExpression> arguments)
    {
      // identifier expression has no arguments
      return "\"" + identifier + "\"";
    }
  }

  /**
   * Builds expressions for a static constant value
   */
  public static class LiteralExpressionBuilder implements ExpressionBuilder
  {
    private final String literal;

    public LiteralExpressionBuilder(String literal)
    {
      this.literal = literal;
    }

    @Override
    public String buildExpression(List<DruidExpression> arguments)
    {
      // literal expression has no arguments
      return literal;
    }
  }

  public static class ExpressionVirtualColumnBuilder implements VirtualColumnBuilder
  {
    @Override
    public VirtualColumn build(String name, ColumnType outputType, String expression, ExprMacroTable macroTable)
    {
      return new ExpressionVirtualColumn(name, expression, outputType, macroTable);
    }
  }
}
