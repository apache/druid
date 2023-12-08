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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.planner.ExpressionParser;

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
 * {@link #toVirtualColumn(String, ColumnType, ExpressionParser)}
 *
 * Approximate expression structure is retained in the {@link #arguments}, which when fed into the
 * {@link ExpressionGenerator} that all {@link DruidExpression} must be created with will produce the final String
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
  private static final VirtualColumnCreator DEFAULT_VIRTUAL_COLUMN_BUILDER = new ExpressionVirtualColumnCreator();

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

  public static String longLiteral(final long n)
  {
    return String.valueOf(n);
  }

  public static String doubleLiteral(final double n)
  {
    return String.valueOf(n);
  }

  public static String stringLiteral(final String s)
  {
    return s == null ? nullLiteral() : "'" + escape(s) + "'";
  }

  public static String nullLiteral()
  {
    return "null";
  }

  public static ExpressionGenerator functionCall(final String functionName)
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
    return functionCall(functionName).compile(args);
  }

  /**
   * @deprecated use {@link #functionCall(String)} instead
   */
  @Deprecated
  public static String functionCall(final String functionName, final DruidExpression... args)
  {
    return functionCall(functionName).compile(Arrays.asList(args));
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
        new LiteralExpressionGenerator(literal),
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
        new IdentifierExpressionGenerator(column),
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
      final ExpressionGenerator expressionGenerator,
      final List<DruidExpression> arguments,
      final VirtualColumnCreator virtualColumnCreator
  )
  {
    return new DruidExpression(NodeType.SPECIALIZED, type, null, expressionGenerator, arguments, virtualColumnCreator);
  }

  public static DruidExpression ofExpression(
      @Nullable final ColumnType columnType,
      final ExpressionGenerator expressionGenerator,
      final List<DruidExpression> arguments
  )
  {
    return new DruidExpression(NodeType.EXPRESSION, columnType, null, expressionGenerator, arguments, null);
  }

  public static DruidExpression ofExpression(
      @Nullable final ColumnType columnType,
      final SimpleExtraction simpleExtraction,
      final ExpressionGenerator expressionGenerator,
      final List<DruidExpression> arguments
  )
  {
    return new DruidExpression(NodeType.EXPRESSION, columnType, simpleExtraction, expressionGenerator, arguments, null);
  }

  /**
   * @deprecated use {@link #ofExpression(ColumnType, SimpleExtraction, ExpressionGenerator, List)} instead to participate
   * in virtual column and expression optimization
   */
  @Deprecated
  public static DruidExpression of(final SimpleExtraction simpleExtraction, final String expression)
  {
    return new DruidExpression(
        NodeType.EXPRESSION,
        null,
        simpleExtraction,
        new LiteralExpressionGenerator(expression),
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
        new IdentifierExpressionGenerator(column),
        Collections.emptyList(),
        null
    );
  }

  /**
   * @deprecated use {@link #ofExpression(ColumnType, ExpressionGenerator, List)} instead to participate in virtual
   * column and expression optimization
   */
  @Deprecated
  public static DruidExpression fromExpression(final String expression)
  {
    return new DruidExpression(
        NodeType.EXPRESSION,
        null,
        null,
        new LiteralExpressionGenerator(expression),
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
        new LiteralExpressionGenerator(functionCall(functionName, args)),
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
  private final ExpressionGenerator expressionGenerator;
  private final VirtualColumnCreator virtualColumnCreator;

  private final Supplier<String> expression;

  private DruidExpression(
      final NodeType nodeType,
      @Nullable final ColumnType druidType,
      @Nullable final SimpleExtraction simpleExtraction,
      final ExpressionGenerator expressionGenerator,
      final List<DruidExpression> arguments,
      @Nullable final VirtualColumnCreator virtualColumnCreator
  )
  {
    this.nodeType = nodeType;
    this.druidType = druidType;
    this.simpleExtraction = simpleExtraction;
    this.expressionGenerator = Preconditions.checkNotNull(expressionGenerator);
    this.arguments = arguments;
    this.virtualColumnCreator = virtualColumnCreator != null ? virtualColumnCreator : DEFAULT_VIRTUAL_COLUMN_BUILDER;
    this.expression = Suppliers.memoize(() -> this.expressionGenerator.compile(this.arguments));
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

  public boolean isArray()
  {
    return druidType != null && druidType.isArray();
  }

  /**
   * Get sub {@link DruidExpression} arguments of this expression
   */
  public List<DruidExpression> getArguments()
  {
    return arguments;
  }

  public VirtualColumn toVirtualColumn(
      final String name,
      final ColumnType outputType,
      final ExpressionParser parser
  )
  {
    return virtualColumnCreator.create(name, outputType, expression.get(), parser);
  }

  public VirtualColumn toExpressionVirtualColumn(
      final String name,
      final ColumnType outputType,
      final ExpressionParser parser
  )
  {
    return DEFAULT_VIRTUAL_COLUMN_BUILDER.create(name, outputType, expression.get(), parser);
  }

  public NodeType getType()
  {
    return nodeType;
  }

  /**
   * The {@link ColumnType} of this expression as inferred when this expression was created. This is likely the result
   * of converting the output of {@link org.apache.calcite.rex.RexNode#getType()} using
   * {@link org.apache.druid.sql.calcite.planner.Calcites#getColumnTypeForRelDataType(RelDataType)}, but may also be
   * supplied by other means.
   *
   * This value is not currently used other than for tracking the types of the {@link DruidExpression} tree. The
   * value passed to {@link #toVirtualColumn(String, ColumnType, ExpressionParser)} will instead be whatever type "hint"
   * was specified when the expression was added to the {@link org.apache.druid.sql.calcite.rel.VirtualColumnRegistry}.
   */
  @Nullable
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
        (args) -> expressionMap.apply(expressionGenerator.compile(args)),
        arguments,
        virtualColumnCreator
    );
  }

  public DruidExpression map(
      final Function<SimpleExtraction, SimpleExtraction> extractionMap,
      final Function<String, String> expressionMap,
      final ColumnType newType
  )
  {
    return new DruidExpression(
        nodeType,
        newType,
        simpleExtraction == null ? null : extractionMap.apply(simpleExtraction),
        (args) -> expressionMap.apply(expressionGenerator.compile(args)),
        arguments,
        virtualColumnCreator
    );
  }

  public DruidExpression withArguments(List<DruidExpression> newArgs)
  {
    return new DruidExpression(
        nodeType,
        druidType,
        simpleExtraction,
        expressionGenerator,
        newArgs,
        virtualColumnCreator
    );
  }

  /**
   * Visit all sub {@link DruidExpression} (the {@link #arguments} of this expression), allowing the
   * {@link DruidExpressionShuttle} to potentially rewrite these arguments with new {@link DruidExpression}, finally
   * building a new version of this {@link DruidExpression} with updated {@link #arguments}.
   */
  public DruidExpression visit(DruidExpressionShuttle shuttle)
  {
    return new DruidExpression(
        nodeType,
        druidType,
        simpleExtraction,
        expressionGenerator,
        shuttle.visitAll(arguments),
        virtualColumnCreator
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
        list.add(visit(expr.visit(this)));
      }
      return list;
    }
  }

  /**
   * Create a {@link DruidExpression} given some set of input argument sub-expressions
   */
  @FunctionalInterface
  public interface DruidExpressionCreator
  {
    DruidExpression create(List<DruidExpression> arguments);
  }

  /**
   * Used by {@link DruidExpression} to compile a string which can be parsed into an {@link Expr} from given the
   * sub-expression arguments
   */
  @FunctionalInterface
  public interface ExpressionGenerator
  {
    String compile(List<DruidExpression> arguments);
  }

  /**
   * Direct reference to a physical or virtual column
   */
  public static class IdentifierExpressionGenerator implements ExpressionGenerator
  {
    private final String identifier;

    public IdentifierExpressionGenerator(String identifier)
    {
      this.identifier = escape(identifier);
    }

    @Override
    public String compile(List<DruidExpression> arguments)
    {
      // identifier expression has no arguments
      return "\"" + identifier + "\"";
    }
  }

  /**
   * Builds expressions for a static constant value
   */
  public static class LiteralExpressionGenerator implements ExpressionGenerator
  {
    private final String literal;

    public LiteralExpressionGenerator(String literal)
    {
      this.literal = literal;
    }

    @Override
    public String compile(List<DruidExpression> arguments)
    {
      // literal expression has no arguments
      return literal;
    }
  }

  /**
   * Used by a {@link DruidExpression} to translate itself into a {@link VirtualColumn} to add to a native query when
   * referenced by a projection, filter, aggregator, etc.
   */
  @FunctionalInterface
  public interface VirtualColumnCreator
  {
    VirtualColumn create(String name, ColumnType outputType, String expression, ExpressionParser parser);
  }

  public static class ExpressionVirtualColumnCreator implements VirtualColumnCreator
  {
    @Override
    public VirtualColumn create(String name, ColumnType outputType, String expression, ExpressionParser parser)
    {
      return new ExpressionVirtualColumn(name, expression, parser.parse(expression), outputType);
    }
  }
}
