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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Chars;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.math.expr.ExprType;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.Column;
import io.druid.sql.calcite.aggregation.PostAggregatorFactory;
import io.druid.sql.calcite.filtration.BoundRefKey;
import io.druid.sql.calcite.filtration.Bounds;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * A collection of functions for translating from Calcite expressions into Druid objects.
 */
public class Expressions
{
  private static final Map<String, String> MATH_FUNCTIONS = ImmutableMap.<String, String>builder()
      .put("ABS", "abs")
      .put("CEIL", "ceil")
      .put("EXP", "exp")
      .put("FLOOR", "floor")
      .put("LN", "log")
      .put("LOG10", "log10")
      .put("POWER", "pow")
      .put("SQRT", "sqrt")
      .build();

  private static final Map<SqlTypeName, ExprType> MATH_TYPES;

  static {
    final ImmutableMap.Builder<SqlTypeName, ExprType> builder = ImmutableMap.builder();

    for (SqlTypeName type : SqlTypeName.APPROX_TYPES) {
      builder.put(type, ExprType.DOUBLE);
    }

    for (SqlTypeName type : SqlTypeName.EXACT_TYPES) {
      builder.put(type, ExprType.LONG);
    }

    for (SqlTypeName type : SqlTypeName.STRING_TYPES) {
      builder.put(type, ExprType.STRING);
    }

    MATH_TYPES = builder.build();
  }

  private Expressions()
  {
    // No instantiation.
  }

  /**
   * Translate a field access, possibly through a projection, to an underlying Druid dataSource.
   *
   * @param rowSignature row signature of underlying Druid dataSource
   * @param project      projection, or null
   * @param fieldNumber  number of the field to access
   *
   * @return row expression
   */
  public static RexNode fromFieldAccess(
      final RowSignature rowSignature,
      final Project project,
      final int fieldNumber
  )
  {
    if (project == null) {
      // I don't think the factory impl matters here.
      return RexInputRef.of(fieldNumber, rowSignature.getRelDataType(new JavaTypeFactoryImpl()));
    } else {
      return project.getChildExps().get(fieldNumber);
    }
  }

  /**
   * Translate a Calcite row-expression to a Druid row extraction. Note that this signature will probably need to
   * change once we support extractions from multiple columns.
   *
   * @param plannerContext SQL planner context
   * @param rowOrder       order of fields in the Druid rows to be extracted from
   * @param expression     expression meant to be applied on top of the rows
   *
   * @return RowExtraction or null if not possible
   */
  public static RowExtraction toRowExtraction(
      final DruidOperatorTable operatorTable,
      final PlannerContext plannerContext,
      final List<String> rowOrder,
      final RexNode expression
  )
  {
    if (expression.getKind() == SqlKind.INPUT_REF) {
      final RexInputRef ref = (RexInputRef) expression;
      final String columnName = rowOrder.get(ref.getIndex());
      if (columnName == null) {
        throw new ISE("WTF?! Expression referred to nonexistent index[%d]", ref.getIndex());
      }

      return RowExtraction.of(columnName, null);
    } else if (expression.getKind() == SqlKind.CAST) {
      final RexNode operand = ((RexCall) expression).getOperands().get(0);
      if (expression.getType().getSqlTypeName() == SqlTypeName.DATE
          && operand.getType().getSqlTypeName() == SqlTypeName.TIMESTAMP) {
        // Handling casting TIMESTAMP to DATE by flooring to DAY.
        return FloorExtractionOperator.applyTimestampFloor(
            toRowExtraction(operatorTable, plannerContext, rowOrder, operand),
            TimeUnits.toQueryGranularity(TimeUnitRange.DAY, plannerContext.getTimeZone())
        );
      } else {
        // Ignore other casts.
        // TODO(gianm): Probably not a good idea to ignore other CASTs like this.
        return toRowExtraction(operatorTable, plannerContext, rowOrder, ((RexCall) expression).getOperands().get(0));
      }
    } else {
      // Try conversion using a SqlExtractionOperator.
      final RowExtraction retVal;

      if (expression instanceof RexCall) {
        final SqlExtractionOperator extractionOperator = operatorTable.lookupExtractionOperator(
            expression.getKind(),
            ((RexCall) expression).getOperator().getName()
        );

        retVal = extractionOperator != null
                 ? extractionOperator.convert(operatorTable, plannerContext, rowOrder, expression)
                 : null;
      } else {
        retVal = null;
      }

      return retVal;
    }
  }

  /**
   * Translate a Calcite row-expression to a Druid PostAggregator. One day, when possible, this could be folded
   * into {@link #toRowExtraction(DruidOperatorTable, PlannerContext, List, RexNode)} .
   *
   * @param name                              name of the PostAggregator
   * @param rowOrder                          order of fields in the Druid rows to be extracted from
   * @param finalizingPostAggregatorFactories post-aggregators that should be used for specific entries in rowOrder.
   *                                          May be empty, and individual values may be null. Missing or null values
   *                                          will lead to creation of {@link FieldAccessPostAggregator}.
   * @param expression                        expression meant to be applied on top of the rows
   *
   * @return PostAggregator or null if not possible
   */
  public static PostAggregator toPostAggregator(
      final String name,
      final List<String> rowOrder,
      final List<PostAggregatorFactory> finalizingPostAggregatorFactories,
      final RexNode expression
  )
  {
    final PostAggregator retVal;

    if (expression.getKind() == SqlKind.INPUT_REF) {
      final RexInputRef ref = (RexInputRef) expression;
      final PostAggregatorFactory finalizingPostAggregatorFactory = finalizingPostAggregatorFactories.get(ref.getIndex());
      retVal = finalizingPostAggregatorFactory != null
               ? finalizingPostAggregatorFactory.factorize(name)
               : new FieldAccessPostAggregator(name, rowOrder.get(ref.getIndex()));
    } else if (expression.getKind() == SqlKind.CAST) {
      // Ignore CAST when translating to PostAggregators and hope for the best. They are really loosey-goosey with
      // types internally and there isn't much we can do to respect
      // TODO(gianm): Probably not a good idea to ignore CAST like this.
      final RexNode operand = ((RexCall) expression).getOperands().get(0);
      retVal = toPostAggregator(name, rowOrder, finalizingPostAggregatorFactories, operand);
    } else if (expression.getKind() == SqlKind.LITERAL
               && SqlTypeName.NUMERIC_TYPES.contains(expression.getType().getSqlTypeName())) {
      retVal = new ConstantPostAggregator(name, (Number) RexLiteral.value(expression));
    } else if (expression.getKind() == SqlKind.TIMES
               || expression.getKind() == SqlKind.DIVIDE
               || expression.getKind() == SqlKind.PLUS
               || expression.getKind() == SqlKind.MINUS) {
      final String fnName = ImmutableMap.<SqlKind, String>builder()
          .put(SqlKind.TIMES, "*")
          .put(SqlKind.DIVIDE, "quotient")
          .put(SqlKind.PLUS, "+")
          .put(SqlKind.MINUS, "-")
          .build().get(expression.getKind());
      final List<PostAggregator> operands = Lists.newArrayList();
      for (RexNode operand : ((RexCall) expression).getOperands()) {
        final PostAggregator translatedOperand = toPostAggregator(
            null,
            rowOrder,
            finalizingPostAggregatorFactories,
            operand
        );
        if (translatedOperand == null) {
          return null;
        }
        operands.add(translatedOperand);
      }
      retVal = new ArithmeticPostAggregator(name, fnName, operands);
    } else {
      // Try converting to a math expression.
      final String mathExpression = Expressions.toMathExpression(rowOrder, expression);
      if (mathExpression == null) {
        retVal = null;
      } else {
        retVal = new ExpressionPostAggregator(name, mathExpression);
      }
    }

    if (retVal != null && name != null && !name.equals(retVal.getName())) {
      throw new ISE("WTF?! Was about to return a PostAggregator with bad name, [%s] != [%s]", name, retVal.getName());
    }

    return retVal;
  }

  /**
   * Translate a row-expression to a Druid math expression. One day, when possible, this could be folded into
   * {@link #toRowExtraction(DruidOperatorTable, PlannerContext, List, RexNode)}.
   *
   * @param rowOrder   order of fields in the Druid rows to be extracted from
   * @param expression expression meant to be applied on top of the rows
   *
   * @return expression referring to fields in rowOrder, or null if not possible
   */
  public static String toMathExpression(
      final List<String> rowOrder,
      final RexNode expression
  )
  {
    final SqlKind kind = expression.getKind();
    final SqlTypeName sqlTypeName = expression.getType().getSqlTypeName();

    if (kind == SqlKind.INPUT_REF) {
      // Translate field references.
      final RexInputRef ref = (RexInputRef) expression;
      final String columnName = rowOrder.get(ref.getIndex());
      if (columnName == null) {
        throw new ISE("WTF?! Expression referred to nonexistent index[%d]", ref.getIndex());
      }

      return String.format("\"%s\"", escape(columnName));
    } else if (kind == SqlKind.CAST || kind == SqlKind.REINTERPRET) {
      // Translate casts.
      final RexNode operand = ((RexCall) expression).getOperands().get(0);
      final String operandExpression = toMathExpression(rowOrder, operand);
      if (operandExpression == null) {
        return null;
      }

      final ExprType fromType = MATH_TYPES.get(operand.getType().getSqlTypeName());
      final ExprType toType = MATH_TYPES.get(sqlTypeName);
      if (fromType != toType) {
        return String.format("CAST(%s, '%s')", operandExpression, toType.toString());
      } else {
        return operandExpression;
      }
    } else if (kind == SqlKind.TIMES || kind == SqlKind.DIVIDE || kind == SqlKind.PLUS || kind == SqlKind.MINUS) {
      // Translate simple arithmetic.
      final List<RexNode> operands = ((RexCall) expression).getOperands();
      final String lhsExpression = toMathExpression(rowOrder, operands.get(0));
      final String rhsExpression = toMathExpression(rowOrder, operands.get(1));
      if (lhsExpression == null || rhsExpression == null) {
        return null;
      }

      final String op = ImmutableMap.of(
          SqlKind.TIMES, "*",
          SqlKind.DIVIDE, "/",
          SqlKind.PLUS, "+",
          SqlKind.MINUS, "-"
      ).get(kind);

      return String.format("(%s %s %s)", lhsExpression, op, rhsExpression);
    } else if (kind == SqlKind.OTHER_FUNCTION) {
      final String calciteFunction = ((RexCall) expression).getOperator().getName();
      final String druidFunction = MATH_FUNCTIONS.get(calciteFunction);
      final List<String> functionArgs = Lists.newArrayList();

      for (final RexNode operand : ((RexCall) expression).getOperands()) {
        final String operandExpression = toMathExpression(rowOrder, operand);
        if (operandExpression == null) {
          return null;
        }
        functionArgs.add(operandExpression);
      }

      if ("MOD".equals(calciteFunction)) {
        // Special handling for MOD, which is a function in Calcite but a binary operator in Druid.
        Preconditions.checkState(functionArgs.size() == 2, "WTF?! Expected 2 args for MOD.");
        return String.format("(%s %s %s)", functionArgs.get(0), "%", functionArgs.get(1));
      }

      if (druidFunction == null) {
        return null;
      }

      return String.format("%s(%s)", druidFunction, Joiner.on(", ").join(functionArgs));
    } else if (kind == SqlKind.LITERAL) {
      // Translate literal.
      if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
        // Include literal numbers as-is.
        return String.valueOf(RexLiteral.value(expression));
      } else if (SqlTypeName.STRING_TYPES.contains(sqlTypeName)) {
        // Quote literal strings.
        return "\'" + escape(RexLiteral.stringValue(expression)) + "\'";
      } else {
        // Can't translate other literals.
        return null;
      }
    } else {
      // Can't translate other kinds of expressions.
      return null;
    }
  }

  /**
   * Translates "literal" (a TIMESTAMP or DATE literal) to milliseconds since the epoch using the provided
   * session time zone.
   *
   * @param literal  TIMESTAMP or DATE literal
   * @param timeZone session time zone
   *
   * @return milliseconds time
   */
  public static long toMillisLiteral(final RexNode literal, final DateTimeZone timeZone)
  {
    final SqlTypeName typeName = literal.getType().getSqlTypeName();
    if (literal.getKind() != SqlKind.LITERAL || (typeName != SqlTypeName.TIMESTAMP && typeName != SqlTypeName.DATE)) {
      throw new IAE("Expected TIMESTAMP or DATE literal but got[%s:%s]", literal.getKind(), typeName);
    }

    final Calendar calendar = (Calendar) RexLiteral.value(literal);
    return Calcites.calciteTimestampToJoda(calendar.getTimeInMillis(), timeZone).getMillis();
  }

  /**
   * Translates "condition" to a Druid filter, or returns null if we cannot translate the condition.
   *
   * @param plannerContext planner context
   * @param rowSignature   row signature of the dataSource to be filtered
   * @param expression     Calcite row expression
   */
  public static DimFilter toFilter(
      final DruidOperatorTable operatorTable,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode expression
  )
  {
    if (expression.getKind() == SqlKind.AND
        || expression.getKind() == SqlKind.OR
        || expression.getKind() == SqlKind.NOT) {
      final List<DimFilter> filters = Lists.newArrayList();
      for (final RexNode rexNode : ((RexCall) expression).getOperands()) {
        final DimFilter nextFilter = toFilter(operatorTable, plannerContext, rowSignature, rexNode);
        if (nextFilter == null) {
          return null;
        }
        filters.add(nextFilter);
      }

      if (expression.getKind() == SqlKind.AND) {
        return new AndDimFilter(filters);
      } else if (expression.getKind() == SqlKind.OR) {
        return new OrDimFilter(filters);
      } else {
        assert expression.getKind() == SqlKind.NOT;
        return new NotDimFilter(Iterables.getOnlyElement(filters));
      }
    } else {
      // Handle filter conditions on everything else.
      return toLeafFilter(operatorTable, plannerContext, rowSignature, expression);
    }
  }

  /**
   * Translates "condition" to a Druid filter, assuming it does not contain any boolean expressions. Returns null
   * if we cannot translate the condition.
   *
   * @param plannerContext planner context
   * @param rowSignature   row signature of the dataSource to be filtered
   * @param expression     Calcite row expression
   */
  private static DimFilter toLeafFilter(
      final DruidOperatorTable operatorTable,
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode expression
  )
  {
    if (expression.isAlwaysTrue()) {
      return Filtration.matchEverything();
    } else if (expression.isAlwaysFalse()) {
      return Filtration.matchNothing();
    }

    final SqlKind kind = expression.getKind();

    if (kind == SqlKind.LIKE) {
      final List<RexNode> operands = ((RexCall) expression).getOperands();
      final RowExtraction rex = toRowExtraction(
          operatorTable,
          plannerContext,
          rowSignature.getRowOrder(),
          operands.get(0)
      );
      if (rex == null || !rex.isFilterable(rowSignature)) {
        return null;
      }
      return new LikeDimFilter(
          rex.getColumn(),
          RexLiteral.stringValue(operands.get(1)),
          operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
          rex.getExtractionFn()
      );
    } else if (kind == SqlKind.EQUALS
               || kind == SqlKind.NOT_EQUALS
               || kind == SqlKind.GREATER_THAN
               || kind == SqlKind.GREATER_THAN_OR_EQUAL
               || kind == SqlKind.LESS_THAN
               || kind == SqlKind.LESS_THAN_OR_EQUAL) {
      final List<RexNode> operands = ((RexCall) expression).getOperands();
      Preconditions.checkState(operands.size() == 2, "WTF?! Expected 2 operands, got[%,d]", operands.size());
      boolean flip = false;
      RexNode lhs = operands.get(0);
      RexNode rhs = operands.get(1);

      if (lhs.getKind() == SqlKind.LITERAL && rhs.getKind() != SqlKind.LITERAL) {
        // swap lhs, rhs
        RexNode x = lhs;
        lhs = rhs;
        rhs = x;
        flip = true;
      }

      // rhs must be a literal
      if (rhs.getKind() != SqlKind.LITERAL) {
        return null;
      }

      // lhs must be translatable to a RowExtraction to be filterable
      final RowExtraction rex = toRowExtraction(operatorTable, plannerContext, rowSignature.getRowOrder(), lhs);
      if (rex == null || !rex.isFilterable(rowSignature)) {
        return null;
      }

      final String column = rex.getColumn();
      final ExtractionFn extractionFn = rex.getExtractionFn();

      if (column.equals(Column.TIME_COLUMN_NAME) && extractionFn instanceof TimeFormatExtractionFn) {
        // Check if we can strip the extractionFn and convert the filter to a direct filter on __time.
        // This allows potential conversion to query-level "intervals" later on, which is ideal for Druid queries.

        final Granularity granularity = ExtractionFns.toQueryGranularity(extractionFn);
        if (granularity != null) {
          // lhs is FLOOR(__time TO granularity); rhs must be a timestamp
          final long rhsMillis = toMillisLiteral(rhs, plannerContext.getTimeZone());
          final Interval rhsInterval = granularity.bucket(new DateTime(rhsMillis));

          // Is rhs aligned on granularity boundaries?
          final boolean rhsAligned = rhsInterval.getStartMillis() == rhsMillis;

          // Create a BoundRefKey that strips the extractionFn and compares __time as a number.
          final BoundRefKey boundRefKey = new BoundRefKey(column, null, StringComparators.NUMERIC);

          if (kind == SqlKind.EQUALS) {
            return rhsAligned
                   ? Bounds.interval(boundRefKey, rhsInterval)
                   : Filtration.matchNothing();
          } else if (kind == SqlKind.NOT_EQUALS) {
            return rhsAligned
                   ? new NotDimFilter(Bounds.interval(boundRefKey, rhsInterval))
                   : Filtration.matchEverything();
          } else if ((!flip && kind == SqlKind.GREATER_THAN) || (flip && kind == SqlKind.LESS_THAN)) {
            return Bounds.greaterThanOrEqualTo(boundRefKey, String.valueOf(rhsInterval.getEndMillis()));
          } else if ((!flip && kind == SqlKind.GREATER_THAN_OR_EQUAL) || (flip && kind == SqlKind.LESS_THAN_OR_EQUAL)) {
            return rhsAligned
                   ? Bounds.greaterThanOrEqualTo(boundRefKey, String.valueOf(rhsInterval.getStartMillis()))
                   : Bounds.greaterThanOrEqualTo(boundRefKey, String.valueOf(rhsInterval.getEndMillis()));
          } else if ((!flip && kind == SqlKind.LESS_THAN) || (flip && kind == SqlKind.GREATER_THAN)) {
            return rhsAligned
                   ? Bounds.lessThan(boundRefKey, String.valueOf(rhsInterval.getStartMillis()))
                   : Bounds.lessThan(boundRefKey, String.valueOf(rhsInterval.getEndMillis()));
          } else if ((!flip && kind == SqlKind.LESS_THAN_OR_EQUAL) || (flip && kind == SqlKind.GREATER_THAN_OR_EQUAL)) {
            return Bounds.lessThan(boundRefKey, String.valueOf(rhsInterval.getEndMillis()));
          } else {
            throw new IllegalStateException("WTF?! Shouldn't have got here...");
          }
        }
      }

      final String val;
      final RexLiteral rhsLiteral = (RexLiteral) rhs;
      if (SqlTypeName.NUMERIC_TYPES.contains(rhsLiteral.getTypeName())) {
        val = String.valueOf(RexLiteral.value(rhsLiteral));
      } else if (SqlTypeName.CHAR_TYPES.contains(rhsLiteral.getTypeName())) {
        val = String.valueOf(RexLiteral.stringValue(rhsLiteral));
      } else if (SqlTypeName.TIMESTAMP == rhsLiteral.getTypeName() || SqlTypeName.DATE == rhsLiteral.getTypeName()) {
        val = String.valueOf(toMillisLiteral(rhsLiteral, plannerContext.getTimeZone()));
      } else {
        // Don't know how to filter on this kind of literal.
        return null;
      }

      // Numeric lhs needs a numeric comparison.
      final boolean lhsIsNumeric = SqlTypeName.NUMERIC_TYPES.contains(lhs.getType().getSqlTypeName())
                                   || SqlTypeName.TIMESTAMP == lhs.getType().getSqlTypeName()
                                   || SqlTypeName.DATE == lhs.getType().getSqlTypeName();
      final StringComparator comparator = lhsIsNumeric ? StringComparators.NUMERIC : StringComparators.LEXICOGRAPHIC;

      final BoundRefKey boundRefKey = new BoundRefKey(column, extractionFn, comparator);
      final DimFilter filter;

      // Always use BoundDimFilters, to simplify filter optimization later (it helps to remember the comparator).
      if (kind == SqlKind.EQUALS) {
        filter = Bounds.equalTo(boundRefKey, val);
      } else if (kind == SqlKind.NOT_EQUALS) {
        filter = new NotDimFilter(Bounds.equalTo(boundRefKey, val));
      } else if ((!flip && kind == SqlKind.GREATER_THAN) || (flip && kind == SqlKind.LESS_THAN)) {
        filter = Bounds.greaterThan(boundRefKey, val);
      } else if ((!flip && kind == SqlKind.GREATER_THAN_OR_EQUAL) || (flip && kind == SqlKind.LESS_THAN_OR_EQUAL)) {
        filter = Bounds.greaterThanOrEqualTo(boundRefKey, val);
      } else if ((!flip && kind == SqlKind.LESS_THAN) || (flip && kind == SqlKind.GREATER_THAN)) {
        filter = Bounds.lessThan(boundRefKey, val);
      } else if ((!flip && kind == SqlKind.LESS_THAN_OR_EQUAL) || (flip && kind == SqlKind.GREATER_THAN_OR_EQUAL)) {
        filter = Bounds.lessThanOrEqualTo(boundRefKey, val);
      } else {
        throw new IllegalStateException("WTF?! Shouldn't have got here...");
      }

      return filter;
    } else {
      return null;
    }
  }

  private static String escape(final String s)
  {
    final StringBuilder escaped = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      if (Character.isLetterOrDigit(c) || Character.isWhitespace(c)) {
        escaped.append(c);
      } else {
        escaped.append("\\u").append(BaseEncoding.base16().encode(Chars.toByteArray(c)));
      }
    }
    return escaped.toString();
  }
}
