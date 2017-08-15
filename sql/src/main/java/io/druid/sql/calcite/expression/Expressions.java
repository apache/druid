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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.granularity.PeriodGranularity;
import io.druid.math.expr.ExprType;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExpressionDimFilter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.ordering.StringComparator;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.filtration.BoundRefKey;
import io.druid.sql.calcite.filtration.Bounds;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A collection of functions for translating from Calcite expressions into Druid objects.
 */
public class Expressions
{
  private static final Map<SqlOperator, String> DIRECT_CONVERSIONS = ImmutableMap.<SqlOperator, String>builder()
      .put(SqlStdOperatorTable.ABS, "abs")
      .put(SqlStdOperatorTable.CASE, "case_searched")
      .put(SqlStdOperatorTable.CHAR_LENGTH, "strlen")
      .put(SqlStdOperatorTable.CHARACTER_LENGTH, "strlen")
      .put(SqlStdOperatorTable.CONCAT, "concat")
      .put(SqlStdOperatorTable.EXP, "exp")
      .put(SqlStdOperatorTable.DIVIDE_INTEGER, "div")
      .put(SqlStdOperatorTable.LIKE, "like")
      .put(SqlStdOperatorTable.LN, "log")
      .put(SqlStdOperatorTable.LOWER, "lower")
      .put(SqlStdOperatorTable.LOG10, "log10")
      .put(SqlStdOperatorTable.POWER, "pow")
      .put(SqlStdOperatorTable.REPLACE, "replace")
      .put(SqlStdOperatorTable.SQRT, "sqrt")
      .put(SqlStdOperatorTable.TRIM, "trim")
      .put(SqlStdOperatorTable.UPPER, "upper")
      .build();

  private static final Map<SqlOperator, String> UNARY_PREFIX_OPERATOR_MAP = ImmutableMap.<SqlOperator, String>builder()
      .put(SqlStdOperatorTable.NOT, "!")
      .put(SqlStdOperatorTable.UNARY_MINUS, "-")
      .build();

  private static final Map<SqlOperator, String> UNARY_SUFFIX_OPERATOR_MAP = ImmutableMap.<SqlOperator, String>builder()
      .put(SqlStdOperatorTable.IS_NULL, "== ''")
      .put(SqlStdOperatorTable.IS_NOT_NULL, "!= ''")
      .put(SqlStdOperatorTable.IS_FALSE, "<= 0") // Matches Evals.asBoolean
      .put(SqlStdOperatorTable.IS_NOT_TRUE, "<= 0") // Matches Evals.asBoolean
      .put(SqlStdOperatorTable.IS_TRUE, "> 0") // Matches Evals.asBoolean
      .put(SqlStdOperatorTable.IS_NOT_FALSE, "> 0") // Matches Evals.asBoolean
      .build();

  private static final Map<SqlOperator, String> BINARY_OPERATOR_MAP = ImmutableMap.<SqlOperator, String>builder()
      .put(SqlStdOperatorTable.MULTIPLY, "*")
      .put(SqlStdOperatorTable.MOD, "%")
      .put(SqlStdOperatorTable.DIVIDE, "/")
      .put(SqlStdOperatorTable.PLUS, "+")
      .put(SqlStdOperatorTable.MINUS, "-")
      .put(SqlStdOperatorTable.EQUALS, "==")
      .put(SqlStdOperatorTable.NOT_EQUALS, "!=")
      .put(SqlStdOperatorTable.GREATER_THAN, ">")
      .put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, ">=")
      .put(SqlStdOperatorTable.LESS_THAN, "<")
      .put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "<=")
      .put(SqlStdOperatorTable.AND, "&&")
      .put(SqlStdOperatorTable.OR, "||")
      .build();

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
   * Translate a list of Calcite {@code RexNode} to Druid expressions.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNodes       list of Calcite expressions meant to be applied on top of the rows
   *
   * @return list of Druid expressions in the same order as rexNodes, or null if not possible.
   * If a non-null list is returned, all elements will be non-null.
   */
  @Nullable
  public static List<DruidExpression> toDruidExpressions(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final List<RexNode> rexNodes
  )
  {
    final List<DruidExpression> retVal = new ArrayList<>(rexNodes.size());
    for (RexNode rexNode : rexNodes) {
      final DruidExpression druidExpression = toDruidExpression(plannerContext, rowSignature, rexNode);
      if (druidExpression == null) {
        return null;
      }

      retVal.add(druidExpression);
    }
    return retVal;
  }

  /**
   * Translate a Calcite {@code RexNode} to a Druid expressions.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return rexNode referring to fields in rowOrder, or null if not possible
   */
  @Nullable
  public static DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final SqlKind kind = rexNode.getKind();
    final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();

    if (kind == SqlKind.INPUT_REF) {
      // Translate field references.
      final RexInputRef ref = (RexInputRef) rexNode;
      final String columnName = rowSignature.getRowOrder().get(ref.getIndex());
      if (columnName == null) {
        throw new ISE("WTF?! Expression referred to nonexistent index[%d]", ref.getIndex());
      }

      return DruidExpression.fromColumn(columnName);
    } else if (kind == SqlKind.CAST || kind == SqlKind.REINTERPRET) {
      // Translate casts.
      final RexNode operand = ((RexCall) rexNode).getOperands().get(0);
      final DruidExpression operandExpression = toDruidExpression(
          plannerContext,
          rowSignature,
          operand
      );
      if (operandExpression == null) {
        return null;
      }

      final SqlTypeName fromType = operand.getType().getSqlTypeName();
      final SqlTypeName toType = rexNode.getType().getSqlTypeName();

      if (SqlTypeName.CHAR_TYPES.contains(fromType) && SqlTypeName.DATETIME_TYPES.contains(toType)) {
        // Cast strings to datetimes by parsing them from SQL format.
        final DruidExpression timestampExpression = DruidExpression.fromFunctionCall(
            "timestamp_parse",
            ImmutableList.of(
                operandExpression,
                DruidExpression.fromExpression(DruidExpression.stringLiteral(dateTimeFormatString(toType)))
            )
        );

        if (toType == SqlTypeName.DATE) {
          return TimeFloorOperatorConversion.applyTimestampFloor(
              timestampExpression,
              new PeriodGranularity(Period.days(1), null, plannerContext.getTimeZone())
          );
        } else {
          return timestampExpression;
        }
      } else if (SqlTypeName.DATETIME_TYPES.contains(fromType) && SqlTypeName.CHAR_TYPES.contains(toType)) {
        // Cast datetimes to strings by formatting them in SQL format.
        return DruidExpression.fromFunctionCall(
            "timestamp_format",
            ImmutableList.of(
                operandExpression,
                DruidExpression.fromExpression(DruidExpression.stringLiteral(dateTimeFormatString(fromType)))
            )
        );
      } else {
        // Handle other casts.
        final ExprType fromExprType = exprTypeForValueType(Calcites.getValueTypeForSqlTypeName(fromType));
        final ExprType toExprType = exprTypeForValueType(Calcites.getValueTypeForSqlTypeName(toType));

        if (fromExprType == null || toExprType == null) {
          // We have no runtime type for these SQL types.
          return null;
        }

        final DruidExpression typeCastExpression;

        if (fromExprType != toExprType) {
          // Ignore casts for simple extractions (use Function.identity) since it is ok in many cases.
          typeCastExpression = operandExpression.map(
              Function.identity(),
              expression -> String.format("CAST(%s, '%s')", expression, toExprType.toString())
          );
        } else {
          typeCastExpression = operandExpression;
        }

        if (toType == SqlTypeName.DATE) {
          // Floor to day when casting to DATE.
          return TimeFloorOperatorConversion.applyTimestampFloor(
              typeCastExpression,
              new PeriodGranularity(Period.days(1), null, plannerContext.getTimeZone())
          );
        } else {
          return typeCastExpression;
        }
      }
    } else if (rexNode instanceof RexCall) {
      final SqlOperator operator = ((RexCall) rexNode).getOperator();

      final SqlOperatorConversion conversion = plannerContext.getOperatorTable()
                                                             .lookupOperatorConversion(operator);

      if (conversion != null) {
        return conversion.toDruidExpression(plannerContext, rowSignature, rexNode);
      }

      final List<DruidExpression> operands = Expressions.toDruidExpressions(
          plannerContext,
          rowSignature,
          ((RexCall) rexNode).getOperands()
      );

      if (operands == null) {
        return null;
      } else if (UNARY_PREFIX_OPERATOR_MAP.containsKey(operator)) {
        return DruidExpression.fromExpression(
            String.format(
                "(%s %s)",
                UNARY_PREFIX_OPERATOR_MAP.get(operator),
                Iterables.getOnlyElement(operands).getExpression()
            )
        );
      } else if (UNARY_SUFFIX_OPERATOR_MAP.containsKey(operator)) {
        return DruidExpression.fromExpression(
            String.format(
                "(%s %s)",
                Iterables.getOnlyElement(operands).getExpression(),
                UNARY_SUFFIX_OPERATOR_MAP.get(operator)
            )
        );
      } else if (BINARY_OPERATOR_MAP.containsKey(operator)) {
        if (operands.size() != 2) {
          throw new ISE("WTF?! Got binary operator[%s] with %s args?", kind, operands.size());
        }
        return DruidExpression.fromExpression(
            String.format(
                "(%s %s %s)",
                operands.get(0).getExpression(),
                BINARY_OPERATOR_MAP.get(operator),
                operands.get(1).getExpression()
            )
        );
      } else if (DIRECT_CONVERSIONS.containsKey(operator)) {
        final String functionName = DIRECT_CONVERSIONS.get(operator);
        return DruidExpression.fromExpression(DruidExpression.functionCall(functionName, operands));
      } else {
        return null;
      }
    } else if (kind == SqlKind.LITERAL) {
      // Translate literal.
      if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
        return DruidExpression.fromExpression(DruidExpression.numberLiteral((Number) RexLiteral.value(rexNode)));
      } else if (SqlTypeFamily.INTERVAL_DAY_TIME == sqlTypeName.getFamily()) {
        // Calcite represents DAY-TIME intervals in milliseconds.
        final long milliseconds = ((Number) RexLiteral.value(rexNode)).longValue();
        return DruidExpression.fromExpression(DruidExpression.numberLiteral(milliseconds));
      } else if (SqlTypeFamily.INTERVAL_YEAR_MONTH == sqlTypeName.getFamily()) {
        // Calcite represents YEAR-MONTH intervals in months.
        final long months = ((Number) RexLiteral.value(rexNode)).longValue();
        return DruidExpression.fromExpression(DruidExpression.numberLiteral(months));
      } else if (SqlTypeName.STRING_TYPES.contains(sqlTypeName)) {
        return DruidExpression.fromExpression(DruidExpression.stringLiteral(RexLiteral.stringValue(rexNode)));
      } else if (SqlTypeName.TIMESTAMP == sqlTypeName || SqlTypeName.DATE == sqlTypeName) {
        if (RexLiteral.isNullLiteral(rexNode)) {
          return DruidExpression.fromExpression(DruidExpression.nullLiteral());
        } else {
          return DruidExpression.fromExpression(
              DruidExpression.numberLiteral(
                  Calcites.calciteDateTimeLiteralToJoda(rexNode, plannerContext.getTimeZone()).getMillis()
              )
          );
        }
      } else if (SqlTypeName.BOOLEAN == sqlTypeName) {
        return DruidExpression.fromExpression(DruidExpression.numberLiteral(RexLiteral.booleanValue(rexNode) ? 1 : 0));
      } else {
        // Can't translate other literals.
        return null;
      }
    } else {
      // Can't translate.
      return null;
    }
  }

  /**
   * Translates "condition" to a Druid filter, or returns null if we cannot translate the condition.
   *
   * @param plannerContext planner context
   * @param rowSignature   row signature of the dataSource to be filtered
   * @param expression     Calcite row expression
   */
  public static DimFilter toFilter(
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
        final DimFilter nextFilter = toFilter(
            plannerContext,
            rowSignature,
            rexNode
        );
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
      return toLeafFilter(plannerContext, rowSignature, expression);
    }
  }

  /**
   * Translates "condition" to a Druid filter, assuming it does not contain any boolean expressions. Returns null
   * if we cannot translate the condition.
   *
   * @param plannerContext planner context
   * @param rowSignature   row signature of the dataSource to be filtered
   * @param rexNode        Calcite row expression
   */
  private static DimFilter toLeafFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    if (rexNode.isAlwaysTrue()) {
      return Filtration.matchEverything();
    } else if (rexNode.isAlwaysFalse()) {
      return Filtration.matchNothing();
    }

    final DimFilter simpleFilter = toSimpleLeafFilter(plannerContext, rowSignature, rexNode);
    return simpleFilter != null ? simpleFilter : toExpressionLeafFilter(plannerContext, rowSignature, rexNode);
  }

  /**
   * Translates to a simple leaf filter, meaning one that hits just a single column and is not an expression filter.
   */
  private static DimFilter toSimpleLeafFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final SqlKind kind = rexNode.getKind();

    if (kind == SqlKind.IS_TRUE || kind == SqlKind.IS_NOT_FALSE) {
      return toSimpleLeafFilter(
          plannerContext,
          rowSignature,
          Iterables.getOnlyElement(((RexCall) rexNode).getOperands())
      );
    } else if (kind == SqlKind.IS_FALSE || kind == SqlKind.IS_NOT_TRUE) {
      return new NotDimFilter(
          toSimpleLeafFilter(
              plannerContext,
              rowSignature,
              Iterables.getOnlyElement(((RexCall) rexNode).getOperands())
          )
      );
    } else if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
      final RexNode operand = Iterables.getOnlyElement(((RexCall) rexNode).getOperands());

      // operand must be translatable to a SimpleExtraction to be simple-filterable
      final DruidExpression druidExpression = toDruidExpression(plannerContext, rowSignature, operand);
      if (druidExpression == null || !druidExpression.isSimpleExtraction()) {
        return null;
      }

      final BoundDimFilter equalFilter = Bounds.equalTo(
          new BoundRefKey(
              druidExpression.getSimpleExtraction().getColumn(),
              druidExpression.getSimpleExtraction().getExtractionFn(),
              StringComparators.LEXICOGRAPHIC
          ),
          ""
      );

      return kind == SqlKind.IS_NOT_NULL ? new NotDimFilter(equalFilter) : equalFilter;
    } else if (kind == SqlKind.EQUALS
               || kind == SqlKind.NOT_EQUALS
               || kind == SqlKind.GREATER_THAN
               || kind == SqlKind.GREATER_THAN_OR_EQUAL
               || kind == SqlKind.LESS_THAN
               || kind == SqlKind.LESS_THAN_OR_EQUAL) {
      final List<RexNode> operands = ((RexCall) rexNode).getOperands();
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

      // lhs must be translatable to a SimpleExtraction to be simple-filterable
      final DruidExpression lhsExpression = toDruidExpression(plannerContext, rowSignature, lhs);
      if (lhsExpression == null || !lhsExpression.isSimpleExtraction()) {
        return null;
      }

      final String column = lhsExpression.getSimpleExtraction().getColumn();
      final ExtractionFn extractionFn = lhsExpression.getSimpleExtraction().getExtractionFn();

      if (column.equals(Column.TIME_COLUMN_NAME) && extractionFn instanceof TimeFormatExtractionFn) {
        // Check if we can strip the extractionFn and convert the filter to a direct filter on __time.
        // This allows potential conversion to query-level "intervals" later on, which is ideal for Druid queries.

        final Granularity granularity = ExtractionFns.toQueryGranularity(extractionFn);
        if (granularity != null) {
          // lhs is FLOOR(__time TO granularity); rhs must be a timestamp
          final long rhsMillis = Calcites.calciteDateTimeLiteralToJoda(rhs, plannerContext.getTimeZone()).getMillis();
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
        val = String.valueOf(
            Calcites.calciteDateTimeLiteralToJoda(
                rhsLiteral,
                plannerContext.getTimeZone()
            ).getMillis()
        );
      } else {
        // Don't know how to filter on this kind of literal.
        return null;
      }

      // Numeric lhs needs a numeric comparison.
      final StringComparator comparator = Calcites.getStringComparatorForSqlTypeName(lhs.getType().getSqlTypeName());
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
    } else if (kind == SqlKind.LIKE) {
      final List<RexNode> operands = ((RexCall) rexNode).getOperands();
      final DruidExpression druidExpression = toDruidExpression(
          plannerContext,
          rowSignature,
          operands.get(0)
      );
      if (druidExpression == null || !druidExpression.isSimpleExtraction()) {
        return null;
      }
      return new LikeDimFilter(
          druidExpression.getSimpleExtraction().getColumn(),
          RexLiteral.stringValue(operands.get(1)),
          operands.size() > 2 ? RexLiteral.stringValue(operands.get(2)) : null,
          druidExpression.getSimpleExtraction().getExtractionFn()
      );
    } else {
      return null;
    }
  }

  public static ExprType exprTypeForValueType(final ValueType valueType)
  {
    switch (valueType) {
      case LONG:
        return ExprType.LONG;
      case FLOAT:
      case DOUBLE:
        return ExprType.DOUBLE;
      case STRING:
        return ExprType.STRING;
      default:
        throw new ISE("No ExprType for valueType[%s]", valueType);
    }
  }

  /**
   * Translates to an "expression" type leaf filter. Used as a fallback if we can't use a simple leaf filter.
   */
  private static DimFilter toExpressionLeafFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final DruidExpression druidExpression = toDruidExpression(plannerContext, rowSignature, rexNode);
    return druidExpression == null
           ? null
           : new ExpressionDimFilter(druidExpression.getExpression(), plannerContext.getExprMacroTable());
  }

  private static String dateTimeFormatString(final SqlTypeName sqlTypeName)
  {
    if (sqlTypeName == SqlTypeName.DATE) {
      return "yyyy-MM-dd";
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return "yyyy-MM-dd HH:mm:ss";
    } else {
      throw new ISE("Unsupported DateTime type[%s]", sqlTypeName);
    }
  }
}
