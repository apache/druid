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
import com.google.common.collect.Iterables;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExpressionDimFilter;
import org.apache.druid.query.filter.IsFalseDimFilter;
import org.apache.druid.query.filter.IsTrueDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.Types;
import org.apache.druid.sql.calcite.filtration.BoundRefKey;
import org.apache.druid.sql.calcite.filtration.Bounds;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.filtration.RangeRefKey;
import org.apache.druid.sql.calcite.filtration.Ranges;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.ExpressionParser;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A collection of functions for translating from Calcite expressions into Druid objects.
 */
public class Expressions
{
  private Expressions()
  {
    // No instantiation.
  }

  /**
   * Old method used to translate a field access, possibly through a projection, to an underlying Druid dataSource.
   *
   * This exists to provide API compatibility to extensions, but is deprecated because there is a 4 argument version
   * that should be used instead.  Call sites should have access to a RexBuilder instance that they can get the
   * typeFactory from.
   *
   * @param rowSignature row signature of underlying Druid dataSource
   * @param project      projection, or null
   * @param fieldNumber  number of the field to access
   *
   * @return row expression
   */
  @Deprecated
  public static RexNode fromFieldAccess(
      final RowSignature rowSignature,
      @Nullable final Project project,
      final int fieldNumber
  )
  {
    //noinspection VariableNotUsedInsideIf
    return fromFieldAccess(project == null ? new JavaTypeFactoryImpl() : null, rowSignature, project, fieldNumber);
  }

  /**
   * Translate a field access, possibly through a projection, to an underlying Druid dataSource.
   *
   * @param typeFactory  factory for creating SQL types
   * @param rowSignature row signature of underlying Druid dataSource
   * @param project      projection, or null
   * @param fieldNumber  number of the field to access
   *
   * @return row expression
   */
  public static RexNode fromFieldAccess(
      final RelDataTypeFactory typeFactory,
      final RowSignature rowSignature,
      @Nullable final Project project,
      final int fieldNumber
  )
  {
    if (project == null) {
      return RexInputRef.of(fieldNumber, RowSignatures.toRelDataType(rowSignature, typeFactory));
    } else {
      return project.getProjects().get(fieldNumber);
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
   * Translate a list of Calcite {@code RexNode} to Druid expressions, with the possibility of having postagg operands.
   *
   * @param plannerContext        SQL planner context
   * @param rowSignature          signature of the rows to be extracted from
   * @param rexNodes              list of Calcite expressions meant to be applied on top of the rows
   * @param postAggregatorVisitor visitor that manages postagg names and tracks postaggs that were created as
   *                              by the translation
   *
   * @return list of Druid expressions in the same order as rexNodes, or null if not possible.
   * If a non-null list is returned, all elements will be non-null.
   */
  @Nullable
  public static List<DruidExpression> toDruidExpressionsWithPostAggOperands(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final List<RexNode> rexNodes,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final List<DruidExpression> retVal = new ArrayList<>(rexNodes.size());
    for (RexNode rexNode : rexNodes) {
      final DruidExpression druidExpression = toDruidExpressionWithPostAggOperands(
          plannerContext,
          rowSignature,
          rexNode,
          postAggregatorVisitor
      );
      if (druidExpression == null) {
        return null;
      }

      retVal.add(druidExpression);
    }
    return retVal;
  }

  /**
   * Translate a Calcite {@link RexNode} to a Druid expression for projections or the aggregators that don't
   * require numeric inputs.
   *
   * Consider using {@link org.apache.druid.sql.calcite.aggregation.Aggregations#toDruidExpressionForNumericAggregator}
   * for the aggregators that require numeric inputs.
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return DruidExpression referring to fields in rowOrder, or null if not possible to translate
   */
  @Nullable
  public static DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return toDruidExpressionWithPostAggOperands(
        plannerContext,
        rowSignature,
        rexNode,
        null
    );
  }

  @Nullable
  public static DruidExpression toDruidExpressionWithPostAggOperands(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      @Nullable final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final SqlKind kind = rexNode.getKind();
    if (kind == SqlKind.INPUT_REF) {
      return inputRefToDruidExpression(rowSignature, rexNode);
    } else if (rexNode instanceof RexCall) {
      return rexCallToDruidExpression(plannerContext, rowSignature, rexNode, postAggregatorVisitor);
    } else if (kind == SqlKind.LITERAL) {
      return literalToDruidExpression(plannerContext, rexNode);
    } else {
      // Can't translate.
      return null;
    }
  }

  private static DruidExpression inputRefToDruidExpression(
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    // Translate field references.
    final RexInputRef ref = (RexInputRef) rexNode;
    final String columnName = rowSignature.getColumnName(ref.getIndex());
    final Optional<ColumnType> columnType = rowSignature.getColumnType(ref.getIndex());
    if (columnName == null) {
      throw new ISE("Expression referred to nonexistent index[%d]", ref.getIndex());
    }

    return DruidExpression.ofColumn(columnType.orElse(null), columnName);
  }

  private static DruidExpression rexCallToDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final PostAggregatorVisitor postAggregatorVisitor
  )
  {
    final SqlOperator operator = ((RexCall) rexNode).getOperator();

    final SqlOperatorConversion conversion = plannerContext.getPlannerToolbox().operatorTable()
                                                           .lookupOperatorConversion(operator);

    if (conversion == null) {
      plannerContext.setPlanningError("SQL query requires '%s' operator that is not supported.", operator.getName());
      return null;
    } else {

      if (postAggregatorVisitor != null) {
        // try making postagg first
        PostAggregator postAggregator = conversion.toPostAggregator(
            plannerContext,
            rowSignature,
            rexNode,
            postAggregatorVisitor
        );

        if (postAggregator != null) {
          postAggregatorVisitor.addPostAgg(postAggregator);
          String exprName = postAggregator.getName();
          return DruidExpression.ofColumn(postAggregator.getType(rowSignature), exprName);
        }
      }

      DruidExpression expression = conversion.toDruidExpressionWithPostAggOperands(
          plannerContext,
          rowSignature,
          rexNode,
          postAggregatorVisitor
      );
      return expression;
    }
  }

  @Nullable
  static DruidExpression literalToDruidExpression(
      final PlannerContext plannerContext,
      final RexNode rexNode
  )
  {
    final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();

    // Translate literal.
    final ColumnType columnType = Calcites.getColumnTypeForRelDataType(rexNode.getType());
    if (RexLiteral.isNullLiteral(rexNode)) {
      return DruidExpression.ofLiteral(columnType, DruidExpression.nullLiteral());
    } else if (SqlTypeName.INT_TYPES.contains(sqlTypeName)) {
      final Number number = (Number) RexLiteral.value(rexNode);
      return DruidExpression.ofLiteral(
          columnType,
          number == null ? DruidExpression.nullLiteral() : DruidExpression.longLiteral(number.longValue())
      );
    } else if (SqlTypeName.NUMERIC_TYPES.contains(sqlTypeName)) {
      // Numeric, non-INT, means we represent it as a double.
      final Number number = (Number) RexLiteral.value(rexNode);
      return DruidExpression.ofLiteral(
          columnType,
          number == null ? DruidExpression.nullLiteral() : DruidExpression.doubleLiteral(number.doubleValue())
      );
    } else if (SqlTypeFamily.INTERVAL_DAY_TIME == sqlTypeName.getFamily()) {
      // Calcite represents DAY-TIME intervals in milliseconds.
      final long milliseconds = ((Number) RexLiteral.value(rexNode)).longValue();
      return DruidExpression.ofLiteral(columnType, DruidExpression.longLiteral(milliseconds));
    } else if (SqlTypeFamily.INTERVAL_YEAR_MONTH == sqlTypeName.getFamily()) {
      // Calcite represents YEAR-MONTH intervals in months.
      final long months = ((Number) RexLiteral.value(rexNode)).longValue();
      return DruidExpression.ofLiteral(columnType, DruidExpression.longLiteral(months));
    } else if (SqlTypeName.STRING_TYPES.contains(sqlTypeName)) {
      return DruidExpression.ofStringLiteral(RexLiteral.stringValue(rexNode));
    } else if (SqlTypeName.TIMESTAMP == sqlTypeName || SqlTypeName.DATE == sqlTypeName) {
      if (RexLiteral.isNullLiteral(rexNode)) {
        return DruidExpression.ofLiteral(columnType, DruidExpression.nullLiteral());
      } else {
        return DruidExpression.ofLiteral(
            columnType,
            DruidExpression.longLiteral(
                Calcites.calciteDateTimeLiteralToJoda(rexNode, plannerContext.getTimeZone()).getMillis()
            )
        );
      }
    } else if (SqlTypeName.BOOLEAN == sqlTypeName) {
      return DruidExpression.ofLiteral(
          columnType,
          DruidExpression.longLiteral(RexLiteral.booleanValue(rexNode) ? 1 : 0)
      );
    } else {
      // Can't translate other literals.
      return null;
    }
  }

  /**
   * Translates "condition" to a Druid filter, or returns null if we cannot translate the condition.
   *
   * @param plannerContext        planner context
   * @param rowSignature          input row signature
   * @param virtualColumnRegistry re-usable virtual column references, may be null if virtual columns aren't allowed
   * @param expression            Calcite row expression
   */
  @Nullable
  public static DimFilter toFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode expression
  )
  {
    final SqlKind kind = expression.getKind();

    if (kind == SqlKind.IS_TRUE
        || kind == SqlKind.IS_NOT_TRUE
        || kind == SqlKind.IS_FALSE
        || kind == SqlKind.IS_NOT_FALSE) {
      if (NullHandling.useThreeValueLogic()) {
        final DimFilter baseFilter = toFilter(
            plannerContext,
            rowSignature,
            virtualColumnRegistry,
            Iterables.getOnlyElement(((RexCall) expression).getOperands())
        );

        if (kind == SqlKind.IS_TRUE) {
          return IsTrueDimFilter.of(baseFilter);
        } else if (kind == SqlKind.IS_NOT_TRUE) {
          return NotDimFilter.of(IsTrueDimFilter.of(baseFilter));
        } else if (kind == SqlKind.IS_FALSE) {
          return IsFalseDimFilter.of(baseFilter);
        } else { // SqlKind.IS_NOT_FALSE
          return NotDimFilter.of(IsFalseDimFilter.of(baseFilter));
        }
      } else {
        // legacy behavior
        if (kind == SqlKind.IS_TRUE || kind == SqlKind.IS_NOT_FALSE) {
          return toFilter(
              plannerContext,
              rowSignature,
              virtualColumnRegistry,
              Iterables.getOnlyElement(((RexCall) expression).getOperands())
          );
        } else { // SqlKind.IS_FALSE || SqlKind.IS_NOT_TRUE
          return new NotDimFilter(
              toFilter(
                  plannerContext,
                  rowSignature,
                  virtualColumnRegistry,
                  Iterables.getOnlyElement(((RexCall) expression).getOperands())
              )
          );
        }
      }
    } else if (kind == SqlKind.CAST && expression.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
      // Calcite sometimes leaves errant, useless cast-to-booleans inside filters. Strip them and continue.
      return toFilter(
          plannerContext,
          rowSignature,
          virtualColumnRegistry,
          Iterables.getOnlyElement(((RexCall) expression).getOperands())
      );
    } else if (kind == SqlKind.AND || kind == SqlKind.OR || kind == SqlKind.NOT) {
      final List<DimFilter> filters = new ArrayList<>();
      for (final RexNode rexNode : ((RexCall) expression).getOperands()) {
        final DimFilter nextFilter = toFilter(
            plannerContext,
            rowSignature,
            virtualColumnRegistry,
            rexNode
        );
        if (nextFilter == null) {
          return null;
        }
        filters.add(nextFilter);
      }

      if (kind == SqlKind.AND) {
        return new AndDimFilter(filters);
      } else if (kind == SqlKind.OR) {
        return new OrDimFilter(filters);
      } else { // SqlKind.NOT
        return new NotDimFilter(Iterables.getOnlyElement(filters));
      }
    } else {
      // Handle filter conditions on everything else.
      return toLeafFilter(plannerContext, rowSignature, virtualColumnRegistry, expression);
    }
  }

  /**
   * Translates "condition" to a Druid filter, assuming it does not contain any boolean expressions. Returns null
   * if we cannot translate the condition.
   *
   * @param plannerContext        planner context
   * @param rowSignature          input row signature
   * @param virtualColumnRegistry re-usable virtual column references, may be null if virtual columns aren't allowed
   * @param rexNode               Calcite row expression
   */
  @Nullable
  private static DimFilter toLeafFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    if (rexNode.isAlwaysTrue()) {
      return Filtration.matchEverything();
    } else if (rexNode.isAlwaysFalse()) {
      return Filtration.matchNothing();
    }

    final DimFilter simpleFilter = toSimpleLeafFilter(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        rexNode
    );
    return simpleFilter != null
           ? simpleFilter
           : toExpressionLeafFilter(plannerContext, rowSignature, rexNode);
  }

  /**
   * Translates to a simple leaf filter, i.e. not an "expression" type filter. Note that the filter may still
   * reference expression virtual columns, if and only if "virtualColumnRegistry" is defined.
   *
   * @param plannerContext        planner context
   * @param rowSignature          input row signature
   * @param virtualColumnRegistry re-usable virtual column references, may be null if virtual columns aren't allowed
   * @param rexNode               Calcite row expression
   */
  @Nullable
  private static DimFilter toSimpleLeafFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    final SqlKind kind = rexNode.getKind();

    if (kind == SqlKind.IS_TRUE || kind == SqlKind.IS_NOT_FALSE) {
      if (NullHandling.useThreeValueLogic()) {
        // use expression filter to get istrue or notfalse expressions for correct 3vl behavior
        return toExpressionLeafFilter(plannerContext, rowSignature, rexNode);
      }
      // legacy behavior
      return toSimpleLeafFilter(
          plannerContext,
          rowSignature,
          virtualColumnRegistry,
          Iterables.getOnlyElement(((RexCall) rexNode).getOperands())
      );
    } else if (kind == SqlKind.IS_FALSE || kind == SqlKind.IS_NOT_TRUE) {
      if (NullHandling.useThreeValueLogic()) {
        // use expression filter to get isfalse or nottrue expressions for correct 3vl behavior
        return toExpressionLeafFilter(plannerContext, rowSignature, rexNode);
      }
      // legacy behavior
      return new NotDimFilter(
          toSimpleLeafFilter(
              plannerContext,
              rowSignature,
              virtualColumnRegistry,
              Iterables.getOnlyElement(((RexCall) rexNode).getOperands())
          )
      );
    } else if (kind == SqlKind.IS_NULL || kind == SqlKind.IS_NOT_NULL) {
      final RexNode operand = Iterables.getOnlyElement(((RexCall) rexNode).getOperands());

      final DruidExpression druidExpression = toDruidExpression(plannerContext, rowSignature, operand);
      if (druidExpression == null) {
        return null;
      }

      final DimFilter equalFilter;
      final ColumnType outputType = druidExpression.getDruidType();
      final boolean isOutputNumeric = Types.isNumeric(outputType);
      // if a simple extraction, we can typically use the base column directly for filtering. however, some expressions
      // such as cast also appear as a simple extraction because some native layer things can handle the cast
      // themselves, so we check the output type of the expression and compare it to the type of the direct column. a
      // string column might produce additional null values when converting to a number, so we should use the virtual
      // column instead for filtering to ensure that results are correct
      if (druidExpression.isSimpleExtraction() &&
          !(isOutputNumeric && !rowSignature.isNumeric(druidExpression.getDirectColumn()))) {
        if (plannerContext.isUseBoundsAndSelectors()) {
          equalFilter = new SelectorDimFilter(
              druidExpression.getSimpleExtraction().getColumn(),
              NullHandling.defaultStringValue(),
              druidExpression.getSimpleExtraction().getExtractionFn()
          );
        } else {
          if (druidExpression.getSimpleExtraction().getExtractionFn() != null) {
            if (virtualColumnRegistry != null) {
              String column = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                  druidExpression,
                  druidExpression.getDruidType()
              );
              equalFilter = NullFilter.forColumn(column);
            } else {
              // virtual column registry unavailable, fallback to expression filter
              return null;
            }
          } else {
            equalFilter = NullFilter.forColumn(druidExpression.getDirectColumn());
          }
        }
      } else if (virtualColumnRegistry != null) {
        final String virtualColumn = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            druidExpression,
            operand.getType()
        );

        if (plannerContext.isUseBoundsAndSelectors()) {
          equalFilter = new SelectorDimFilter(virtualColumn, NullHandling.defaultStringValue(), null);
        } else {
          equalFilter = NullFilter.forColumn(virtualColumn);
        }
      } else {
        return null;
      }

      return kind == SqlKind.IS_NOT_NULL ? new NotDimFilter(equalFilter) : equalFilter;
    } else if (kind == SqlKind.EQUALS
               || kind == SqlKind.NOT_EQUALS
               || kind == SqlKind.IS_NOT_DISTINCT_FROM
               || kind == SqlKind.IS_DISTINCT_FROM
               || kind == SqlKind.GREATER_THAN
               || kind == SqlKind.GREATER_THAN_OR_EQUAL
               || kind == SqlKind.LESS_THAN
               || kind == SqlKind.LESS_THAN_OR_EQUAL) {
      final List<RexNode> operands = ((RexCall) rexNode).getOperands();
      Preconditions.checkState(operands.size() == 2, "Expected 2 operands, got[%s]", operands.size());
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

      // Flip operator, maybe.
      final SqlKind flippedKind;

      if (flip) {
        switch (kind) {
          case EQUALS:
          case NOT_EQUALS:
          case IS_NOT_DISTINCT_FROM:
          case IS_DISTINCT_FROM:
            flippedKind = kind;
            break;
          case GREATER_THAN:
            flippedKind = SqlKind.LESS_THAN;
            break;
          case GREATER_THAN_OR_EQUAL:
            flippedKind = SqlKind.LESS_THAN_OR_EQUAL;
            break;
          case LESS_THAN:
            flippedKind = SqlKind.GREATER_THAN;
            break;
          case LESS_THAN_OR_EQUAL:
            flippedKind = SqlKind.GREATER_THAN_OR_EQUAL;
            break;
          default:
            throw new ISE("Kind[%s] not expected here", kind);
        }
      } else {
        flippedKind = kind;
      }

      final DruidExpression rhsExpression = toDruidExpression(plannerContext, rowSignature, rhs);
      final Expr rhsParsed = rhsExpression != null
                                       ? plannerContext.parseExpression(rhsExpression.getExpression())
                                       : null;
      // rhs must be a literal
      if (rhsParsed == null || !rhsParsed.isLiteral()) {
        return null;
      }

      // Translate lhs to a DruidExpression.
      final DruidExpression lhsExpression = toDruidExpression(plannerContext, rowSignature, lhs);
      if (lhsExpression == null) {
        return null;
      }

      // Special handling for filters on FLOOR(__time TO granularity).
      final Granularity queryGranularity =
          toQueryGranularity(lhsExpression, plannerContext.getExpressionParser());
      if (queryGranularity != null) {
        // lhs is FLOOR(__time TO granularity); rhs must be a timestamp
        final long rhsMillis = Calcites.calciteDateTimeLiteralToJoda(rhs, plannerContext.getTimeZone()).getMillis();
        return buildTimeFloorFilter(
            ColumnHolder.TIME_COLUMN_NAME,
            queryGranularity,
            flippedKind,
            rhsMillis,
            plannerContext
        );
      }

      String column;
      final ExtractionFn extractionFn;
      if (lhsExpression.isSimpleExtraction()) {
        column = lhsExpression.getSimpleExtraction().getColumn();
        extractionFn = lhsExpression.getSimpleExtraction().getExtractionFn();
      } else if (virtualColumnRegistry != null) {
        column = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
            lhsExpression,
            lhs.getType()
        );
        extractionFn = null;
      } else {
        return null;
      }

      if (column.equals(ColumnHolder.TIME_COLUMN_NAME) && extractionFn instanceof TimeFormatExtractionFn) {
        // Check if we can strip the extractionFn and convert the filter to a direct filter on __time.
        // This allows potential conversion to query-level "intervals" later on, which is ideal for Druid queries.

        final Granularity granularity = ExtractionFns.toQueryGranularity(extractionFn);
        if (granularity != null) {
          // lhs is FLOOR(__time TO granularity); rhs must be a timestamp.
          final long rhsMillis = Calcites.calciteDateTimeLiteralToJoda(rhs, plannerContext.getTimeZone()).getMillis();
          return buildTimeFloorFilter(column, granularity, flippedKind, rhsMillis, plannerContext);
        }
      }

      final ColumnType matchValueType = Calcites.getColumnTypeForRelDataType(rhs.getType());

      if (plannerContext.isUseBoundsAndSelectors()) {
        if (matchValueType == null || !matchValueType.isPrimitive()) {
          // Fall back to expression filter.
          return null;
        }

        final String stringVal;

        if (rhsParsed.getLiteralValue() == null) {
          stringVal = NullHandling.defaultStringValue();
        } else if (RexUtil.isLiteral(rhs, true) && SqlTypeName.NUMERIC_TYPES.contains(rhs.getType().getSqlTypeName())) {
          // Peek inside the original rhs for numerics, rather than using the parsed version, for highest fidelity
          // to what the query originally contained. (It may be a BigDecimal.)
          stringVal = String.valueOf(RexLiteral.value(rhs));
        } else {
          stringVal = String.valueOf(rhsParsed.getLiteralValue());
        }

        if (stringVal == null) {
          // Fall back to expression filter.
          return null;
        }

        // Numeric lhs needs a numeric comparison.
        final StringComparator comparator = Calcites.getStringComparatorForRelDataType(lhs.getType());
        final BoundRefKey boundRefKey = new BoundRefKey(column, extractionFn, comparator);
        final DimFilter filter;

        // Always use BoundDimFilters, to simplify filter optimization later (it helps to remember the comparator).
        switch (flippedKind) {
          case EQUALS:
          case IS_NOT_DISTINCT_FROM:
            // OK to treat EQUALS, IS_NOT_DISTINCT_FROM the same since we know stringVal is nonnull.
            filter = Bounds.equalTo(boundRefKey, stringVal);
            break;
          case NOT_EQUALS:
          case IS_DISTINCT_FROM:
            // OK to treat NOT_EQUALS, IS_DISTINCT_FROM the same since we know stringVal is nonnull.
            filter = new NotDimFilter(Bounds.equalTo(boundRefKey, stringVal));
            break;
          case GREATER_THAN:
            filter = Bounds.greaterThan(boundRefKey, stringVal);
            break;
          case GREATER_THAN_OR_EQUAL:
            filter = Bounds.greaterThanOrEqualTo(boundRefKey, stringVal);
            break;
          case LESS_THAN:
            filter = Bounds.lessThan(boundRefKey, stringVal);
            break;
          case LESS_THAN_OR_EQUAL:
            filter = Bounds.lessThanOrEqualTo(boundRefKey, stringVal);
            break;
          default:
            throw new IllegalStateException("Shouldn't have got here");
        }

        return filter;
      } else {
        final Object val = rhsParsed.getLiteralValue();

        if (val == null) {
          // fall back to expression filter
          return null;
        }

        // extractionFn are not supported by equality/range filter
        if (extractionFn != null) {
          if (virtualColumnRegistry != null) {
            column = virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                lhsExpression,
                lhs.getType()
            );
          } else {
            // if this happens for some reason, bail and use an expression filter
            return null;
          }
        }

        final RangeRefKey rangeRefKey = new RangeRefKey(column, matchValueType);
        final DimFilter filter;

        // Always use RangeFilter, to simplify filter optimization later
        switch (flippedKind) {
          case EQUALS:
          case IS_NOT_DISTINCT_FROM:
            filter = Ranges.equalTo(rangeRefKey, val);
            break;
          case NOT_EQUALS:
          case IS_DISTINCT_FROM:
            filter = new NotDimFilter(Ranges.equalTo(rangeRefKey, val));
            break;
          case GREATER_THAN:
            filter = Ranges.greaterThan(rangeRefKey, val);
            break;
          case GREATER_THAN_OR_EQUAL:
            filter = Ranges.greaterThanOrEqualTo(rangeRefKey, val);
            break;
          case LESS_THAN:
            filter = Ranges.lessThan(rangeRefKey, val);
            break;
          case LESS_THAN_OR_EQUAL:
            filter = Ranges.lessThanOrEqualTo(rangeRefKey, val);
            break;
          default:
            throw new IllegalStateException("Shouldn't have got here");
        }

        return filter;
      }
    } else if (rexNode instanceof RexCall) {
      final SqlOperator operator = ((RexCall) rexNode).getOperator();
      final SqlOperatorConversion conversion = plannerContext.getPlannerToolbox().operatorTable().lookupOperatorConversion(operator);

      if (conversion == null) {
        return null;
      } else {
        return conversion.toDruidFilter(plannerContext, rowSignature, virtualColumnRegistry, rexNode);
      }
    } else {
      return null;
    }
  }

  /**
   * Translates to an "expression" type leaf filter. Used as a fallback if we can't use a simple leaf filter.
   */
  @Nullable
  private static DimFilter toExpressionLeafFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final DruidExpression druidExpression = toDruidExpression(plannerContext, rowSignature, rexNode);

    if (druidExpression != null) {
      return new ExpressionDimFilter(
          druidExpression.getExpression(),
          plannerContext.parseExpression(druidExpression.getExpression()),
          null
      );
    }

    return null;
  }

  /**
   * Converts an expression to a Granularity, if possible. This is possible if, and only if, the expression
   * is a timestamp_floor function on the __time column with literal parameters for period, origin, and timeZone.
   *
   * @return granularity or null if not possible
   */
  @Nullable
  public static Granularity toQueryGranularity(final DruidExpression expression, final ExpressionParser parser)
  {
    final TimestampFloorExprMacro.TimestampFloorExpr expr = asTimestampFloorExpr(expression, parser);

    if (expr == null) {
      return null;
    }

    final Expr arg = expr.getArg();
    final Granularity granularity = expr.getGranularity();

    if (ColumnHolder.TIME_COLUMN_NAME.equals(arg.getBindingIfIdentifier())) {
      return granularity;
    } else {
      return null;
    }
  }

  @Nullable
  public static TimestampFloorExprMacro.TimestampFloorExpr asTimestampFloorExpr(
      final DruidExpression expression,
      final ExpressionParser parser
  )
  {
    final Expr expr = parser.parse(expression.getExpression());

    if (expr instanceof TimestampFloorExprMacro.TimestampFloorExpr) {
      return (TimestampFloorExprMacro.TimestampFloorExpr) expr;
    } else {
      return null;
    }
  }

  /**
   * Build a filter for an expression like FLOOR(column TO granularity) [operator] rhsMillis
   */
  private static DimFilter buildTimeFloorFilter(
      final String column,
      final Granularity granularity,
      final SqlKind operatorKind,
      final long rhsMillis,
      final PlannerContext plannerContext
  )
  {
    final Interval rhsInterval = granularity.bucket(DateTimes.utc(rhsMillis));

    // Is rhs aligned on granularity boundaries?
    final boolean rhsAligned = rhsInterval.getStartMillis() == rhsMillis;

    if (plannerContext.isUseBoundsAndSelectors()) {
      final BoundRefKey boundRefKey = new BoundRefKey(column, null, StringComparators.NUMERIC);
      return getBoundTimeDimFilter(operatorKind, boundRefKey, rhsInterval, rhsAligned);
    } else {
      final RangeRefKey rangeRefKey = new RangeRefKey(column, ColumnType.LONG);
      return getRangeTimeDimFilter(operatorKind, rangeRefKey, rhsInterval, rhsAligned);
    }
  }

  private static DimFilter getBoundTimeDimFilter(
      SqlKind operatorKind,
      BoundRefKey boundRefKey,
      Interval interval,
      boolean isAligned
  )
  {
    switch (operatorKind) {
      case EQUALS:
        return isAligned
               ? Bounds.interval(boundRefKey, interval)
               : Filtration.matchNothing();
      case NOT_EQUALS:
        return isAligned
               ? new NotDimFilter(Bounds.interval(boundRefKey, interval))
               : Filtration.matchEverything();
      case GREATER_THAN:
        return Bounds.greaterThanOrEqualTo(boundRefKey, String.valueOf(interval.getEndMillis()));
      case GREATER_THAN_OR_EQUAL:
        return isAligned
               ? Bounds.greaterThanOrEqualTo(boundRefKey, String.valueOf(interval.getStartMillis()))
               : Bounds.greaterThanOrEqualTo(boundRefKey, String.valueOf(interval.getEndMillis()));
      case LESS_THAN:
        return isAligned
               ? Bounds.lessThan(boundRefKey, String.valueOf(interval.getStartMillis()))
               : Bounds.lessThan(boundRefKey, String.valueOf(interval.getEndMillis()));
      case LESS_THAN_OR_EQUAL:
        return Bounds.lessThan(boundRefKey, String.valueOf(interval.getEndMillis()));
      default:
        throw new IllegalStateException("Shouldn't have got here");
    }
  }

  private static DimFilter getRangeTimeDimFilter(
      SqlKind operatorKind,
      RangeRefKey rangeRefKey,
      Interval interval,
      boolean isAligned
  )
  {
    switch (operatorKind) {
      case EQUALS:
        return isAligned
               ? Ranges.interval(rangeRefKey, interval)
               : Filtration.matchNothing();
      case NOT_EQUALS:
        return isAligned
               ? new NotDimFilter(Ranges.interval(rangeRefKey, interval))
               : Filtration.matchEverything();
      case GREATER_THAN:
        return Ranges.greaterThanOrEqualTo(rangeRefKey, interval.getEndMillis());
      case GREATER_THAN_OR_EQUAL:
        return isAligned
               ? Ranges.greaterThanOrEqualTo(rangeRefKey, interval.getStartMillis())
               : Ranges.greaterThanOrEqualTo(rangeRefKey, interval.getEndMillis());
      case LESS_THAN:
        return isAligned
               ? Ranges.lessThan(rangeRefKey, interval.getStartMillis())
               : Ranges.lessThan(rangeRefKey, interval.getEndMillis());
      case LESS_THAN_OR_EQUAL:
        return Ranges.lessThan(rangeRefKey, interval.getEndMillis());
      default:
        throw new IllegalStateException("Shouldn't have got here");
    }
  }
}
