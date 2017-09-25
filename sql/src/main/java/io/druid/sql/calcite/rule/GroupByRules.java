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

package io.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FloatMaxAggregatorFactory;
import io.druid.query.aggregation.FloatMinAggregatorFactory;
import io.druid.query.aggregation.FloatSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.segment.column.ValueType;
import io.druid.sql.calcite.aggregation.Aggregation;
import io.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.DruidExpression;
import io.druid.sql.calcite.expression.Expressions;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.function.Function;

public class GroupByRules
{
  private static final ApproxCountDistinctSqlAggregator APPROX_COUNT_DISTINCT = new ApproxCountDistinctSqlAggregator();

  private GroupByRules()
  {
    // No instantiation.
  }

  /**
   * Translate an AggregateCall to Druid equivalents.
   *
   * @return translated aggregation, or null if translation failed.
   */
  public static Aggregation translateAggregateCall(
      final PlannerContext plannerContext,
      final RowSignature sourceRowSignature,
      final RexBuilder rexBuilder,
      final Project project,
      final AggregateCall call,
      final List<Aggregation> existingAggregations,
      final String name
  )
  {
    final DimFilter filter;
    final SqlKind kind = call.getAggregation().getKind();
    final SqlTypeName outputType = call.getType().getSqlTypeName();

    if (call.filterArg >= 0) {
      // AGG(xxx) FILTER(WHERE yyy)
      if (project == null) {
        // We need some kind of projection to support filtered aggregations.
        return null;
      }

      final RexNode expression = project.getChildExps().get(call.filterArg);
      filter = Expressions.toFilter(plannerContext, sourceRowSignature, expression);
      if (filter == null) {
        return null;
      }
    } else {
      filter = null;
    }

    if (kind == SqlKind.COUNT && call.getArgList().isEmpty()) {
      // COUNT(*)
      return Aggregation.create(new CountAggregatorFactory(name)).filter(makeFilter(filter, sourceRowSignature));
    } else if (call.isDistinct()) {
      // AGG(DISTINCT x)
      if (kind == SqlKind.COUNT && plannerContext.getPlannerConfig().isUseApproximateCountDistinct()) {
        // Approximate COUNT(DISTINCT x)
        return APPROX_COUNT_DISTINCT.toDruidAggregation(
            name,
            sourceRowSignature,
            plannerContext,
            existingAggregations,
            project,
            call,
            makeFilter(filter, sourceRowSignature)
        );
      } else {
        // Exact COUNT(DISTINCT x), or some non-COUNT aggregator.
        return null;
      }
    } else if (kind == SqlKind.COUNT
               || kind == SqlKind.SUM
               || kind == SqlKind.SUM0
               || kind == SqlKind.MIN
               || kind == SqlKind.MAX
               || kind == SqlKind.AVG) {
      // Built-in agg, not distinct, not COUNT(*)
      final RexNode rexNode = Expressions.fromFieldAccess(
          sourceRowSignature,
          project,
          Iterables.getOnlyElement(call.getArgList())
      );

      final DruidExpression input = toDruidExpressionForAggregator(plannerContext, sourceRowSignature, rexNode);
      if (input == null) {
        return null;
      }

      if (kind == SqlKind.COUNT) {
        // COUNT(x) should count all non-null values of x.
        if (rexNode.getType().isNullable()) {
          final DimFilter nonNullFilter = Expressions.toFilter(
              plannerContext,
              sourceRowSignature,
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ImmutableList.of(rexNode))
          );

          if (nonNullFilter == null) {
            // Don't expect this to happen.
            throw new ISE("Could not create not-null filter for rexNode[%s]", rexNode);
          }

          return Aggregation.create(new CountAggregatorFactory(name)).filter(
              makeFilter(
                  filter == null ? nonNullFilter : new AndDimFilter(ImmutableList.of(filter, nonNullFilter)),
                  sourceRowSignature
              )
          );
        } else {
          return Aggregation.create(new CountAggregatorFactory(name)).filter(makeFilter(filter, sourceRowSignature));
        }
      } else {
        // Built-in aggregator that is not COUNT.
        final Aggregation retVal;

        final ValueType aggregationType;

        if (SqlTypeName.INT_TYPES.contains(outputType)
            || SqlTypeName.TIMESTAMP == outputType
            || SqlTypeName.DATE == outputType) {
          aggregationType = ValueType.LONG;
        } else if (SqlTypeName.FLOAT == outputType) {
          aggregationType = ValueType.FLOAT;
        } else if (SqlTypeName.FRACTIONAL_TYPES.contains(outputType)) {
          aggregationType = ValueType.DOUBLE;
        } else {
          throw new ISE(
              "Cannot determine aggregation type for SQL operator[%s] type[%s]",
              call.getAggregation().getName(),
              outputType
          );
        }

        final String fieldName;
        final String expression;
        final ExprMacroTable macroTable = plannerContext.getExprMacroTable();

        if (input.isDirectColumnAccess()) {
          fieldName = input.getDirectColumn();
          expression = null;
        } else {
          fieldName = null;
          expression = input.getExpression();
        }

        if (kind == SqlKind.SUM || kind == SqlKind.SUM0) {
          retVal = Aggregation.create(
              createSumAggregatorFactory(aggregationType, name, fieldName, expression, macroTable)
          );
        } else if (kind == SqlKind.MIN) {
          retVal = Aggregation.create(
              createMinAggregatorFactory(aggregationType, name, fieldName, expression, macroTable)
          );
        } else if (kind == SqlKind.MAX) {
          retVal = Aggregation.create(
              createMaxAggregatorFactory(aggregationType, name, fieldName, expression, macroTable)
          );
        } else if (kind == SqlKind.AVG) {
          final String sumName = StringUtils.format("%s:sum", name);
          final String countName = StringUtils.format("%s:count", name);
          final AggregatorFactory sum = createSumAggregatorFactory(
              aggregationType,
              sumName,
              fieldName,
              expression,
              macroTable
          );
          final AggregatorFactory count = new CountAggregatorFactory(countName);
          retVal = Aggregation.create(
              ImmutableList.of(sum, count),
              new ArithmeticPostAggregator(
                  name,
                  "quotient",
                  ImmutableList.of(
                      new FieldAccessPostAggregator(null, sumName),
                      new FieldAccessPostAggregator(null, countName)
                  )
              )
          );
        } else {
          // Not reached.
          throw new ISE("WTF?! Kind[%s] got into the built-in aggregator path somehow?!", kind);
        }

        return retVal.filter(makeFilter(filter, sourceRowSignature));
      }
    } else {
      // Not a built-in aggregator, check operator table.
      final SqlAggregator sqlAggregator = plannerContext.getOperatorTable()
                                                        .lookupAggregator(call.getAggregation());

      if (sqlAggregator != null) {
        return sqlAggregator.toDruidAggregation(
            name,
            sourceRowSignature,
            plannerContext,
            existingAggregations,
            project,
            call,
            makeFilter(filter, sourceRowSignature)
        );
      } else {
        return null;
      }
    }
  }

  private static DruidExpression toDruidExpressionForAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    final DruidExpression druidExpression = Expressions.toDruidExpression(plannerContext, rowSignature, rexNode);
    if (druidExpression == null) {
      return null;
    }

    if (druidExpression.isSimpleExtraction() &&
        (!druidExpression.isDirectColumnAccess()
         || rowSignature.getColumnType(druidExpression.getDirectColumn()) == ValueType.STRING)) {
      // Aggregators are unable to implicitly cast strings to numbers. So remove the simple extraction in this case.
      return druidExpression.map(simpleExtraction -> null, Function.identity());
    } else {
      return druidExpression;
    }
  }

  private static DimFilter makeFilter(final DimFilter filter, final RowSignature sourceRowSignature)
  {
    return filter == null
           ? null
           : Filtration.create(filter)
                       .optimizeFilterOnly(sourceRowSignature)
                       .getDimFilter();
  }

  private static AggregatorFactory createSumAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongSumAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatSumAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleSumAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }

  private static AggregatorFactory createMinAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongMinAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatMinAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleMinAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }

  private static AggregatorFactory createMaxAggregatorFactory(
      final ValueType aggregationType,
      final String name,
      final String fieldName,
      final String expression,
      final ExprMacroTable macroTable
  )
  {
    switch (aggregationType) {
      case LONG:
        return new LongMaxAggregatorFactory(name, fieldName, expression, macroTable);
      case FLOAT:
        return new FloatMaxAggregatorFactory(name, fieldName, expression, macroTable);
      case DOUBLE:
        return new DoubleMaxAggregatorFactory(name, fieldName, expression, macroTable);
      default:
        throw new ISE("Cannot create aggregator factory for type[%s]", aggregationType);
    }
  }
}
