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

package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.query.filter.ArrayContainsElementFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NullFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class ArrayOverlapOperatorConversion extends BaseExpressionDimFilterOperatorConversion
{
  private static final String EXPR_FUNCTION = "array_overlap";

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder("ARRAY_OVERLAP")
      .operandTypeChecker(
          OperandTypes.sequence(
              "'ARRAY_OVERLAP(array, array)'",
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.ARRAY),
                  OperandTypes.family(SqlTypeFamily.STRING)
              ),
              OperandTypes.or(
                  OperandTypes.family(SqlTypeFamily.ARRAY),
                  OperandTypes.family(SqlTypeFamily.STRING)
              )
          )
      )
      .returnTypeInference(ReturnTypes.BOOLEAN)
      .build();

  public ArrayOverlapOperatorConversion()
  {
    super(SQL_FUNCTION, EXPR_FUNCTION);
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      RowSignature rowSignature,
      @Nullable VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        operands
    );
    if (druidExpressions == null) {
      return null;
    }

    // Converts array_overlaps() function into an OR of Selector filters if possible.
    final DruidExpression leftExpr = druidExpressions.get(0);
    final DruidExpression rightExpr = druidExpressions.get(1);
    final boolean leftSimpleExtractionExpr = leftExpr.isSimpleExtraction();
    final boolean rightSimpleExtractionExpr = rightExpr.isSimpleExtraction();
    final DruidExpression simpleExtractionExpr;
    final DruidExpression complexExpr;

    if (leftSimpleExtractionExpr ^ rightSimpleExtractionExpr) {
      if (leftSimpleExtractionExpr) {
        simpleExtractionExpr = leftExpr;
        complexExpr = rightExpr;
      } else {
        simpleExtractionExpr = rightExpr;
        complexExpr = leftExpr;
      }
    } else {
      return toExpressionFilter(plannerContext, getDruidFunctionName(), druidExpressions);
    }

    final Expr expr = plannerContext.parseExpression(complexExpr.getExpression());
    if (expr.isLiteral() && !simpleExtractionExpr.isArray()) {
      // Evaluate the expression to take out the array elements.
      // We can safely pass null if the expression is literal.
      ExprEval<?> exprEval = expr.eval(InputBindings.nilBindings());
      Object[] arrayElements = exprEval.asArray();
      if (arrayElements == null || arrayElements.length == 0) {
        // If arrayElements is empty which means complexExpr is an empty array,
        // it is technically more correct to return a TrueDimFiler here.
        // However, since both Calcite's SqlMultisetValueConstructor and Druid's ArrayConstructorFunction don't allow
        // to create an empty array with no argument, we just return null.
        return null;
      } else if (arrayElements.length == 1) {
        if (plannerContext.isUseBoundsAndSelectors()) {
          return newSelectorDimFilter(simpleExtractionExpr.getSimpleExtraction(), Evals.asString(arrayElements[0]));
        } else {
          final String column = simpleExtractionExpr.isDirectColumnAccess()
                                ? simpleExtractionExpr.getSimpleExtraction().getColumn()
                                : virtualColumnRegistry.getOrCreateVirtualColumnForExpression(
                                    simpleExtractionExpr,
                                    simpleExtractionExpr.getDruidType()
                                );
          final Object elementValue = arrayElements[0];
          if (elementValue == null) {
            return NullFilter.forColumn(column);
          }
          return new EqualityFilter(
              column,
              ExpressionType.toColumnType(exprEval.type()),
              elementValue,
              null
          );
        }
      } else {
        final InDimFilter.ValuesSet valuesSet = InDimFilter.ValuesSet.create();
        for (final Object arrayElement : arrayElements) {
          valuesSet.add(Evals.asString(arrayElement));
        }

        return new InDimFilter(
            simpleExtractionExpr.getSimpleExtraction().getColumn(),
            valuesSet,
            simpleExtractionExpr.getSimpleExtraction().getExtractionFn(),
            null
        );
      }
    }

    // if the input is a direct array column, we can use sweet array filter
    if (simpleExtractionExpr.isDirectColumnAccess() && simpleExtractionExpr.isArray()) {
      // To convert this expression filter into an OR of ArrayContainsElement filters, we need to extract all array
      // elements.
      if (expr.isLiteral()) {
        // Evaluate the expression to get out the array elements.
        // We can safely pass a nil ObjectBinding if the expression is literal.
        ExprEval<?> exprEval = expr.eval(InputBindings.nilBindings());
        if (exprEval.isArray()) {
          final Object[] arrayElements = exprEval.asArray();
          if (arrayElements.length == 0) {
            // this isn't likely possible today because array constructor function does not accept empty argument list
            // but just in case, return null
            return null;
          }
          final List<DimFilter> filters = new ArrayList<>(arrayElements.length);
          final ColumnType elementType = ExpressionType.toColumnType(ExpressionType.elementType(exprEval.type()));
          for (final Object val : arrayElements) {
            filters.add(
                new ArrayContainsElementFilter(
                    leftExpr.getSimpleExtraction().getColumn(),
                    elementType,
                    val,
                    null
                )
            );
          }

          return filters.size() == 1 ? filters.get(0) : new OrDimFilter(filters);
        } else {
          return new ArrayContainsElementFilter(
              leftExpr.getSimpleExtraction().getColumn(),
              ExpressionType.toColumnType(exprEval.type()),
              exprEval.valueOrDefault(),
              null
          );
        }
      }
    }

    return toExpressionFilter(plannerContext, getDruidFunctionName(), druidExpressions);
  }
}
